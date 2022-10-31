package com.dorisdb.rtbench;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.typesafe.config.Config;

public class DorisDBHandler implements WorkloadHandler {
    private static final Logger LOG = LogManager.getLogger(DorisDBHandler.class);
    private static java.lang.Long maxVersionCount = 0L;
    Config conf;
    Workload load;
    Connection con;
    Statement st;
    boolean dryRun;
    boolean recordMaxVersionCount;
    long fileSize;
    String dbName;
    String curLabel;
    long loadWait;
    int loadConcurrency;
    long totalOp = 0;
    long totalBytes = 0;
    double totalTimeSec = 0;
    ExecutorService streamLoadWorker = null;
    Future<Void> lastStreamLoadTask = null;

    long lastReportTs = System.nanoTime();
    private void report(boolean isFinal) {
        long now = System.nanoTime();
        if (isFinal || lastReportTs + 180 * 1000000000L < now) {
            double realTimeSec = (now - lastReportTs) / 1000000000.0;
            synchronized (this) {
                LOG.info(String.format("total op: %d %.1fM pureLoad: %.1fs %.0fop/s %.0fM/s total: %.1fs %.0fop/s %.0fM/s ",
                        totalOp, totalBytes/1024.0/1024.0,
                        totalTimeSec, totalOp/totalTimeSec, totalBytes/1024.0/1024.0/totalTimeSec,
                        realTimeSec, totalOp/realTimeSec, totalBytes/1024.0/1024.0/realTimeSec));
                totalOp = 0;
                totalBytes = 0;
                totalTimeSec = 0;
            }
            lastReportTs = now;
        }
    }

    Connection getConnection() throws SQLException {
        try {
            String url = conf.getString("handler.dorisdb.url");
            String user = conf.getString("handler.dorisdb.user");
            String password = conf.getString("handler.dorisdb.password");
            Connection ret = DriverManager.getConnection(url, user, password);
            return ret;
        } catch (SQLException e) {
            LOG.warn("connect to dorisdb failed", e);
            throw e;
        }
    }

    private void prepareStatement() throws SQLException {
        if (dryRun) {
            return;
        }
        if (con == null) {
            con = getConnection();
        }
        if (st == null) {
            st = con.createStatement();
        }
    }

    private void closeStatement() throws SQLException {
        if (st != null) {
            st.close();
            st = null;
        }
        if (con != null) {
            con.close();
            con = null;
        }
    }

    @Override
    public void init(Config conf, Workload load) throws Exception {
        this.conf = conf;
        this.load = load;
        this.dryRun = conf.getBoolean("dry_run");
        this.recordMaxVersionCount = conf.getBoolean("handler.dorisdb.record_max_version_count");
        this.loadWait = conf.getDuration("handler.dorisdb.load_wait").toMillis();
        this.dbName = conf.getString("db.name");
        this.loadConcurrency = conf.getInt("handler.dorisdb.load_concurrency");
        if (conf.getBoolean("handler.dorisdb.async_stream_load")) {
            streamLoadWorker = Executors.newFixedThreadPool(1, new ThreadFactory() {
                @Override
                public Thread newThread(Runnable r) {
                    Thread ret = new Thread(r, "async-streamload");
                    ret.setDaemon(true);
                    return ret;
                }
            });
        }
        loadByTable = new HashMap<String, DorisLoad>();
    }

    @Override
    public void onSqlOperation(SqlOperation op) throws Exception {
        if (dryRun) {
            LOG.info("execute sql: " + op.sql);
        } else {
            LOG.info("execute sql: " + op.sql);
            st.execute(op.sql);
        }
    }

    @Override
    public java.sql.ResultSet onSqlOperationResult(SqlOperation op) throws Exception {
        if (dryRun) {
            LOG.info("execute sql: " + op.sql);
            return null;
        } else {
            st.execute(op.sql);
            return st.getResultSet();
        }
    }

    Map<String, DorisLoad> loadByTable;

    void executeAndClearAllLoads() throws Exception {
        for (Map.Entry<String, DorisLoad> e : loadByTable.entrySet()) {
            final DorisLoad load = e.getValue();
            fileSize += load.getFileSize();
            if (streamLoadWorker != null) {
                if (lastStreamLoadTask != null) {
                    lastStreamLoadTask.get();
                    lastStreamLoadTask = null;
                }
                lastStreamLoadTask = streamLoadWorker.submit(new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        runLoadAndReport(load);
                        return null;
                    }
                });
            } else {
                runLoadAndReport(load);
            }
            Thread.sleep(loadWait);
            if (recordMaxVersionCount) {
                st.execute(String.format("use " + dbName));
                st.execute(String.format("show tablet from %s", load.getTable()));
                java.lang.Long versionCount = 0L;
                java.sql.ResultSet rs = st.getResultSet();
                while (rs.next()) {
                    versionCount += rs.getLong("VersionCount");
                }
                if (maxVersionCount < versionCount) {
                    maxVersionCount = versionCount;
                }
            }
        }
        if (recordMaxVersionCount) {
            LOG.info(String.format("maxVersionCount: %d", maxVersionCount));
        }
        loadByTable.clear();
    }

    private void runLoadAndReport(DorisLoad load) throws Exception {
        long t0 = System.nanoTime();
        long fileSize = load.getFileSize();
        load.send();
        long t1 = System.nanoTime();
        double sec = (t1-t0) / 1000000000.0;
        double fileSizeMB = fileSize / (1024.0 * 1024.0);
        LOG.info(String.format("load %s done op: %d %.2fs %.0f op/s  %.1fM %.1f M/s", load.getLabel(), load.getOpCount(),
                sec, load.getOpCount() / sec, fileSizeMB, fileSizeMB / sec));
        synchronized (this) {
            totalOp += load.getOpCount();
            totalBytes += fileSize;
            totalTimeSec += sec;
        }
        report(false);
    }

    DorisLoad getLoad(String table) {
        DorisLoad ret = loadByTable.get(table);
        if (ret == null) {
            String label = String.format("load-%s-%s", table, curLabel);
            if (loadConcurrency == 1) {
                ret = new DorisStreamLoad(conf,  dbName, table, label);
            } else {
                ret = new ConcurrentDorisStreamLoad(conf,  dbName, table, label, loadConcurrency);
            }
            loadByTable.put(table, ret);
        }
        return ret;
    }

    @Override
    public void onDataOperation(DataOperation op) throws Exception {
        DorisLoad load = getLoad(op.table);
        load.addData(op);
    }

    @Override
    public void flush() throws Exception {
    }

    @Override
    public void onSetupBegin() throws Exception {
        curLabel = "setup";
        prepareStatement();
    }

    @Override
    public void onSetupEnd() throws Exception {
        closeStatement();
        executeAndClearAllLoads();
    }

    @Override
    public void onEpochBegin(long id, String name) throws Exception {
        curLabel = name;
        prepareStatement();
    }

    @Override
    public void onEpochEnd(long id, String name) throws Exception {
        executeAndClearAllLoads();
    }

    @Override
    public void onClose() throws Exception {
        if (lastStreamLoadTask != null) {
            lastStreamLoadTask.get();
            lastStreamLoadTask = null;
        }
        closeStatement();
        report(true);
    }

    @Override
    public long getFileSize() { return fileSize; }

}
