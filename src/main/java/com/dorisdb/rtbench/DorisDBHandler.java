package com.dorisdb.rtbench;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

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

    Map<String, DorisLoad> loadByTable;

    void executeAndClearAllLoads() throws Exception {
        for (Map.Entry<String, DorisLoad> e : loadByTable.entrySet()) {
            DorisLoad load = e.getValue();
            // LOG.info(String.format("stream load %s op:%d start", load.getLabel(), load.getOpCount()));
            // long t0 = System.nanoTime();
            load.send();
            fileSize = load.getFileSize();
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
            // long t1 = System.nanoTime();
            // LOG.info(String.format("load %s op:%d done %.2fs", load.getLabel(), load.getOpCount(), (t1-t0) / 1000000000.0));
            Thread.sleep(loadWait);
        }
        if (recordMaxVersionCount) {
            LOG.info(String.format("maxVersionCount: %d", maxVersionCount));
        }
        loadByTable.clear();
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
        closeStatement();
    }

    @Override
    public long getFileSize() throws Exception { return fileSize; }

}
