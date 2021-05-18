package com.dorisdb.rtbench;

import java.io.File;
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
    Config conf;
    Workload load;
    Connection con;
    Statement st;
    boolean dryRun;
    String streamLoadAddr;
    boolean streamLoadKeepFile;
    String dbName;
    String curLabel;
    String tmpDir;
    long loadWait;

    Connection getConnection() throws SQLException {
        try {
            String url = conf.getString("handler.dorisdb.url");
            Connection ret = DriverManager.getConnection(url);
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
        this.streamLoadAddr = conf.getString("handler.dorisdb.stream_load.addr");
        this.streamLoadKeepFile = conf.getBoolean("handler.dorisdb.stream_load.keep_file");
        this.tmpDir = conf.getString("handler.dorisdb.tmpdir");
        this.loadWait = conf.getDuration("handler.dorisdb.load_wait").toMillis();
        this.dbName = conf.getString("db.name");
        loadByTable = new HashMap<String, DorisStreamLoad>();
        new File(tmpDir).mkdirs();
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

    Map<String, DorisStreamLoad> loadByTable;

    void executeAndClearAllLoads() throws Exception {
        for (Map.Entry<String, DorisStreamLoad> e : loadByTable.entrySet()) {
            DorisStreamLoad load = e.getValue();
            LOG.info(String.format("stream load %s op:%d start", load.label, load.opCount));
            long t0 = System.nanoTime();
            load.send();
            long t1 = System.nanoTime();
            LOG.info(String.format("stream load %s op:%d done %.2fs", load.label, load.opCount, (t1-t0) / 1000000000.0));
            Thread.sleep(loadWait);
        }
        loadByTable.clear();
    }

    DorisStreamLoad getLoad(String table) {
        DorisStreamLoad ret = loadByTable.get(table);
        if (ret == null) {
            String label = String.format("load-%s-%s", table, curLabel);
            ret = new DorisStreamLoad(streamLoadAddr, dbName, table, label, tmpDir, streamLoadKeepFile, dryRun);
            loadByTable.put(table, ret);
        }
        return ret;
    }

    @Override
    public void onDataOperation(DataOperation op) throws Exception {
        DorisStreamLoad load = getLoad(op.table);
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

}
