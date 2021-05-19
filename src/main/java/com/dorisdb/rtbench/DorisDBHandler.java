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
    Config conf;
    Workload load;
    Connection con;
    Statement st;
    boolean dryRun;
    String streamLoadAddr;
    boolean streamLoadKeepFile;
    String dbName;
    String curLabel;
    long loadWait;

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
        this.loadWait = conf.getDuration("handler.dorisdb.load_wait").toMillis();
        this.dbName = conf.getString("db.name");
        loadByTable = new HashMap<String, DorisStreamLoad>();
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
            ret = new DorisStreamLoad(conf,  dbName, table, label);
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
