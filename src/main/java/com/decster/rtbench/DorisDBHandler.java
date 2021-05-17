package com.decster.rtbench;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.typesafe.config.Config;

public class DorisDBHandler implements WorkloadHandler {
    private static final Logger LOG = LogManager.getLogger(MysqlHandler.class);
    Config conf;
    Workload load;
    Connection con;
    Statement st;
    boolean dryRun;

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
    }

    @Override
    public void onSqlOperation(SqlOperation op) throws Exception {
        // TODO Auto-generated method stub

    }

    @Override
    public void onDataOperation(DataOperation op) throws Exception {
    }

    @Override
    public void flush() throws Exception {
    }

    @Override
    public void onSetupBegin() throws Exception {
        prepareStatement();
    }

    @Override
    public void onSetupEnd() throws Exception {
        flush();
        closeStatement();
    }

    @Override
    public void onEpochBegin(long id, String name) throws Exception {
        prepareStatement();
        if (!dryRun) {
            String dbName = conf.getString("db.name");
            st.execute("use " + dbName);
        }
    }

    @Override
    public void onEpochEnd(long id, String name) throws Exception {
        flush();
    }

    @Override
    public void onClose() throws Exception {
        flush();
        closeStatement();
    }

}
