package com.decster.rtbench;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.typesafe.config.Config;

public class MysqlHandler implements WorkloadHandler {
    private static final Logger LOG = LogManager.getLogger(MysqlHandler.class);
    Config conf;
    Workload load;
    Connection con;
    Statement st;

    Connection getConnection() throws SQLException {
        try {
            String url = conf.getString("handler.mysql.url");
            Connection ret = DriverManager.getConnection(url);
            return ret;
        } catch (SQLException e) {
            LOG.warn("connect to mysql failed", e);
            throw e;
        }
    }

    @Override
    public void init(Config conf, Workload load) throws Exception {
        this.conf = conf;
        this.load = load;
    }

    @Override
    public void onSetupBegin() throws Exception {
        if (con == null) {
            con = getConnection();
        }
        st = con.createStatement();
    }

    @Override
    public void onSqlOperation(SqlOperation op) throws Exception {
        LOG.info("execute sql: " + op.sql);
        st.execute(op.sql);
    }

    @Override
    public void onSetupEnd() throws Exception {
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
    public void onEpochBegin(long id, String name) throws Exception {
    }

    @Override
    public void onDataOperation(DataOperation op) throws Exception {
    }

    @Override
    public void onEpochEnd(long id, String name) throws Exception {
    }

    @Override
    public void onClose() throws Exception {
    }
}
