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
    boolean dryRun;

    StringBuilder batchSB;
    String batchTable;
    DataOperation.Op batchOp;
    int curBatchSize = 0;
    int batchSize;

    void startBatch(String table, DataOperation.Op op) {
        batchSB = new StringBuilder();
        batchTable = table;
        batchOp = op;
        curBatchSize = 0;
        if (op == DataOperation.Op.INSERT) {
            // upsert
            batchSB.append("replace into ");
            batchSB.append(table);
            batchSB.append(" values ");
        } else {
            // TODO: delete
        }
    }

    void batchAdd(DataOperation op) {
        if (batchOp == DataOperation.Op.INSERT) {
            if (curBatchSize > 0) {
                batchSB.append(',');
            }
            batchSB.append(" (");
            for (int i=0;i<op.fields.length;i++) {
                if (i > 0) {
                    batchSB.append(',');
                }
                Object f = op.fields[i];
                if (f != null) {
                    if (f instanceof String) {
                        batchSB.append('\'');
                        // TODO: escape
                        batchSB.append((String)f);
                        batchSB.append('\'');
                    } else if (f instanceof Number) {
                        batchSB.append(f.toString());
                    } else {
                        batchSB.append(f.toString());
                    }
                } else {
                    batchSB.append("NULL");
                }
            }
            batchSB.append(")");
        } else {
            // TODO: delete
        }
        curBatchSize++;
    }

    void flushCurBatch() throws SQLException {
        if (curBatchSize == 0) {
            return;
        }
        if (batchOp == DataOperation.Op.INSERT) {
            //
        } else {
            // TODO: delete
        }
        String sql = batchSB.toString();
        if (dryRun) {
            LOG.info("sql batch: " + sql);
        } else {
            st.execute(sql);
        }
        batchSB = null;
        curBatchSize = 0;
    }

    void process(DataOperation op) throws Exception {
        if (curBatchSize == 0) {
            startBatch(op.table, op.op);
        } else if (op.table != batchTable || op.op != batchOp) {
            flushCurBatch();
            startBatch(op.table, op.op);
        }
        batchAdd(op);
        if (curBatchSize == batchSize) {
            flushCurBatch();
        }
    }

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
        this.batchSize = conf.getInt("handler.mysql.batch_size");
        this.dryRun = conf.getBoolean("dry_run");
    }

    @Override
    public void onSetupBegin() throws Exception {
        prepareStatement();
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
    public void onSqlOperation(SqlOperation op) throws Exception {
        LOG.info("execute sql: " + op.sql);
        if (!dryRun) {
            st.execute(op.sql);
        }
    }

    @Override
    public void onSetupEnd() throws Exception {
        flushCurBatch();
        closeStatement();
    }

    @Override
    public void onEpochBegin(long id, String name) throws Exception {
        prepareStatement();
    }

    @Override
    public void onDataOperation(DataOperation op) throws Exception {
        process(op);
    }

    @Override
    public void onEpochEnd(long id, String name) throws Exception {
        flushCurBatch();
    }

    @Override
    public void onClose() throws Exception {
        closeStatement();
    }
}
