package com.decster.rtbench;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.decster.rtbench.DataOperation.Op;
import com.typesafe.config.Config;

public class MysqlHandler implements WorkloadHandler {
    private static final Logger LOG = LogManager.getLogger(MysqlHandler.class);
    Config conf;
    Workload load;
    Connection con;
    Statement st;
    boolean dryRun;

    StringBuilder batchSB;
    DataOperation batchFirst;
    int curBatchSize = 0;
    int batchSize;

    void startBatch(DataOperation op) throws Exception {
        batchSB = new StringBuilder();
        batchFirst = op;
        curBatchSize = 0;
        if (batchFirst.op == Op.INSERT || batchFirst.op == Op.UPSERT) {
            // insert & upsert both use insert
            batchSB.append("insert into ");
            batchSB.append(batchFirst.table);
            batchSB.append(" values ");
        } else {
            throw new Exception("op not supported");
        }
    }

    void batchAdd(DataOperation op) throws Exception {
        if (batchFirst.op == Op.INSERT || batchFirst.op == Op.UPSERT) {
            if (curBatchSize > 0) {
                batchSB.append(',');
            }
            batchSB.append(" (");
            for (int i=0;i<op.fullFields.length;i++) {
                if (i > 0) {
                    batchSB.append(',');
                }
                Object f = op.fullFields[i];
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
            throw new Exception("op not supported");
        }
        curBatchSize++;
    }

    void flushCurBatch() throws Exception {
        if (curBatchSize == 0) {
            return;
        }
        if (batchFirst.op == Op.UPSERT) {
            boolean first = true;
            batchSB.append("as v on duplicate key update ");
            int kidx = 0;
            int k = batchFirst.keyFieldIdxs[kidx];
            for (int i=0;i<batchFirst.fullFieldNames.length;i++) {
                if (i == k) {
                    ++kidx;
                    k = kidx < batchFirst.keyFieldIdxs.length ? batchFirst.keyFieldIdxs[kidx] : -1;
                    continue;
                }
                String name = batchFirst.fullFieldNames[i];
                if (first) {
                    first = false;
                } else {
                    batchSB.append(',');
                }
                batchSB.append(name);
                batchSB.append("=v.");
                batchSB.append(name);

            }
        } else if (batchFirst.op == Op.UPDATE) {
            throw new Exception("op UPDATE not supported");
        } else if (batchFirst.op == Op.DELETE) {
            throw new Exception("op DELETE not supported");
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
            startBatch(op);
        } else if (op.table != batchFirst.table || op.op != batchFirst.op) {
            flushCurBatch();
            startBatch(op);
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
    public void onDataOperation(DataOperation op) throws Exception {
        process(op);
    }

    @Override
    public void flush() throws Exception {
        flushCurBatch();
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
