package com.dorisdb.rtbench;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.dorisdb.rtbench.DataOperation.Op;
import com.typesafe.config.Config;

public class MysqlHandler implements WorkloadHandler {
    private static final Logger LOG = LogManager.getLogger(MysqlHandler.class);
    Config conf;
    Workload load;
    Connection con;
    Statement st;
    boolean dryRun;
    String dbName;

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
        } else if (batchFirst.op == Op.DELETE) {
            batchSB.append("delete from ");
            batchSB.append(batchFirst.table);
            if (batchFirst.keyFieldIdxs.length == 1) {
                batchSB.append(" where ");
                batchSB.append(batchFirst.fullFieldNames[batchFirst.keyFieldIdxs[0]]);
                batchSB.append(" in (");
            } else {
                batchSB.append(" where (");
                for (int i=0;i<batchFirst.keyFieldIdxs.length;i++) {
                    if (i>0) {
                        batchSB.append(",");
                    }
                    batchSB.append(batchFirst.fullFieldNames[batchFirst.keyFieldIdxs[i]]);
                }
                batchSB.append(") in (");
            }
        } else {
            throw new Exception("op not supported");
        }
    }

    static void appendObj(StringBuilder batchSB, Object f) {
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
                appendObj(batchSB, op.fullFields[i]);
            }
            batchSB.append(")");
        } else if (batchFirst.op == Op.DELETE) {
            if (curBatchSize > 0) {
                batchSB.append(',');
            }
            if (batchFirst.keyFieldIdxs.length == 1) {
                appendObj(batchSB, op.fullFields[batchFirst.keyFieldIdxs[0]]);
            } else {
                batchSB.append("(");
                for (int i=0;i<batchFirst.keyFieldIdxs.length;i++) {
                    if (i>0) {
                        batchSB.append(",");
                    }
                    appendObj(batchSB, op.fullFields[batchFirst.keyFieldIdxs[i]]);
                }
                batchSB.append(")");
            }
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
        } else if (batchFirst.op == Op.DELETE) {
            batchSB.append(")");
        } else if (batchFirst.op == Op.UPDATE) {
            throw new Exception("op UPDATE not supported");
        }
        String sql = batchSB.toString();
        if (dryRun) {
            LOG.info(sql.length() < 80 ? sql : sql.substring(0, 80));
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
        this.dbName = conf.getString("db.name");
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
        if (dryRun) {
            LOG.info("execute sql: " + op.sql);
        } else {
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
