package com.dorisdb.rtbench.partialUpdate;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.dorisdb.rtbench.SqlOperation;
import com.dorisdb.rtbench.Workload;

public class PartialUpdateWorkload extends Workload {
    private static final Logger LOG = LogManager.getLogger(PartialUpdateWorkload.class);

    String dbName;
    PaymentsPartialUpdate paymentsPartialUpdate;

    public PartialUpdateWorkload() {
    }

    @Override
    public void setup() throws Exception {
        paymentsPartialUpdate = new PaymentsPartialUpdate(this, conf);
        dbName = conf.getString("db.name");
        handler.onSqlOperation(new SqlOperation(String.format("create database if not exists %s", dbName)));
        handler.onSqlOperation(new SqlOperation("use " + dbName));
        if (conf.getBoolean("cleanup")) {
            handler.onSqlOperation(new SqlOperation("drop table if exists " + paymentsPartialUpdate.tableName));
        }
        handler.onSqlOperation(new SqlOperation(paymentsPartialUpdate.getCreateTableSql()));
        handler.flush();
    }

    @Override
    public void processEpoch(long id, long epochTs, long duration) throws Exception {
        paymentsPartialUpdate.processEpoch((int)epochTs, (int)duration);
    }

    @Override
    public void close() {
        try {
            if (conf.getBoolean("handler.dorisdb.query_after_large_quantity_versions")) {
                long t0 = System.nanoTime();
                handler.onEpochBegin(0, "query on close");
                handler.onSqlOperation(new SqlOperation("use " + dbName));
                String sql = String.format("select account_id00, sum(new_amount00) from payments_partial_update group by account_id00 order by sum(new_amount00) desc limit 10");
                handler.onSqlOperation(new SqlOperation(sql));
                long t1 = System.nanoTime();
                LOG.info(String.format("onClose: test query for large quantity versions: %s: %.2fms", sql, (t1-t0) / 1000000.0));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
