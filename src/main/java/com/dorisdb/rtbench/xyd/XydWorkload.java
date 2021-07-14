package com.dorisdb.rtbench.xyd;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.dorisdb.rtbench.SqlOperation;
import com.dorisdb.rtbench.Workload;

public class XydWorkload extends Workload {
    private static final Logger LOG = LogManager.getLogger(XydWorkload.class);

    String dbName;
    Installments installments;
    Payments payments;

    public XydWorkload() {
    }

    @Override
    public void setup() throws Exception {
        installments = new Installments(this, conf);
        payments = new Payments(this, conf);
        dbName = conf.getString("db.name");
        handler.onSqlOperation(new SqlOperation(String.format("create database if not exists %s", dbName)));
        handler.onSqlOperation(new SqlOperation("use " + dbName));
        if (conf.getBoolean("cleanup")) {
            handler.onSqlOperation(new SqlOperation("drop table if exists " + installments.tableName));
            handler.onSqlOperation(new SqlOperation("drop table if exists " + payments.tableName));
        }
        handler.onSqlOperation(new SqlOperation(installments.getCreateTableSql()));
        handler.onSqlOperation(new SqlOperation(payments.getCreateTableSql()));
        handler.flush();
    }

    @Override
    public void processEpoch(long id, long epochTs, long duration) throws Exception {
        installments.processEpoch((int)epochTs, (int)duration);
        payments.processEpoch((int)epochTs, (int)duration);
    }

    @Override
    public void close() {
    }
}
