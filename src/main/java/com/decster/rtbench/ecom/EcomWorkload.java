package com.decster.rtbench.ecom;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.decster.rtbench.SqlOperation;
import com.decster.rtbench.Workload;

public class EcomWorkload extends Workload {
    private static final Logger LOG = LogManager.getLogger(EcomWorkload.class);

    Users users;
    Merchants merchants;
    Goods goods;
    Orders orders;

    public EcomWorkload() {
    }

    @Override
    public void setup() throws Exception {
        users = new Users(this, conf);
        merchants = new Merchants(this, conf);
        goods = new Goods(this, merchants, conf);
        orders = new Orders(this, conf);
        if (conf.getBoolean("db.cleanup")) {
            String dbName = conf.getString("db.name");
            handler.onSqlOperation(new SqlOperation(String.format("drop database if exists %s", dbName)));
            handler.onSqlOperation(new SqlOperation(String.format("create database if not exists %s", dbName)));
            handler.onSqlOperation(new SqlOperation("use " + dbName));
        }
        handler.onSqlOperation(new SqlOperation(users.getCreateTableSql()));
        handler.onSqlOperation(new SqlOperation(merchants.getCreateTableSql()));
        handler.onSqlOperation(new SqlOperation(goods.getCreateTableSql()));
        handler.onSqlOperation(new SqlOperation(orders.getCreateTableSql()));
        LOG.info("load users table data");
        users.
    }

    @Override
    public void processEpoch(long id, long epochTs, long duraton) {

    }

    @Override
    public void close() {
    }
}
