package com.decster.rtbench.ecom;

import com.decster.rtbench.Locations;
import com.decster.rtbench.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.decster.rtbench.SqlOperation;
import com.decster.rtbench.Workload;

public class EcomWorkload extends Workload {
    private static final Logger LOG = LogManager.getLogger(EcomWorkload.class);

    Locations locations;
    Users users;
    Merchants merchants;
    Goods goods;
    Orders orders;

    public EcomWorkload() {
    }

    @Override
    public void setup() throws Exception {
        locations = new Locations(conf);
        users = new Users(this, conf);
        merchants = new Merchants(this, conf);
        goods = new Goods(this, merchants, conf);
        orders = new Orders(this, conf);
        String dbName = conf.getString("db.name");
        if (conf.getBoolean("cleanup")) {
            handler.onSqlOperation(new SqlOperation(String.format("drop database if exists %s", dbName)));
            handler.onSqlOperation(new SqlOperation(String.format("create database if not exists %s", dbName)));
        }
        handler.onSqlOperation(new SqlOperation("use " + dbName));
        handler.onSqlOperation(new SqlOperation(users.getCreateTableSql()));
        handler.onSqlOperation(new SqlOperation(merchants.getCreateTableSql()));
        handler.onSqlOperation(new SqlOperation(goods.getCreateTableSql()));
        handler.onSqlOperation(new SqlOperation(orders.getCreateTableSql()));
        LOG.info("load users table data: " + users.size());
        users.loadAllData(handler);
        LOG.info("load merchants table data: " + merchants.size());
        merchants.loadAllData(handler);
        LOG.info("load goods table data: " + goods.size());
        goods.loadAllData(handler);
    }

    @Override
    public void processEpoch(long id, long epochTs, long duration) throws Exception {
        orders.processEpoch((int)epochTs, (int)duration);
    }

    @Override
    public void close() {
    }
}
