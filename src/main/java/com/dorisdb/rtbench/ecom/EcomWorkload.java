package com.dorisdb.rtbench.ecom;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.dorisdb.rtbench.Locations;
import com.dorisdb.rtbench.SqlOperation;
import com.dorisdb.rtbench.Workload;

public class EcomWorkload extends Workload {
    private static final Logger LOG = LogManager.getLogger(EcomWorkload.class);

    String dbName;
    Locations locations;
    Users users;
    Merchants merchants;
    Goods goods;
    Orders orders;

    public EcomWorkload() {
    }

    @Override
    public void setup() throws Exception {
        boolean allTable = conf.getBoolean("ecom.all_table");
        locations = new Locations(conf);
        users = new Users(this, conf);
        merchants = new Merchants(this, conf);
        goods = new Goods(this, merchants, conf);
        orders = new Orders(this, conf);
        dbName = conf.getString("db.name");
        handler.onSqlOperation(new SqlOperation(String.format("create database if not exists %s", dbName)));
        handler.onSqlOperation(new SqlOperation("use " + dbName));
        if (conf.getBoolean("cleanup")) {
            if (allTable) {
                handler.onSqlOperation(new SqlOperation("drop table if exists " + Users.tableName));
                handler.onSqlOperation(new SqlOperation("drop table if exists " + Merchants.tableName));
                handler.onSqlOperation(new SqlOperation("drop table if exists " + Goods.tableName));
            }
            handler.onSqlOperation(new SqlOperation("drop table if exists " + Orders.tableName));
        }
        if (allTable) {
            handler.onSqlOperation(new SqlOperation(users.getCreateTableSql()));
            handler.onSqlOperation(new SqlOperation(merchants.getCreateTableSql()));
            handler.onSqlOperation(new SqlOperation(goods.getCreateTableSql()));
        }
        handler.onSqlOperation(new SqlOperation(orders.getCreateTableSql()));
        if (allTable) {
            LOG.info("load users table data: " + users.size());
            users.loadAllData(handler);
            handler.flush();
            LOG.info("load merchants table data: " + merchants.size());
            merchants.loadAllData(handler);
            handler.flush();
            LOG.info("load goods table data: " + goods.size());
            goods.loadAllData(handler);
            handler.flush();
        }
    }

    @Override
    public void processEpoch(long id, long epochTs, long duration) throws Exception {
        orders.processEpoch((int)epochTs, (int)duration);
    }

    @Override
    public void close() {
        try {
            if (conf.getBoolean("handler.dorisdb.query_after_large_quantity_versions")) {
                long t0 = System.nanoTime();
                handler.onEpochBegin(0, "query on close");
                handler.onSqlOperation(new SqlOperation("use " + dbName));
                String sql = String.format("select merchantid, sum(revenue) from orders group by merchantid order by sum(revenue) desc limit 10");
                handler.onSqlOperation(new SqlOperation(sql));
                long t1 = System.nanoTime();
                LOG.info(String.format("onClose: test query for large quantity versions: %s: %.2fms", sql, (t1-t0) / 1000000.0));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
