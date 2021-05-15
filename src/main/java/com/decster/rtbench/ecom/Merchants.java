package com.decster.rtbench.ecom;

import java.util.Random;

import com.typesafe.config.Config;

public class Merchants {
    EcomWorkload load;
    Config conf;
    Random rand;
    long num;

    public Merchants(EcomWorkload load, Config conf) {
        this.load = load;
        this.conf = conf;
        long ordersPerDay = conf.getLong("orders_per_day");
        this.num = ordersPerDay / 100;
    }

    public long size() {
        return num;
    }

    Merchant get(long id) {
        return null;
    }

    String getCreateTableSql() {
        String ret = "create table merchants ("
                + "id int not null,"
                + "name varchar(64) not null,"
                + "address varchar(256) not null,"
                + "city varchar(64) not null,"
                + "province varchar(64) not null,"
                + "country varchar(64) not null,"
                + "phone varchar(32)";
        if (conf.getString("db.type").toLowerCase().startsWith("doris")) {
            ret += ") primary key(id) ";
            ret += String.format("DISTRIBUTED BY HASH(id) BUCKETS %d"
                    + " PROPERTIES(\"replication_num\" = \"%d\")",
                    conf.getInt("db.merchants.bucket"),
                    conf.getInt("db.replication"));
        } else {
            ret += ", primary key(id))";
        }
        return ret;
    }
}
