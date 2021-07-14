package com.dorisdb.rtbench.xyd;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.typesafe.config.Config;

public class Installments {
    private static final Logger LOG = LogManager.getLogger(Payments.class);

    XydWorkload load;
    Config conf;
    int ordersPerDay;
    double ordersPerSecond;
    String tableName;

    public Installments(XydWorkload load, Config conf) {
        this.load = load;
        this.conf = conf;
        this.tableName = "installments";
        this.ordersPerDay = conf.getInt("orders_per_day");
        this.ordersPerSecond = (ordersPerDay / (3600 * 24.0));
    }

    void processEpoch(int ts, int duration) throws Exception {
    }

    String getCreateTableSql() {
        String ret = "create table orders ("
                + "id bigint not null," + "userid bigint not null,"
                + "goodid int not null," + "merchantid int not null," + "ship_address varchar(256) not null,"
                + "ship_mode varchar(32) not null," + "order_date int not null," + "order_ts int not null,"
                + "payment_ts int null," + "delivery_start_ts int null," + "delivery_finish_ts int null,"
                + "quantify int not null," + "price int not null," + "discount tinyint not null,"
                + "revenue int not null," + "state tinyint not null";
        if (conf.getString("db.type").toLowerCase().startsWith("doris")) {
            ret += ") primary key(id) ";
            ret += String.format("DISTRIBUTED BY HASH(id) BUCKETS %d" + " PROPERTIES(\"replication_num\" = \"%d\")",
                    conf.getInt("db.orders.bucket"), conf.getInt("db.replication"));
        } else {
            ret += ", primary key(id))";
        }
        return ret;
    }

}
