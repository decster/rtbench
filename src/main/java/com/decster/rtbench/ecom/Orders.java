package com.decster.rtbench.ecom;

import java.text.SimpleDateFormat;
import java.util.Date;

import com.decster.rtbench.Utils;
import com.decster.rtbench.Utils.PowerDist;
import com.typesafe.config.Config;

public class Orders {
    EcomWorkload load;
    Config conf;
    long ordersPerDay;
    double ordersPerSecond;
    long curId;
    long [] activeOrders;
    int numActiveOrders;

    static final SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyyMMdd");
    static final PowerDist paymentTime = new PowerDist(10, 300);
    static final PowerDist deliveryStartTime = new PowerDist(3600, 36000);
    static final PowerDist deliveryFinishTime = new PowerDist(3600*20, 3600*24*5);
    static final PowerDist quantityDist = new PowerDist(1, 20, 2.5f);
    static final PowerDist discountDist = new PowerDist(10, 60, 3);
    Utils.Poisson poisson;

    static enum State {
        CREATED,
        PAYED,
        DELIVER_STARTED,
        DELIVER_FINISHED
    }

    public Orders(EcomWorkload load, Config conf) {
        this.load = load;
        this.conf = conf;
        this.ordersPerDay = conf.getLong("orders_per_day");
        this.ordersPerSecond = (ordersPerDay / (3600 * 24.0));
        this.curId = 1;
        this.numActiveOrders = 0;
        this.activeOrders = new long[0];
        poisson = new Utils.Poisson(ordersPerSecond, 1);
    }

    long[] generate(int ts, int duration) {
        // to save memory, each order is represented as 128bit integer:
        // (orderId int64, startTs int32, nextEventTs int32)
        int n = 0;
        for (int i=0;i<duration;i++) {
            n+=poisson.next();
        }
        if (n == 0) {
            return null;
        }
        long[] ret = new long[n*2];
        for (int i=0;i<n;i++) {
            ret[i*2] = curId + i;
            ret[i*2+1] = ((long)ts << 32) | ts;
        }
        curId += n;
        return ret;
    }

    void addNewOrders(long [] newOrders) throws Exception {
        int newNumActiveOrders = numActiveOrders + newOrders.length;
        if (newNumActiveOrders > 2000000000) {
            throw new Exception("too many active orders: " + newNumActiveOrders);
        }
        if (newNumActiveOrders > activeOrders.length) {
            long [] newActiveOrders = new long[(numActiveOrders+newOrders.length)*3/2];
            System.arraycopy(activeOrders, 0, newActiveOrders, 0, numActiveOrders);
            System.arraycopy(newOrders, 0, newActiveOrders, numActiveOrders, newOrders.length);
            activeOrders = newActiveOrders;
        } else {
            System.arraycopy(newOrders, 0, activeOrders, numActiveOrders, newOrders.length);
        }
        numActiveOrders = newNumActiveOrders;
    }

    void processEpoch(int ts, int duration) throws Exception {
        long [] newOrders = generate(ts, duration);
        addNewOrders(newOrders);
        for (int curTS = ts; curTS < ts + duration; curTS++) {

        }
    }

    Order getOrder(long id, long startTsAndNextTs, int curTs) {
        Order ret = new Order();
        ret.id = id;
        ret.userId = load.users.sample(id);
        ret.goodId = load.goods.sample(id);
        ret.merchantId = load.goods.getMerchatId(ret.goodId);
        ret.shipAddress = load.users.genAddress(ret.userId);
        long rs = Utils.nextRand(id);
        ret.shipMode = String.format("mode%d", rs % 10);
        ret.orderTs = (int)(startTsAndNextTs >>> 32);
        ret.orderDate = Integer.parseInt(dateFormatter.format(new Date(ret.orderTs)));
        rs = Utils.nextRand(rs);
        ret.paymentTs = ret.orderTs + paymentTime.sample(rs);
        rs = Utils.nextRand(rs);
        ret.deliveryStartTs = ret.paymentTs + deliveryStartTime.sample(rs);
        rs = Utils.nextRand(rs);
        ret.deliveryFinishTs = ret.deliveryStartTs + deliveryFinishTime.sample(rs);
        rs = Utils.nextRand(rs);
        ret.quantity = quantityDist.sample(rs);
        ret.price = load.goods.getPrice(ret.goodId);
        rs = Utils.nextRand(rs);
        ret.discount = discountDist.sample(rs) - 10;
        ret.revenue = ret.price * ret.quantity * (100 - ret.discount) / 100;
        if (curTs <= ret.paymentTs) {
            ret.state = State.CREATED.ordinal();
            ret.nextEventTs = ret.paymentTs;
        } else if (curTs <= ret.deliveryStartTs) {
            ret.state = State.PAYED.ordinal();
            ret.nextEventTs = ret.deliveryStartTs;
        } else if (curTs <= ret.deliveryFinishTs) {
            ret.state = State.DELIVER_STARTED.ordinal();
            ret.nextEventTs = ret.deliveryFinishTs;
        } else {
            ret.state = State.DELIVER_FINISHED.ordinal();
            ret.nextEventTs = 0;
        }
        return ret;
    }

    String getCreateTableSql() {
        String ret = "create table orders ("
                + "id bigint not null,"
                + "userid bigint not null,"
                + "goodid int not null,"
                + "merchantid int not null,"
                + "ship_address varchar(256) not null,"
                + "ship_mode varchar(32) not null,"
                + "order_date int not null,"
                + "order_ts int not null,"
                + "payment_ts int null,"
                + "delivery_start_ts int null,"
                + "delivery_finish_ts int null,"
                + "quantify int not null,"
                + "price int not null,"
                + "discount tinyint not null,"
                + "revenue int not null,"
                + "state tinyint not null";
        if (conf.getString("db.type").toLowerCase().startsWith("doris")) {
            ret += ") primary key(id) ";
            ret += String.format("DISTRIBUTED BY HASH(id) BUCKETS %d"
                    + " PROPERTIES(\"replication_num\" = \"%d\")",
                    conf.getInt("db.orders.bucket"),
                    conf.getInt("db.replication"));
        } else {
            ret += ", primary key(id))";
        }
        return ret;
    }

}
