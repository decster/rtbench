package com.dorisdb.rtbench.ecom;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.dorisdb.rtbench.DataOperation;
import com.dorisdb.rtbench.IntArray;
import com.dorisdb.rtbench.LongArray;
import com.dorisdb.rtbench.Utils;
import com.dorisdb.rtbench.DataOperation.Op;
import com.dorisdb.rtbench.Utils.PowerDist;
import com.typesafe.config.Config;

public class Orders {
    private static final Logger LOG = LogManager.getLogger(Orders.class);

    EcomWorkload load;
    Config conf;
    int ordersPerDay;
    double ordersPerSecond;
    long curId;
    OrderArray activeOrders;
    IntArray[] nextEventTsIndex;
    int indexingStartTs;
    int indexingEndTs;
    int indexingDuration;
    boolean partial_update;

    static final SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyyMMdd");
    static final PowerDist paymentTime = new PowerDist(10, 300);
    static final PowerDist deliveryStartTime = new PowerDist(3600, 36000);
    static final PowerDist deliveryFinishTime = new PowerDist(3600 * 20, 3600 * 24 * 5);
    static final PowerDist quantityDist = new PowerDist(1, 20, 2.5f);
    static final PowerDist discountDist = new PowerDist(10, 60, 3);
    Utils.Poisson poisson;

    static enum State {
        CREATED, PAYED, DELIVER_STARTED, DELIVER_FINISHED
    }

    public Orders(EcomWorkload load, Config conf) {
        this.load = load;
        this.conf = conf;
        this.ordersPerDay = conf.getInt("record_per_day");
        this.ordersPerSecond = (ordersPerDay / (3600 * 24.0));
        this.curId = conf.getLong("generator_id_start");
        this.activeOrders = new OrderArray(ordersPerDay * 4);
        this.partial_update = conf.getBoolean("partial_update");
        nextEventTsIndex = new IntArray[0];
        indexingStartTs = 0;
        indexingEndTs = 0;
        if (conf.hasPath("order_index_duration")) {
            indexingDuration = (int)conf.getDuration("order_index_duration").getSeconds();
        } else {
            indexingDuration = (int)conf.getDuration("epoch_duration").getSeconds();
        }
        poisson = new Utils.Poisson(ordersPerSecond, 1);
    }

    long[] generate(int ts, int duration) {
        // to save memory, each order is represented as 128bit integer:
        // (orderId int64, startTs int32, nextEventTs int32)
        int total = 0;
        int[] numPerTs = new int[duration];
        for (int i = 0; i < duration; i++) {
            int tmp = poisson.next();
            numPerTs[i] = tmp;
            total += tmp;
        }
        if (total == 0) {
            return null;
        }
        long[] ret = new long[total * 2];
        int idx = 0;
        for (int i = 0; i < duration; i++) {
            for (int j = 0; j < numPerTs[i]; j++) {
                ret[idx] = curId;
                curId++;
                idx++;
                ret[idx] = ((long) (ts + i) << 32) | (ts + i);
                idx++;
            }
        }
        return ret;
    }

    void indexingIncremental(int start, int end) throws Exception {
        for (int i = start; i < end; i++) {
            int nextTs = activeOrders.getNextEventTs(i);
            if (nextTs <= 0) {
                continue;
            }
            if (nextTs < indexingStartTs) {
                // should not happen
                throw new Exception("nextTs < indexingStartTs");
            } else if (nextTs < indexingEndTs) {
                nextEventTsIndex[nextTs - indexingStartTs].append(i);
            }
        }
    }

    void indexingFull() throws Exception {
        nextEventTsIndex = new IntArray[indexingEndTs - indexingStartTs];
        for (int i = 0; i < nextEventTsIndex.length; i++) {
            nextEventTsIndex[i] = new IntArray(0, (int) (ordersPerSecond * 2));
        }
        IntArray deleted = new IntArray(0, 10000);
        int idAfterDelete = 0;
        int total = 0;
        for (int i = 0; i < activeOrders.getSize(); i++) {
            int nextTs = activeOrders.getNextEventTs(i);
            if (nextTs <= 0) {
                // order expired, mark delete
                deleted.append(i);
                continue;
            }
            if (nextTs < indexingStartTs) {
                // should not happen
                throw new Exception("nextTs < indexingStartTs");
            } else if (nextTs < indexingEndTs) {
                nextEventTsIndex[nextTs - indexingStartTs].append(idAfterDelete);
                total++;
            }
            idAfterDelete++;
        }
        if (deleted.getSize() > 0) {
            activeOrders.remove(deleted);
        }
        LOG.info(String.format("full indexing [%s, %s) indexed: %d deleted: %d", Utils.tsToString(indexingStartTs),
                Utils.tsToString(indexingEndTs), total, deleted.getSize()));
    }

    void processEpoch(int ts, int duration) throws Exception {
        long[] newOrders = generate(ts, duration);
        int oldSize = activeOrders.getSize();
        if (newOrders != null) {
            activeOrders.append(newOrders, 0, newOrders.length);
            LOG.info(String.format("Epoch:%s new orders: %d total: %d", Utils.tsToString(ts), newOrders.length/2,
                    activeOrders.getSize()));
        }
        if (indexingEndTs == 0 || ts + duration > indexingEndTs) {
            indexingStartTs = ts;
            indexingEndTs = ts + Math.max(indexingDuration, duration);
            indexingFull();
        } else if (ts >= indexingStartTs && ts + duration <= indexingEndTs) {
            indexingIncremental(oldSize, activeOrders.getSize());
        } else {
            throw new Exception("bad epoch ts for indexing");
        }
        for (int i=0;i<duration;i++) {
            int curTs = ts + i;
            processEvents(curTs);
        }
    }

    static final String tableName = "orders";
    static final String[] allColumnNames = {
            "id", "userid", "goodid", "merchantid", "ship_address",
            "ship_mode", "order_date", "order_ts", "payment_ts",
            "delivery_start_ts", "delivery_finish_ts", "quantify",
            "price", "discount", "revenue", "state"};
    static final int[] keyColumnIdxs = {0};
    static final int[] updatePayedIdxs = {8, 15};
    static final int[] updateDeliverStartedIdxs = {9, 15};
    static final int[] updateDeliverFinishedIdxs = {10, 15};

    void processEvents(int ts) throws Exception {
        IntArray idxs = nextEventTsIndex[ts-indexingStartTs];
        int nInsert = 0;
        int nUpdate = 0;
        Order order = new Order();
        for (int i=0;i<idxs.getSize();i++) {
            int idx = idxs.get(i);
            long orderId = activeOrders.getId(idx);
            int startTs = activeOrders.getStartTs(idx);
            getOrder(order, orderId, startTs, ts);
            DataOperation op = new DataOperation();
            op.table = tableName;
            op.keyFieldIdxs = keyColumnIdxs;
            // upsert is used for both insert & update, so:
            // mysql handler can use insert xx on duplicate key xx,
            // file handler can just upsert
            // doris handler can just upsert
            if (order.orderTs == ts) {
                // create new order
                op.op = Op.UPSERT;
                nInsert++;
            } else {
                // update order
                op.op = Op.UPSERT;
                nUpdate++;
                if (order.state == State.PAYED.ordinal()) {
                    op.updateFieldIdxs = updatePayedIdxs;
                } else if (order.state == State.DELIVER_STARTED.ordinal()) {
                    op.updateFieldIdxs = updateDeliverStartedIdxs;
                } else if (order.state == State.DELIVER_FINISHED.ordinal()) {
                    op.updateFieldIdxs = updateDeliverFinishedIdxs;
                }
            }
            op.fullFieldNames = allColumnNames;
            op.keyFieldIdxs = keyColumnIdxs;
            if (partial_update) {
                op.fullFields = new Object[] {
                    order.id,
                    order.state
                };
            } else {
                op.fullFields = new Object[] {
                    order.id,
                    order.userId,
                    order.goodId,
                    order.merchantId,
                    order.shipAddress,
                    order.shipMode,
                    order.orderDate,
                    order.orderTs,
                    order.state >= State.PAYED.ordinal() ? order.paymentTs : null,
                    order.state >= State.DELIVER_STARTED.ordinal() ? order.deliveryStartTs : null,
                    order.state >= State.DELIVER_FINISHED.ordinal() ? order.deliveryFinishTs : null,
                    order.quantity,
                    order.price,
                    order.discount,
                    order.revenue,
                    order.state
                };
            }
            load.handler.onDataOperation(op);
            if (order.nextEventTs == 0) {
                activeOrders.setNextEventTs(idx, order.nextEventTs);
            } else if (order.nextEventTs > ts) {
                activeOrders.setNextEventTs(idx, order.nextEventTs);
                if (order.nextEventTs >= indexingStartTs && order.nextEventTs < indexingEndTs) {
                    nextEventTsIndex[order.nextEventTs-indexingStartTs].append(idx);
                }
            } else {
                throw new Exception("nextEvent <= currentTs");
            }
        }
//        if (nInsert + nUpdate > 0) {
//            LOG.info(String.format("order events at %s inserts: %d updates: %d", Utils.tsToString(ts), nInsert, nUpdate));
//        }
    }

    static Calendar dateFormatCalender = Calendar.getInstance();

    static int getDateInt(int ts) {
        dateFormatCalender.setTimeInMillis(ts * 1000L);
        return dateFormatCalender.get(Calendar.YEAR) * 10000 + (dateFormatCalender.get(Calendar.MONTH)+1) * 100 + dateFormatCalender.get(Calendar.DAY_OF_MONTH);
    }

    Order getOrder(Order ret, long id, int orderTs, int curTs) {
        ret.id = id;
        ret.userId = load.users.sample(id);
        ret.goodId = load.goods.sample(id);
        ret.merchantId = load.goods.getMerchatId(ret.goodId);
        ret.shipAddress = load.users.genAddress(ret.userId);
        long rs = Utils.nextRand(id);
        ret.shipMode = "mode" + (rs % 10);
        ret.orderTs = orderTs;
        ret.orderDate = getDateInt(ret.orderTs);
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
        if (curTs < ret.paymentTs) {
            ret.state = State.CREATED.ordinal();
            ret.nextEventTs = ret.paymentTs;
        } else if (curTs < ret.deliveryStartTs) {
            ret.state = State.PAYED.ordinal();
            ret.nextEventTs = ret.deliveryStartTs;
        } else if (curTs < ret.deliveryFinishTs) {
            ret.state = State.DELIVER_STARTED.ordinal();
            ret.nextEventTs = ret.deliveryFinishTs;
        } else {
            ret.state = State.DELIVER_FINISHED.ordinal();
            ret.nextEventTs = 0;
        }
        return ret;
    }

    String getCreateTableSql() {
        String ret = "create table if not exists orders ("
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
            ret += String.format(") %s key(id) ", conf.getString("handler.dorisdb.table_key_type")) ;
            ret += String.format("DISTRIBUTED BY HASH(id) BUCKETS %d" + " PROPERTIES(\"replication_num\" = \"%d\")",
                    conf.getInt("db.orders.bucket"), conf.getInt("db.replication"));
        } else {
            ret += ", primary key(id))";
        }
        return ret;
    }
}
