package com.decster.rtbench.ecom;

import com.typesafe.config.Config;

public class Orders {
    Config conf;
    long ordersPerDay;
    int ordersPerSecond;
    long curId;

    public Orders(Config conf) {
        this.conf = conf;
        this.ordersPerDay = conf.getLong("orders_per_day");
        this.ordersPerSecond = (int)(ordersPerDay / (3600 * 24));
        this.curId = 1;
    }

    long[] generate(int ts) {
        // to save memory, each order is represented as 128bit integer:
        // (orderId int64, startTs int32, nextEventTs int32)
        long[] ret = new long[ordersPerSecond*2];
        for (int i=0;i<ordersPerSecond;i++) {
            ret[i*2] = curId + i;
            ret[i*2+1] = ((long)ts << 32) | ts;
        }
        curId += ordersPerSecond;
        return ret;
    }
}
