package com.decster.rtbench.ecom;

import com.decster.rtbench.Utils;
import com.typesafe.config.Config;

public class Users {
    Config conf;
    long num;

    public Users(Config conf) {
        this.conf = conf;
        long ordersPerDay = conf.getLong("orders_per_day");
        this.num = ordersPerDay * 10;
    }

    public long size() {
        return num;
    }

    private static final long seed = 5616088622706043L;

    long sample(long id) {
        return Utils.nextRand(id, seed) % num;
    }
}
