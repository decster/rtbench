package com.decster.rtbench.ecom;

import java.util.Random;

import com.typesafe.config.Config;

public class Merchants {
    Config conf;
    Random rand;
    long num;

    public Merchants(Config conf) {
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
}
