package com.decster.rtbench.ecom;

import com.decster.rtbench.Utils;
import com.typesafe.config.Config;

public class Goods {
    Config conf;
    long num;

    public Goods(Merchants merchants, Config conf) {
        this.conf = conf;
        this.num = merchants.size() * 10;
    }

    public long size() {
        return num;
    }

    private static final long seed = 6235285457965874L;

    int sample(long id) {
        return (int)(Utils.nextRand(id, seed) % num);
    }
}
