package com.decster.rtbench.ecom;

import com.decster.rtbench.Utils;
import com.decster.rtbench.Utils.PowerDist;
import com.typesafe.config.Config;

public class Goods {
    EcomWorkload load;
    Config conf;
    long num;

    static final int GOODS_PER_MERCHANT = 10;
    static final PowerDist prices = new PowerDist(2000, 400000, 1.5f);

    public Goods(EcomWorkload load, Merchants merchants, Config conf) {
        this.load = load;
        this.conf = conf;
        this.num = merchants.size() * GOODS_PER_MERCHANT;
    }

    public long size() {
        return num;
    }

    private static final long seed = 6235285457965874L;

    int sample(long id) {
        return (int)(Utils.nextRand(id, seed) % num);
    }

    int getMerchatId(int goodsId) {
        return (int)((Utils.nextRand(goodsId) % num) / GOODS_PER_MERCHANT);
    }

    int getPrice(int goodsId) {
        return prices.sample(Utils.nextRand(goodsId));
    }

    String getCreateTableSql() {
        String ret = "create table goods ("
                + "id int not null,"
                + "name varchar(64) not null,"
                + "category varchar(64) not null,"
                + "subcategory varchar(64) not null,"
                + "brand varchar(64) not null,"
                + "type varchar(64) not null";
        if (conf.getString("db.type").toLowerCase().startsWith("doris")) {
            ret += ") primary key(id) ";
            ret += String.format("DISTRIBUTED BY HASH(id) BUCKETS %d"
                    + " PROPERTIES(\"replication_num\" = \"%d\")",
                    conf.getInt("db.goods.bucket"),
                    conf.getInt("db.replication"));
        } else {
            ret += ", primary key(id))";
        }
        return ret;
    }

}
