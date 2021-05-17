package com.decster.rtbench.ecom;

import com.decster.rtbench.DataOperation;
import com.decster.rtbench.Utils;
import com.decster.rtbench.Utils.PowerDist;
import com.decster.rtbench.WorkloadHandler;
import com.typesafe.config.Config;

public class Goods {
    EcomWorkload load;
    Config conf;
    long num;
    int numCategory;
    int numSubCategory;
    int numBrand;

    static final int GOODS_PER_MERCHANT = 10;
    static final PowerDist prices = new PowerDist(2000, 400000, 1.5f);

    public Goods(EcomWorkload load, Merchants merchants, Config conf) {
        this.load = load;
        this.conf = conf;
        this.num = merchants.size() * GOODS_PER_MERCHANT;
        this.numCategory = (int)(3 * Math.log10(num));
        this.numSubCategory = numCategory * 50;
        this.numBrand = numCategory * 234;
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

    Good get(long id) {
        Good ret = new Good();
        ret.id = (int)id;
        ret.name = String.format("item%d", id);
        long rs = Utils.nextRand(id, seed);
        long scid = rs % numSubCategory;
        ret.subcategory = String.format("s%d", scid);
        long cateid = scid % numCategory;
        ret.categroy = String.format("c%d", cateid);
        rs = Utils.nextRand(rs);
        ret.brand = String.format("c%db%d", cateid, rs % numBrand);
        rs = Utils.nextRand(rs);
        ret.type  = String.format("t%d", rs % (scid % 10 + 1));
        return ret;
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

    static final String tableName = "goods";
    static final String[] allColumnNames = {"id", "name", "category", "subcategory", "brand", "type"};

    void loadAllData(WorkloadHandler handler) throws Exception {
        for (long i=0;i<num;i++) {
            DataOperation op = new DataOperation();
            op.table = tableName;
            op.op = DataOperation.Op.INSERT;
            op.fieldNames = allColumnNames;
            Good good = get(i);
            op.fields = new Object[] {
                    good.id,
                    good.name,
                    good.categroy,
                    good.subcategory,
                    good.brand,
                    good.type
            };
            handler.onDataOperation(op);
        }
    }
}
