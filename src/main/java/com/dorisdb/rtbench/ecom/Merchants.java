package com.dorisdb.rtbench.ecom;

import java.util.Random;

import com.dorisdb.rtbench.DataOperation;
import com.dorisdb.rtbench.Locations;
import com.dorisdb.rtbench.Utils;
import com.dorisdb.rtbench.WorkloadHandler;
import com.typesafe.config.Config;

public class Merchants {
    EcomWorkload load;
    Config conf;
    Random rand;
    long num;

    public Merchants(EcomWorkload load, Config conf) {
        this.load = load;
        this.conf = conf;
        long ordersPerDay = conf.getLong("record_per_day");
        this.num = ordersPerDay / 100;
    }

    public long size() {
        return num;
    }

    private static final long seed = 19037806123321843L;

    public long sample(long id) {
        return Utils.nextRand(id, seed) % num;
    }

    Merchant get(long id) {
        Merchant ret = new Merchant();
        ret.id = (int)id;
        ret.name = String.format("merchant%d", id);
        ret.address = String.format("address m%d", id);
        long rs = Utils.nextRand(id);
        Locations.Location lc = load.locations.sample(rs);
        ret.city = lc.city;
        ret.province = lc.province;
        ret.country = lc.country;
        rs = Utils.nextRand(rs);
        ret.phone = String.format("1%010d", rs % 10000000000L);
        return ret;
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
            ret += ") unique key(id) ";
            ret += String.format("DISTRIBUTED BY HASH(id) BUCKETS %d"
                    + " PROPERTIES(\"replication_num\" = \"%d\")",
                    conf.getInt("db.merchants.bucket"),
                    conf.getInt("db.replication"));
        } else {
            ret += ", primary key(id))";
        }
        return ret;
    }

    static final String tableName = "merchants";
    static final String[] allColumnNames = {"id", "name", "address", "city", "province", "country", "phone"};
    static final int[] keyColumnIdxs = {0};

    void loadAllData(WorkloadHandler handler) throws Exception {
        for (long i=0;i<num;i++) {
            DataOperation op = new DataOperation();
            op.table = tableName;
            op.op = DataOperation.Op.INSERT;
            op.fullFieldNames = allColumnNames;
            op.keyFieldIdxs = keyColumnIdxs;
            Merchant m = get(i);
            op.fullFields = new Object[] {
                    m.id,
                    m.name,
                    m.address,
                    m.city,
                    m.province,
                    m.country,
                    m.phone
            };
            handler.onDataOperation(op);
        }
    }

}
