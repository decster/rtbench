package com.decster.rtbench.ecom;

import com.decster.rtbench.DataOperation;
import com.decster.rtbench.Utils;
import com.decster.rtbench.WorkloadHandler;
import com.typesafe.config.Config;

public class Users {
    EcomWorkload load;
    Config conf;
    long num;

    public Users(EcomWorkload load, Config conf) {
        this.load = load;
        this.conf = conf;
        long ordersPerDay = conf.getLong("orders_per_day");
        this.num = ordersPerDay * 10;
    }

    public long size() {
        return num;
    }

    private static final long seed = 5616088622706043L;

    public long sample(long id) {
        return Utils.nextRand(id, seed) % num;
    }

    public String genAddress(long userId) {
        return String.format("address %d", userId);
    }

    User getUser(long id) {
        User ret = new User();
        ret.id = id;
        ret.name = String.format("user%d", id);
        long rs = Utils.nextRand(id);
        if (rs % 51 == 0) {
            ret.age = null;
        } else {
            ret.age = (short)((rs % 47) + 10);
        }
        rs = Utils.nextRand(rs);
        if (rs % 100 == 0) {
            ret.sex = null;
        } else {
            ret.sex = (byte)(rs % 2);
        }
        ret.address = genAddress(id);
        rs = Utils.nextRand(rs);
        long cid = rs % 1135;
        ret.city = String.format("c%d", cid);
        long pid = cid % 43;
        ret.province = String.format("p%d", pid);
        ret.country = pid == 42 ? "other" : "china";
        return ret;
    }

    String getCreateTableSql() {
        String ret = "create table users ("
                + "id bigint not null,"
                + "name varchar(64) not null,"
                + "age smallint null,"
                + "sex tinyint null,"
                + "address varchar(256) not null,"
                + "city varchar(64) not null,"
                + "province varchar(64) not null,"
                + "country varchar(64) not null";
        if (conf.getString("db.type").toLowerCase().startsWith("doris")) {
            ret += ") primary key(id) ";
            ret += String.format("DISTRIBUTED BY HASH(id) BUCKETS %d"
                    + " PROPERTIES(\"replication_num\" = \"%d\")",
                    conf.getInt("db.users.bucket"),
                    conf.getInt("db.replication"));
        } else {
            ret += ", primary key(id))";
        }
        return ret;
    }

    static final String tableName = "users";
    static final String[] allColumnNames = {"id", "name", "age", "sex", "address", "city", "province", "country"};

    void loadAllData(WorkloadHandler handler) throws Exception {
        for (long i=0;i<num;i++) {
            DataOperation op = new DataOperation();
            op.table = tableName;
            op.fieldNames = allColumnNames;
            handler.onDataOperation(op);
        }
    }
}
