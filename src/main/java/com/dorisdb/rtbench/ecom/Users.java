package com.dorisdb.rtbench.ecom;

import com.dorisdb.rtbench.DataOperation;
import com.dorisdb.rtbench.Locations;
import com.dorisdb.rtbench.Utils;
import com.dorisdb.rtbench.WorkloadHandler;
import com.typesafe.config.Config;

public class Users {
    EcomWorkload load;
    Config conf;
    long num;

    public Users(EcomWorkload load, Config conf) {
        this.load = load;
        this.conf = conf;
        long ordersPerDay = conf.getLong("record_per_day");
        this.num = ordersPerDay * 5;
    }

    public long size() {
        return num;
    }

    private static final long seed = 5616088622706043L;

    public long sample(long id) {
        return Utils.nextRand(id, seed) % num;
    }

    public String genAddress(long userId) {
        return "address " +  userId;
    }

    User get(long id) {
        User ret = new User();
        ret.id = id;
        ret.name = "user" + id;
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
        Locations.Location lc = load.locations.sample(rs);
        ret.city = lc.city;
        ret.province = lc.province;
        ret.country = lc.country;
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
            ret += ") unique key(id) ";
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
    static final int[] keyColumnIdxs = {0};

    void loadAllData(WorkloadHandler handler) throws Exception {
        for (long i=0;i<num;i++) {
            DataOperation op = new DataOperation();
            op.table = tableName;
            op.op = DataOperation.Op.INSERT;
            op.fullFieldNames = allColumnNames;
            op.keyFieldIdxs = keyColumnIdxs;
            User user = get(i);
            op.fullFields = new Object[] {
                    user.id,
                    user.name,
                    user.age,
                    user.sex,
                    user.address,
                    user.city,
                    user.province,
                    user.country
            };
            handler.onDataOperation(op);
        }
    }
}
