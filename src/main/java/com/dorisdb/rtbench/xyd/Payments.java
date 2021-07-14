package com.dorisdb.rtbench.xyd;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.dorisdb.rtbench.Utils;
import com.typesafe.config.Config;


public class Payments {
    private static final Logger LOG = LogManager.getLogger(Payments.class);

    XydWorkload load;
    Config conf;
    int entryPerDay;
    double entryPerSecond;
    String tableName;

    public Payments(XydWorkload load, Config conf) {
        this.load = load;
        this.conf = conf;
        this.tableName = "payments";
        this.entryPerDay = conf.getInt("orders_per_day");
        this.entryPerSecond = (entryPerDay / (3600 * 24.0));
    }

    static class Payment {
        int idx;
        int setup_due_date;
        int original_due_date;
        int new_due_date;
        double override_original_amount;
        double original_amount;
        double new_amount;
        double original_principal;
        double new_principal;
        double original_interest;
        double new_interest;
        double override_original_principal;
        double override_original_interest;
        int loan_id;
        int transferred_scheduled_payment_id;
        int installment_id;
        String set_by;
        byte is_valid;
        int account_id;
        int setup_method_id;
        long created_at; // datetime
        long updated_at; // datetime
        long service_charge; // decimal
        long late_fee; // decimal
        long schedule_time; // datetime
        long penalty; // decimal
        long default_interest; // decimal
        long updated_at_sp; // datetime

        void gen(int idx, int update_idx) {
            this.idx = idx;
            long rs = Utils.nextRand(idx);
            setup_due_date = (int)(rs % 1000);
            rs = Utils.nextRand(rs);
            original_due_date = (int)(rs % 1000);
            rs = Utils.nextRand(rs);
            new_due_date = (int)(rs % 1000);
            rs = Utils.nextRand(rs);
            override_original_amount = (rs % 10000) / 100.0;
            rs = Utils.nextRand(rs);
            original_amount = (rs % 10000) / 100.0;
            rs = Utils.nextRand(rs);
            new_amount = (rs % 10000) / 100.0;
            rs = Utils.nextRand(rs);
            original_principal = (rs % 10000) / 100.0;
            rs = Utils.nextRand(rs);
            new_principal = (rs % 10000) / 100.0;
            rs = Utils.nextRand(rs);
            original_interest = (rs % 1000) / 100.0;
            rs = Utils.nextRand(rs);
            new_interest = (rs % 1000) / 100.0;
            rs = Utils.nextRand(rs);
            override_original_principal = (rs % 10000) / 100.0;
            rs = Utils.nextRand(rs);
            override_original_interest = (rs % 1000) / 100.0;
            rs = Utils.nextRand(rs);
            loan_id = (int)(rs % 100000);
            rs = Utils.nextRand(rs);
            transferred_scheduled_payment_id = (int)(rs % 100000);
            rs = Utils.nextRand(rs);
            installment_id = (int)(rs % 100000);
            rs = Utils.nextRand(rs);
            set_by = String.format("str%d", rs % 100000);
            rs = Utils.nextRand(rs);
            is_valid = rs % 10 == 0 ? (byte)0 : (byte)1;
            rs = Utils.nextRand(rs);
            account_id = (int)(rs % 100000);
            rs = Utils.nextRand(rs);
            setup_method_id = (int)(rs % 100000);
            rs = Utils.nextRand(rs);
            created_at = (rs % 10000000);
            rs = Utils.nextRand(rs);
            updated_at = (rs % 10000000) + update_idx;
            rs = Utils.nextRand(rs);
            service_charge = rs % 10000;
            rs = Utils.nextRand(rs);
            late_fee = rs % 10000;
            rs = Utils.nextRand(rs);
            schedule_time = (int)(rs % 10000000);
            rs = Utils.nextRand(rs);
            penalty = rs % 10000;
            rs = Utils.nextRand(rs);
            default_interest = rs % 1000;
            rs = Utils.nextRand(rs);
            updated_at_sp = (int)(rs % 10000000) + update_idx;
        }

        static SimpleDateFormat dateTimeFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        static SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");
        static long baseTs = 1626268371000L;

        static String fdatetime(long ts) {
            return dateTimeFormatter.format(new Date(baseTs + ts * 1000));
        }

        static String fdate(int ts) {
            return dateTimeFormatter.format(new Date(baseTs + ts * 24 * 3600 * 1000));
        }
    }

    static final String[] allColumnNames = {
            "id", "userid", "goodid", "merchantid", "ship_address",
            "ship_mode", "order_date", "order_ts", "payment_ts",
            "delivery_start_ts", "delivery_finish_ts", "quantify",
            "price", "discount", "revenue", "state"};
    static final int[] keyColumnIdxs = {0};
    static final int[] updatePayedIdxs = {8, 15};

    void processEpoch(int ts, int duration) throws Exception {
    }

    String getCreateTableSql() {
        String ret = "CREATE TABLE t_scheduled_payments ("
                + "  id int NOT NULL,"
                + "  setup_due_date date NULL,"
                + "  original_due_date date NULL,"
                + "  new_due_date date NULL,"
                + "  override_original_amount double NULL,"
                + "  original_amount double NULL,"
                + "  new_amount double NULL,"
                + "  original_principal double NULL,"
                + "  new_principal double NULL,"
                + "  original_interest double NULL,"
                + "  new_interest double NULL,"
                + "  override_original_principal double NULL,"
                + "  override_original_interest double NULL,"
                + "  loan_id int NOT NULL,"
                + "  transferred_scheduled_payment_id int NULL,"
                + "  installment_id int NULL,"
                + "  set_by string NULL,"
                + "  is_valid tinyint(1) NULL,"
                + "  account_id int NOT NULL,"
                + "  setup_method_id int NULL,"
                + "  created_at datetime NOT NULL,"
                + "  updated_at datetime NOT NULL,"
                + "  service_charge decimal(9,2) DEFAULT '0.00',"
                + "  late_fee decimal(9,2) DEFAULT '0.00',"
                + "  schedule_time datetime NULL COMMENT '',"
                + "  penalty decimal(9,2) NULL COMMENT '',"
                + "  default_interest decimal(9,2) DEFAULT '0.00' COMMENT '',"
                + "  updated_at_sp datetime NOT NULL";
        if (conf.getString("db.type").toLowerCase().startsWith("doris")) {
            ret += ") primary key(id) ";
            ret += String.format("DISTRIBUTED BY HASH(id) BUCKETS %d" + " PROPERTIES(\"replication_num\" = \"%d\")",
                    conf.getInt("db.payments.bucket"), conf.getInt("db.replication"));
        } else {
            ret += ", primary key(id))";
        }
        return ret;
    }

}
