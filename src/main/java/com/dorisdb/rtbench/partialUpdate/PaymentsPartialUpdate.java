package com.dorisdb.rtbench.partialUpdate;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.dorisdb.rtbench.DataOperation;
import com.dorisdb.rtbench.DataOperation.Op;
import com.dorisdb.rtbench.IntArray;
import com.dorisdb.rtbench.schema.Schema;
import static com.dorisdb.rtbench.schema.Columns.*;
import com.typesafe.config.Config;


public class PaymentsPartialUpdate {
    private static final Logger LOG = LogManager.getLogger(PaymentsPartialUpdate.class);

    PartialUpdateWorkload load;
    Config conf;
    boolean withDelete;
    boolean partial_update;
    boolean pureDataLoad;
    int repeatedColumnsNum;
    int[] numerousPartialColumnIdxes;
    int entryPerDay;
    double entryPerSecond;
    double deleteRatio = 0.02f;
    double updateRatio;
    String tableName;
    Schema schema;
    IntArray ids;
    int curId;

    public PaymentsPartialUpdate(PartialUpdateWorkload load, Config conf) throws Exception {
        this.load = load;
        this.conf = conf;
        this.withDelete = conf.getBoolean("with_delete");
        this.partial_update = conf.getBoolean("partial_update");
        this.pureDataLoad = conf.getBoolean("pure_data_load");
        this.updateRatio = conf.getDouble("update_ratio");
        this.repeatedColumnsNum = conf.getInt("repeated_columns_num");
        String[] numerousPartialColumnStrIdxes = conf.getString("numerous_partial_columns").split(",");
        this.numerousPartialColumnIdxes = new int[numerousPartialColumnStrIdxes.length];
        for (int i = 0; i < numerousPartialColumnStrIdxes.length; i++) {
            this.numerousPartialColumnIdxes[i] = Integer.parseInt(numerousPartialColumnStrIdxes[i]);
        }
        this.tableName = "payments_partial_update";
        this.entryPerDay = conf.getInt("record_per_day");
        this.entryPerSecond = (entryPerDay / (3600 * 24.0));
        this.ids = new IntArray(0, entryPerDay*2);
        this.curId = 0;

        java.util.ArrayList<com.dorisdb.rtbench.schema.Column> numerous_cols_list = new java.util.ArrayList<com.dorisdb.rtbench.schema.Column>();

        numerous_cols_list.add(IDINT("id"));

        com.dorisdb.rtbench.schema.Column[] prototypes = {
            DATE("setup_due_date", "2020-05-01", 100),
            DATE("original_due_date", "2020-05-01", 100),
            DATE("new_due_date", "2020-05-01", 100),
            DOUBLE("override_original_amount", 10000, 30000, 100.0),
            DOUBLE("original_amount", 10000, 30000, 100.0),
            DOUBLE("new_amount", 10000, 30000, 100.0),
            DOUBLE("original_principal", 10000, 30000, 100.0),
            U(DOUBLE("new_principal", 10000, 30000, 100.0)),
            DOUBLE("original_interest", 300, 1000, 100.0),
            U(DOUBLE("new_interest", 300, 1000, 100.0)),
            DOUBLE("override_original_principal", 10000, 30000, 100.0),
            DOUBLE("override_original_interest", 300,1000, 100.0),
            INT("loan_id", 1, 10000000),
            INT("transferred_scheduled_payment_id", 1, 10000000),
            INT("installment_id", 1, 10000000),
            STRING("set_by", "setby", 1, 10000),
            TINYINT("is_valid", 0, 2),
            INT("account_id", 1, 1000000),
            INT("setup_method_id", 1, 10),
            DATETIME("created_at", "2020-04-01 00:00:00", 3600*24*100),
            DATETIME("updated_at", "2020-04-01 00:00:00", 3600*24*100),
            DECIMAL("service_charge", 27, 9, 1000, 1000000),
            DECIMAL("late_fee", 27, 9, 1000, 100000),
            DATETIME("schedule_time", "2020-04-01 00:00:00", 3600*24*100),
            DECIMAL("penalty", 27, 9, 1000, 100000),
            DECIMAL("default_interest", 27, 9, 300, 1200),
            U(DATETIME("updated_at_sp", "2020-04-01 00:00:00", 3600*24*100))
        };

        for (com.dorisdb.rtbench.schema.Column prototype : prototypes) {
            for (int i = 0; i < repeatedColumnsNum; i++) {
                com.dorisdb.rtbench.schema.Column col = (com.dorisdb.rtbench.schema.Column)prototype.clone();
                col.setName(String.format("%s%02d", col.name, i));
                numerous_cols_list.add(col);
            }
        }

        com.dorisdb.rtbench.schema.Column[] numerous_value_cols = new com.dorisdb.rtbench.schema.Column[numerous_cols_list.size()];
        numerous_value_cols = numerous_cols_list.toArray(numerous_value_cols);
        schema = new Schema(numerous_value_cols);
    }

    int[] generate(int ts, int duration) {
        int[] ret = new int[(int)(duration*entryPerSecond)];
        for (int i = 0; i < ret.length; i++) {
            ret[i] = curId;
            curId++;
        }
        return ret;
    }

    void processEpoch(int ts, int duration) throws Exception {
        int nUpdate = 0;
        int[] newIds = generate(ts, duration);
        if (pureDataLoad == false) {
            // TODO(cbl): more reasonable calculation
            nUpdate = Math.min((int)(entryPerSecond*duration*updateRatio), (int)(ids.getSize()*updateRatio));
            if (nUpdate > 0) {
                int[] updateIds = ids.sample(nUpdate, ts);
                for (int i=0;i<updateIds.length;i++) {
                    DataOperation op = new DataOperation();
                    schema.genOpNumerousColumns(updateIds[i], updateIds[i], ts, op, numerousPartialColumnIdxes);
                    op.table = tableName;
                    op.op = Op.UPSERT;
                    load.handler.onDataOperation(op);
                }
            }
        } else {
            for (int i=0;i<newIds.length;i++) {
                DataOperation op = new DataOperation();
                schema.genOpNumerousColumns(newIds[i], newIds[i], 0, op, numerousPartialColumnIdxes);
                op.table = tableName;
                op.op = Op.INSERT;
                load.handler.onDataOperation(op);
            }
        }
        ids.append(newIds, 0, newIds.length);
        LOG.info(String.format("epoch #update:%d #new:%d #current:%d", nUpdate, newIds.length, ids.getSize()));
    }

    String getCreateTableSql() {
        if (conf.getString("db.type").toLowerCase().startsWith("doris")) {
            return schema.getCreateTableDorisDB(tableName, conf.getInt("db.payments.bucket"), conf.getInt("db.replication"));
        } else {
            return schema.getCreateTableMySql(tableName);
        }
    }
}
