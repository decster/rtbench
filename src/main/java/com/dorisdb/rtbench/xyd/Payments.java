package com.dorisdb.rtbench.xyd;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.dorisdb.rtbench.DataOperation;
import com.dorisdb.rtbench.DataOperation.Op;
import com.dorisdb.rtbench.IntArray;
import com.dorisdb.rtbench.schema.Schema;
import static com.dorisdb.rtbench.schema.Columns.*;
import com.typesafe.config.Config;


public class Payments {
    private static final Logger LOG = LogManager.getLogger(Payments.class);

    XydWorkload load;
    Config conf;
    boolean withDelete;
    boolean partial_update;
    boolean numerous_columns;
    int[] numerousPartialColumnIdxes;
    int entryPerDay;
    double entryPerSecond;
    double deleteRatio = 0.02f;
    double updateRatio = 0.2f;
    String tableName;
    Schema schema;
    IntArray ids;
    int curId;

    public Payments(XydWorkload load, Config conf) throws Exception {
        this.load = load;
        this.conf = conf;
        this.withDelete = conf.getBoolean("with_delete");
        this.partial_update = conf.getBoolean("partial_update");
        this.numerous_columns = conf.getBoolean("numerous_columns");
        String[] numerousPartialColumnStrIdxes = conf.getString("numerous_partial_columns").split(",");
        this.numerousPartialColumnIdxes = new int[numerousPartialColumnStrIdxes.length];
        for (int i = 0; i < numerousPartialColumnStrIdxes.length; i++) {
            this.numerousPartialColumnIdxes[i] = Integer.parseInt(numerousPartialColumnStrIdxes[i]);
        }
        this.tableName = "payments";
        this.entryPerDay = conf.getInt("record_per_day");
        this.entryPerSecond = (entryPerDay / (3600 * 24.0));
        this.ids = new IntArray(0, entryPerDay*2);
        this.curId = 0;

        String schema_type = conf.getString("db.payments.schema_type");

        if (schema_type.equals("ordinary_cols")) {
            schema = new Schema(
                IDINT("id"),
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
                DECIMAL("service_charge", 9, 2, 1000, 1000000),
                DECIMAL("late_fee", 9, 2, 1000, 100000),
                DATETIME("schedule_time", "2020-04-01 00:00:00", 3600*24*100),
                DECIMAL("penalty", 9, 2, 1000, 100000),
                DECIMAL("default_interest", 9, 2, 300, 1200),
                U(DATETIME("updated_at_sp", "2020-04-01 00:00:00", 3600*24*100))
            );
        } else if (schema_type.equals("numerous_key_cols")) {
            schema = new Schema(
                K(IDINT("id")),
                K(DATE("setup_due_date0", "2020-05-01", 100)),
                K(DATE("setup_due_date1", "2020-05-01", 100)),
                K(DATE("setup_due_date2", "2020-05-01", 100)),
                K(DATE("original_due_date", "2020-05-01", 100)),
                K(DATE("new_due_date", "2020-05-01", 100)),
                K(INT("loan_id", 1, 10000000)),
                K(INT("transferred_scheduled_payment_id", 1, 10000000)),
                K(INT("installment_id", 1, 10000000)),
                K(STRING("set_by", "setby", 1, 10000)),
                K(TINYINT("is_valid", 0, 2)),
                K(INT("account_id", 1, 1000000)),
                K(INT("setup_method_id", 1, 10)),
                K(DATETIME("created_at", "2020-04-01 00:00:00", 3600*24*100)),
                K(DATETIME("updated_at", "2020-04-01 00:00:00", 3600*24*100)),
                K(DATETIME("schedule_time", "2020-04-01 00:00:00", 3600*24*100)),
                K(U(DATETIME("updated_at_sp", "2020-04-01 00:00:00", 3600*24*100))),
                K(DATETIME("schedule_time1", "2020-04-01 00:00:00", 3600*24*100)),
                K(DATETIME("schedule_time2", "2020-04-01 00:00:00", 3600*24*100)),
                K(DATETIME("schedule_time3", "2020-04-01 00:00:00", 3600*24*100)),
                K(DATETIME("schedule_time4", "2020-04-01 00:00:00", 3600*24*100)),
                DOUBLE("override_original_amount", 10000, 30000, 100.0),
                DOUBLE("original_amount", 10000, 30000, 100.0),
                DOUBLE("new_amount", 10000, 30000, 100.0),
                DOUBLE("original_principal", 10000, 30000, 100.0),
                U(DOUBLE("new_principal", 10000, 30000, 100.0)),
                DOUBLE("original_interest", 300, 1000, 100.0),
                U(DOUBLE("new_interest", 300, 1000, 100.0)),
                DOUBLE("override_original_principal", 10000, 30000, 100.0),
                DOUBLE("override_original_interest", 300,1000, 100.0),
                DECIMAL("service_charge", 9, 2, 1000, 1000000),
                DECIMAL("late_fee", 9, 2, 1000, 100000),
                DECIMAL("penalty", 9, 2, 1000, 100000),
                DECIMAL("default_interest", 9, 2, 300, 1200)
            );
        } else if (schema_type.equals("numerous_value_cols")) {
            java.util.ArrayList<com.dorisdb.rtbench.schema.Column> numerous_cols_list = new java.util.ArrayList<com.dorisdb.rtbench.schema.Column>();

            numerous_cols_list.add(IDINT("id"));

            final int repeat_value_num = 33;

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
                DECIMAL("service_charge", 9, 2, 1000, 1000000),
                DECIMAL("late_fee", 9, 2, 1000, 100000),
                DATETIME("schedule_time", "2020-04-01 00:00:00", 3600*24*100),
                DECIMAL("penalty", 9, 2, 1000, 100000),
                DECIMAL("default_interest", 9, 2, 300, 1200),
                U(DATETIME("updated_at_sp", "2020-04-01 00:00:00", 3600*24*100))
            };

            for (com.dorisdb.rtbench.schema.Column prototype : prototypes) {
                for (int i = 0; i < repeat_value_num; i++) {
                    com.dorisdb.rtbench.schema.Column col = (com.dorisdb.rtbench.schema.Column)prototype.clone();
                    col.setName(String.format("%s%02d", col.name, i));
                    numerous_cols_list.add(col);
                }
            }

            com.dorisdb.rtbench.schema.Column[] numerous_value_cols = new com.dorisdb.rtbench.schema.Column[numerous_cols_list.size()];
            numerous_value_cols = numerous_cols_list.toArray(numerous_value_cols);
            schema = new Schema(numerous_value_cols);
        }
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
        int nDelete = 0;
        if (withDelete) {
            // TODO(cbl): more reasonable calculation
            nDelete = Math.min((int)(entryPerSecond*duration*deleteRatio), (int)(ids.getSize()*deleteRatio));
            if (nDelete > 0) {
                int[] deleteIds = ids.sample(nDelete, ts);
                for (int i=0;i<deleteIds.length;i++) {
                    DataOperation op = new DataOperation();
                    if (numerous_columns && partial_update) {
                        schema.genOpNumerousColumns(deleteIds[i], deleteIds[i], 0, op, numerousPartialColumnIdxes);
                    } else {
                        schema.genOp(deleteIds[i], deleteIds[i], 0, op);
                    }
                    op.table = tableName;
                    op.op = Op.DELETE;
                    load.handler.onDataOperation(op);
                }
                ids.remove(deleteIds);
            }
        }
        // TODO(cbl): more reasonable calculation
        int nUpdate = Math.min((int)(entryPerSecond*duration*updateRatio), (int)(ids.getSize()*updateRatio));
        if (nUpdate > 0) {
            int[] updateIds = ids.sample(nUpdate, ts);
            for (int i=0;i<updateIds.length;i++) {
                DataOperation op = new DataOperation();
                if (numerous_columns && partial_update) {
                    schema.genOpNumerousColumns(updateIds[i], updateIds[i], ts, op, numerousPartialColumnIdxes);
                } else {
                    schema.genOp(updateIds[i], updateIds[i], ts, op);
                }
                op.table = tableName;
                op.op = Op.UPSERT;
                load.handler.onDataOperation(op);
            }
        }
        int[] newIds = generate(ts, duration);
        for (int i=0;i<newIds.length;i++) {
            DataOperation op = new DataOperation();
            if (numerous_columns && partial_update) {
                schema.genOpNumerousColumns(newIds[i], newIds[i], 0, op, numerousPartialColumnIdxes);
            } else {
                schema.genOp(newIds[i], newIds[i], 0, op);
            }
            op.table = tableName;
            op.op = Op.INSERT;
            load.handler.onDataOperation(op);
        }
        ids.append(newIds, 0, newIds.length);
        LOG.info(String.format("epoch #del:%d #update:%d #new:%d #current:%d", nDelete, nUpdate, newIds.length, ids.getSize()));
    }

    String getCreateTableSql() {
        if (conf.getString("db.type").toLowerCase().startsWith("doris")) {
            return schema.getCreateTableDorisDB(tableName, conf.getInt("db.payments.bucket"), conf.getInt("db.replication"));
        } else {
            return schema.getCreateTableMySql(tableName);
        }
    }
}
