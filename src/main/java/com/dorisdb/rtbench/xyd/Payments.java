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
        this.tableName = "payments";
        this.entryPerDay = conf.getInt("record_per_day");
        this.entryPerSecond = (entryPerDay / (3600 * 24.0));
        this.ids = new IntArray(0, entryPerDay*2);
        this.curId = 0;
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
                    schema.genOp(deleteIds[i], deleteIds[i], 0, op);
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
                schema.genOp(updateIds[i], updateIds[i], ts, op);
                op.table = tableName;
                op.op = Op.UPSERT;
                load.handler.onDataOperation(op);
            }
        }
        int[] newIds = generate(ts, duration);
        for (int i=0;i<newIds.length;i++) {
            DataOperation op = new DataOperation();
            schema.genOp(newIds[i], newIds[i], 0, op);
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
