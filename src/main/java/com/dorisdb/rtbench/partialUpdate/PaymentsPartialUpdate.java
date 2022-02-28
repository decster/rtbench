package com.dorisdb.rtbench.partialUpdate;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.dorisdb.rtbench.DataOperation;
import com.dorisdb.rtbench.DataOperation.Op;
import com.dorisdb.rtbench.IntArray;
import com.dorisdb.rtbench.schema.Schema;
import com.dorisdb.rtbench.Utils;
import com.dorisdb.rtbench.Utils.PowerDist;
import java.util.Random;
import static com.dorisdb.rtbench.schema.Columns.*;
import com.typesafe.config.Config;


public class PaymentsPartialUpdate {
    private static final Logger LOG = LogManager.getLogger(PaymentsPartialUpdate.class);

    PartialUpdateWorkload load;
    Config conf;
    boolean pureDataLoad;
    int repeatedColumnsNum;
    int allColumnNum;
    String tableName;
    Schema schema;
    IntArray ids;
    int curId;

    long rand = 1;
    
    public PaymentsPartialUpdate(PartialUpdateWorkload load, Config conf) throws Exception {
        this.load = load;
        this.conf = conf;
        this.pureDataLoad = conf.getBoolean("pure_data_load");
        this.repeatedColumnsNum = conf.getInt("repeated_columns_num");
        this.allColumnNum = conf.getInt("all_column_num");
        this.tableName = "payments_partial_update";
        this.ids = new IntArray(0, 1024);
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

        Random generator = new Random();

        int[] indexes = new int[prototypes.length];

        for (int i = 0; i < allColumnNum; ++i) {
            int rn = generator.nextInt(prototypes.length);
            com.dorisdb.rtbench.schema.Column prototype = (com.dorisdb.rtbench.schema.Column)prototypes[rn].clone();
            prototype.setName(String.format("%s%03d", prototype.name, indexes[rn]++));
            numerous_cols_list.add(prototype);
        }

        com.dorisdb.rtbench.schema.Column[] numerous_value_cols = new com.dorisdb.rtbench.schema.Column[numerous_cols_list.size()];
        numerous_value_cols = numerous_cols_list.toArray(numerous_value_cols);
        schema = new Schema(numerous_value_cols);
    }

    int[] generate(int ts, int duration) {
        int[] ret = new int[(int)(duration)];
        for (int i = 0; i < ret.length; i++) {
            ret[i] = curId;
            curId++;
        }
        return ret;
    }

    void processEpoch(int ts, int recordNum, double updateRatio, boolean exponential_distribution) throws Exception {
        if (pureDataLoad) {
            int[] newIds = generate(ts, recordNum);
            for (int i=0;i<newIds.length;i++) {
                DataOperation op = new DataOperation();
                schema.genOp(newIds[i], newIds[i], 0, op);
                op.table = tableName;
                op.op = Op.INSERT;
                load.handler.onDataOperation(op);
            }
            ids.append(newIds, 0, newIds.length);
            LOG.info(String.format("epoch #new:%d #current:%d", newIds.length, ids.getSize()));
        } else {
            int nUpdate = recordNum;
            if (nUpdate > 0) {
                IntArray idxes = new IntArray(0, ts);
                curId = 0;
                int[] allNewIds = generate(ts, ts);
                idxes.append(allNewIds, 0, allNewIds.length);
                int updateColumnNum = (int)(schema.nCol * updateRatio);
                int [] updateColumnIdxes = new int[updateColumnNum];
                for (int i = 0; i < updateColumnNum; i++) {
                    updateColumnIdxes[i] = i;
                }
                int[] updateIds = new int[nUpdate];
                if (!exponential_distribution) {
                    updateIds = idxes.sample(nUpdate, ts);
                } else {
                    PowerDist paymentTime = new PowerDist(200, 400, 1.1f);
                    for (int i = 0; i < nUpdate; ++i) {
                        rand = Utils.nextRand(rand);
                        updateIds[i] = paymentTime.sample(rand) - 200;
                    }
                }
                for (int i=0;i<updateIds.length;i++) {
                    DataOperation op = new DataOperation();
                    schema.genOpNumerousColumns(updateIds[i], updateIds[i], ts, op, updateColumnIdxes);
                    op.table = tableName;
                    op.op = Op.UPSERT;
                    load.handler.onDataOperation(op);
                }
                LOG.info(String.format("epoch #update:%d #current:%d", nUpdate, idxes.getSize()));
            }
        }
    }

    String getCreateTableSql() {
        if (conf.getString("db.type").toLowerCase().startsWith("doris")) {
            return schema.getCreateTableDorisDB(tableName, conf.getInt("db.payments.bucket"), conf.getInt("db.replication"));
        } else {
            return schema.getCreateTableMySql(tableName);
        }
    }
}
