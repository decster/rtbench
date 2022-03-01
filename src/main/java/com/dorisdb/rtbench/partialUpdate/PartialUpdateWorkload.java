package com.dorisdb.rtbench.partialUpdate;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.typesafe.config.Config;

import com.dorisdb.rtbench.SqlOperation;
import com.dorisdb.rtbench.Workload;
import com.dorisdb.rtbench.WorkloadHandler;

public class PartialUpdateWorkload extends Workload {
    private static final Logger LOG = LogManager.getLogger(PartialUpdateWorkload.class);
    protected Config conf;
    public WorkloadHandler handler;

    String dbName;
    PaymentsPartialUpdate paymentsPartialUpdate;
    long recordNum;
    boolean pureDataLoad;

    public PartialUpdateWorkload() {
    }

    @Override
    public void init(Config conf, WorkloadHandler handler) throws Exception {
        this.conf = conf;
        this.handler = handler;
        this.recordNum = 1000*1000;
    }

    @Override
    public void setup() throws Exception {
        paymentsPartialUpdate = new PaymentsPartialUpdate(this, conf);
        dbName = conf.getString("db.name");
        pureDataLoad = conf.getBoolean("pure_data_load");
        handler.onSqlOperation(new SqlOperation(String.format("create database if not exists %s", dbName)));
        handler.onSqlOperation(new SqlOperation("use " + dbName));
        if (conf.getBoolean("cleanup") && pureDataLoad) {
            handler.onSqlOperation(new SqlOperation("drop table if exists " + paymentsPartialUpdate.tableName));
        }
        handler.onSqlOperation(new SqlOperation(paymentsPartialUpdate.getCreateTableSql()));
        handler.flush();
    }

    @Override
    public void processEpoch(long id, long epochTs, long recordNum) throws Exception {
    }

    @Override
    public void close() {
    }

    @Override
    public void run() throws Exception {
        handler.onSetupBegin();
        setup();
        handler.onSetupEnd();

        if (pureDataLoad) {
            long id = 0;
            long loadedFileSize = 0;
            long totalFileSize = 1024*1024*1024;
            long loadedRecordNum = 0;
            long oneLoadRecordNum = 1024*1024;
            while (true) {
                String epochName = String.valueOf(id);
                LOG.info("start epoch " + id);
                handler.onEpochBegin(id, epochName);
                paymentsPartialUpdate.processEpoch((int)loadedRecordNum, (int)oneLoadRecordNum, 0, false);
                LOG.info("end epoch " + id);
                handler.onEpochEnd(id, epochName);
                loadedFileSize += handler.getFileSize();
                loadedRecordNum += oneLoadRecordNum;
                LOG.info("loadedRecordNum: " + loadedRecordNum + ", loadedFileSize: " + loadedFileSize + ", totalFileSize: " + totalFileSize);
                if (loadedFileSize > totalFileSize) {
                    break;
                }
                id++;
            }
        } else {
            int loadedRecordNum = conf.getInt("loaded_record_num");
            boolean[] exponential_distributions = {false, true};
            double[] updateRatios = {0.1, 0.2, 0.5, 0.8};
            int[] recordNums = {1000, 10000, 100000, 1000000};
            int updateRepeatTimes = 20;
            for (int l = 0; l < exponential_distributions.length; ++l) {
                boolean exponential_distribution = exponential_distributions[l];
                for (int k = 0; k < updateRatios.length; ++k) {
                    double updateRatio = updateRatios[k];
                    for (int j = 0; j < recordNums.length; ++j) {
                        int oneLoadRecordNum = recordNums[j];
                        LOG.info("===========================" + (exponential_distribution ? "exponential" : "random") + " distribution partial update " + oneLoadRecordNum + " records " + "of leftmost " + updateRatio + " columns " + updateRepeatTimes + " times in " + loadedRecordNum + " records" + "===========================");
                        for (int i = 1; i <= updateRepeatTimes; ++i) {
                            String epochName = "partial-" + oneLoadRecordNum;
                            LOG.info("-------------" + "NO." + i + "/" + updateRepeatTimes + "-------------");
                            handler.onEpochBegin(0, epochName);
                            paymentsPartialUpdate.processEpoch((int)loadedRecordNum, (int)oneLoadRecordNum, updateRatio, exponential_distribution);
                            handler.onEpochEnd(0, epochName);
                        }
                    }
                }
            }
        }

        LOG.info("close workload");
        close();
        handler.onClose();
    }
}
