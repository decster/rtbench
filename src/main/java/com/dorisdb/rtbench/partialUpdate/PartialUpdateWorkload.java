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
    PartialUpdate partialUpdate;
    long recordNum;
    boolean pureDataLoad;
    long totalFileSize;
    long recordPerLoad;

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
        partialUpdate = new PartialUpdate(this, conf);
        dbName = conf.getString("db.name");
        pureDataLoad = conf.getBoolean("pure_data_load");
        totalFileSize = 1024*1024*1024L*conf.getInt("total_file_size");
        recordPerLoad = conf.getLong("record_per_load");
        handler.onSqlOperation(new SqlOperation(String.format("create database if not exists %s", dbName)));
        handler.onSqlOperation(new SqlOperation("use " + dbName));
        if (conf.getBoolean("cleanup") && pureDataLoad) {
            handler.onSqlOperation(new SqlOperation("drop table if exists " + partialUpdate.tableName));
        }
        handler.onSqlOperation(new SqlOperation(partialUpdate.getCreateTableSql()));
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

        long id = 0L;

        if (pureDataLoad) {
            long loadedRecordNum = 0L;
            long loadedFileSize = 0L;
            LOG.info(String.format("=========================== total %.2fG, %d rows per load ===========================", totalFileSize / (1024*1024*1024f), recordPerLoad));
            while (true) {
                String epochName = String.valueOf(id);
                handler.onEpochBegin(id, epochName);
                if (loadedRecordNum != 0) {
                    recordPerLoad = Math.min((int)((double)(totalFileSize - loadedFileSize) / loadedFileSize * loadedRecordNum), recordPerLoad);
                }
                partialUpdate.processEpoch(0, (int)recordPerLoad, 0, false);
                long t0 = System.nanoTime();
                handler.onEpochEnd(id, epochName);
                long t1 = System.nanoTime();
                loadedRecordNum += recordPerLoad;
                loadedFileSize += handler.getFileSize();
                String label = String.format("load-partial_update-%s", epochName);
                LOG.info(String.format("%s.data cost %.2fs, loadedRecordNum: %d, loadedFileSize: %.2fG, progress: %.2f%%", label, (t1-t0) / 1000000000.0, loadedRecordNum,  loadedFileSize / (1024*1024*1024f), loadedFileSize / (double)totalFileSize * 100));
                if (loadedFileSize > totalFileSize) {
                    break;
                }
                id++;
            }
        } else {
            long loadedRecordNum = conf.getLong("loaded_record_num");
            boolean[] exponential_distributions = {false, true};
            double[] updateRatios = {0.1, 0.2, 0.5, 0.8};
            long[] recordNums = {1000L, 10000L, 100000L, 1000000L};
            long updateRepeatTimes = 20L;
            for (int l = 0; l < exponential_distributions.length; ++l) {
                boolean exponential_distribution = exponential_distributions[l];
                for (int k = 0; k < updateRatios.length; ++k) {
                    double updateRatio = updateRatios[k];
                    for (int j = 0; j < recordNums.length; ++j) {
                        long recordPerLoad = recordNums[j];
                        double total_elapsed_second = 0;
                        LOG.info("===========================" + (exponential_distribution ? "exponential" : "random") + " partial update " + recordPerLoad + " records " + "of leftmost " + updateRatio + " columns " + updateRepeatTimes + " times in " + loadedRecordNum + " records" + "===========================");
                        for (int i = 1; i <= updateRepeatTimes; ++i) {
                            String epochName = "partial-" + recordPerLoad + "-" + String.valueOf(id);
                            handler.onEpochBegin(0, epochName);
                            partialUpdate.processEpoch((int)loadedRecordNum, (int)recordPerLoad, updateRatio, exponential_distribution);
                            long t0 = System.nanoTime();
                            handler.onEpochEnd(0, epochName);
                            long t1 = System.nanoTime();
                            double elapsed_second = (t1-t0) / 1000000000.0;
                            total_elapsed_second += elapsed_second;
                            String label = String.format("load-partial_update-%s", epochName);
                            LOG.info(String.format("%s.data elapsed %.2fs", label, elapsed_second));
                            id++;
                        }
                        LOG.info(String.format("average elapsed %.2fs", total_elapsed_second / updateRepeatTimes));
                    }
                }
            }
        }

        LOG.info("close workload");
        close();
        handler.onClose();
    }
}
