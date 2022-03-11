package com.dorisdb.rtbench.partialUpdate;

import java.util.ArrayList;
import java.util.List;

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
        if (pureDataLoad) {
            if (conf.getBoolean("cleanup")) {
                handler.onSqlOperation(new SqlOperation("drop table if exists " + partialUpdate.tableName));
            }
            handler.onSqlOperation(new SqlOperation(partialUpdate.getCreateTableSql()));
        }
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
        long loadedRecordNum = 0L;
        if (!conf.getBoolean("cleanup") || !pureDataLoad) {
            java.sql.ResultSet rs = handler.onSqlOperationResult(new SqlOperation("select max(id) from partial_update"));
            if (rs.next() == false) {
                LOG.warn("ResultSet is empty");
            } else {
                long maxId = rs.getLong("max(id)");
                if (maxId != 0) {
                    loadedRecordNum = maxId + 1;
                }
            }
        }
        handler.onSetupEnd();

        if (pureDataLoad) {
            long loadedFileSize = conf.getBoolean("cleanup") ? 0 : (long)(1024*1024*1024L*conf.getDouble("loaded_file_size"));
            long id = loadedRecordNum / recordPerLoad;
            while (true) {
                String epochName = String.valueOf(id);
                handler.onEpochBegin(id, epochName);
                if (loadedRecordNum != 0) {
                    recordPerLoad = Math.min((int)((double)(totalFileSize - loadedFileSize) / loadedFileSize * loadedRecordNum), recordPerLoad);
                }
                partialUpdate.processEpoch(loadedRecordNum, (int)recordPerLoad, 0, false);
                long t0 = System.nanoTime();
                handler.onEpochEnd(id, epochName);
                long t1 = System.nanoTime();
                loadedRecordNum += recordPerLoad;
                long fileSize = handler.getFileSize();
                loadedFileSize += fileSize;
                String label = String.format("load-partial_update-%s", epochName);
                LOG.info(String.format("%s.data size: %.2fG, elapsed: %.2fs, loadedRecordNum: %d, loadedFileSize: %.2fG, progress: %.2f%%", label, fileSize / (1024*1024*1024f), (t1-t0) / 1000000000.0, loadedRecordNum,  loadedFileSize / (1024*1024*1024f), loadedFileSize / (double)totalFileSize * 100));
                if (loadedFileSize > totalFileSize) {
                    break;
                }
                id++;
            }
            LOG.info(String.format("%d columns, %.1fG total, %d records", conf.getInt("all_column_num"), loadedFileSize / (1024*1024*1024f), loadedRecordNum));
        } else {
            boolean[] exponential_distributions = {false, true};
            double[] updateRatios = {0.1, 0.2, 0.5, 0.8};
            long[] recordNums = {1000L, 10000L, 100000L, 1000000L};
            List<String> table_lines = new ArrayList<>();
            table_lines.add(String.format("|distribution|upd col|row num|AVG time|op per sec|file size|"));
            for (int l = 0; l < exponential_distributions.length; ++l) {
                boolean exponential_distribution = exponential_distributions[l];
                for (int k = 0; k < updateRatios.length; ++k) {
                    double updateRatio = updateRatios[k];
                    for (int j = 0; j < recordNums.length; ++j) {
                        long loadedFileSize = 0L;
                        long recordPerLoad = recordNums[j];
                        long updateRepeatTimes = 5L;
                        if (recordPerLoad >= 100000) {
                            updateRepeatTimes = 3;
                        } else {
                            updateRepeatTimes = 6;
                        }
                        double total_elapsed_second = 0;
                        for (int i = 0; i < updateRepeatTimes; ++i) {
                            String epochName = "partial-" + recordPerLoad + "-P" + (int)(updateRatio * 100) + "-" + String.valueOf(i);
                            handler.onEpochBegin(0, epochName);
                            partialUpdate.processEpoch(loadedRecordNum, (int)recordPerLoad, updateRatio, exponential_distribution);
                            long t0 = System.nanoTime();
                            handler.onEpochEnd(0, epochName);
                            long t1 = System.nanoTime();
                            double elapsed_second = (t1-t0) / 1000000000.0;
                            total_elapsed_second += elapsed_second;
                            long fileSize = handler.getFileSize();
                            loadedFileSize += fileSize;
                            String label = String.format("load-partial_update-%s", epochName);
                            LOG.info(String.format("%s update %d records of leftmost %.0f%% columns %d times in %d records, %s.data size: %.3fM elapsed %.2fs", exponential_distribution ? "exponential" : "random", recordPerLoad, updateRatio * 100, updateRepeatTimes, loadedRecordNum, label, fileSize / (1024*1024f), elapsed_second));
                        }
                        // natural language print
                        // LOG.info(String.format("%s update %d records of leftmost %.0f%% columns %d times in %d records, average elapsed %.2fs", exponential_distribution ? "exponential" : "random", recordPerLoad, updateRatio * 100, updateRepeatTimes, loadedRecordNum, total_elapsed_second / updateRepeatTimes));
                        // table print
                        double average_elapsed_second = total_elapsed_second / updateRepeatTimes;
                        table_lines.add(String.format("|%12s|%6d%%|%7d|%7.2fs|%10.0f|%8.2fM|", exponential_distribution ? "exponential" : "random", (int)(updateRatio * 100), recordPerLoad, average_elapsed_second, recordPerLoad / average_elapsed_second, loadedFileSize / (1024*1024f)));
                    }
                }
            }
            for (String table_line : table_lines) {
                LOG.info(table_line);
            }
        }

        LOG.info("close workload");
        close();
        handler.onClose();
    }
}
