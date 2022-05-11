package com.dorisdb.rtbench.partialUpdate;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.FileEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.typesafe.config.Config;

import com.dorisdb.rtbench.SqlOperation;
import com.dorisdb.rtbench.Workload;
import com.dorisdb.rtbench.WorkloadHandler;
import com.dorisdb.rtbench.Utils;

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
    String partialUpdateColumns;

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
        partialUpdateColumns = conf.getString("partial_update_columns");
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
            if (conf.getBoolean("data_load_prepare")) {
                long generatedRecordNum = 0;
                long generatedFileSize = conf.getBoolean("cleanup") ? 0 : (long)(1024*1024*1024L*conf.getDouble("loaded_file_size"));
                for (long id = generatedRecordNum / recordPerLoad; generatedFileSize < totalFileSize; ++id) {
                    String epochName = String.valueOf(id);
                    handler.onEpochBegin(id, epochName);
                    if (generatedRecordNum != 0) {
                        recordPerLoad = Math.min((int)((double)(totalFileSize - generatedFileSize) / generatedFileSize * generatedRecordNum), recordPerLoad);
                    }
                    long t0 = System.nanoTime();
                    partialUpdate.processEpoch(generatedRecordNum, (int)recordPerLoad, 0, false);
                    long t1 = System.nanoTime();
                    handler.onEpochEnd(id, epochName);
                    generatedRecordNum += recordPerLoad;
                    long fileSize = handler.getFileSize();
                    generatedFileSize += fileSize;
                    String label = String.format("load-partial_update-%s", epochName);
                    LOG.info(String.format("%s.data generated, size: %.2fG, elapsed: %.2fs, RecordNum: %d, FileSize: %.2fG, progress: %.2f%%", label, fileSize / (1024*1024*1024f), (t1-t0) / 1000000000.0, generatedRecordNum,  generatedFileSize / (1024*1024*1024f), generatedFileSize / (double)totalFileSize * 100));
                }
                LOG.info(String.format("%d columns, %.1fG total, %d records", conf.getInt("all_column_num"), generatedFileSize / (1024*1024*1024f), generatedRecordNum));
            } else {
                long loadedFileSize = 0;
                for (long id = 0; id < conf.getInt("loaded_file_num"); ++id) {
                    String label = String.format("load-partial_update-%s", id);

                    final HttpClientBuilder httpClientBuilder = HttpClients
                    .custom()
                    .setRedirectStrategy(new DefaultRedirectStrategy() {
                        @Override
                        protected boolean isRedirectable(String method) {
                            return true;
                        }
                    });
                    CloseableHttpClient client = httpClientBuilder.build();
                    String url = String.format("http://%s/api/%s/%s/_stream_load", conf.getString("handler.dorisdb.stream_load.addr"), dbName, partialUpdate.tableName);
                    HttpPut put = new HttpPut(url);
                    put.setHeader(HttpHeaders.EXPECT, "100-continue");

                    String username = conf.getString("handler.dorisdb.user");
                    String password = conf.getString("handler.dorisdb.password");
                    final String tobeEncode = username + ":" + password;
                    byte[] encoded = Base64.getEncoder().encode(tobeEncode.getBytes(StandardCharsets.UTF_8));
                    String authHeader = "Basic " + new String(encoded);
                    put.setHeader(HttpHeaders.AUTHORIZATION, authHeader);
                    String randLabelSuffix = "_" + Utils.newRandShortID(6);
                    put.setHeader("label", label + randLabelSuffix);
                    put.setHeader("format", "csv");
                    put.setHeader("column_separator", "\\x01");
                    File outFile = new File(String.format("%s/%s.data", conf.getString("handler.dorisdb.tmpdir"), label));
                    put.setEntity(new FileEntity(outFile));
                    long t0 = System.nanoTime();
                    CloseableHttpResponse response = client.execute(put);
                    long t1 = System.nanoTime();
                    final int status = response.getStatusLine().getStatusCode();
                    long fileSize = outFile.length();
                    loadedFileSize += fileSize;
                    String result = "null";
                    if (response.getEntity() != null) {
                        result =  EntityUtils.toString(response.getEntity());
                    }
                    if (status != 200 || !(result.contains("OK") || (result.contains("Label Already Exists") && result.contains("FINISHED")))) {
                        throw new Exception(String.format("doris stream load: db=%s,table=%s label=%s failed, err=%s", dbName, partialUpdate.tableName, label, result));
                    }

                    LOG.info(String.format("%s.data loaded, size: %.2fG, elapsed: %.2fs, FileSize: %.2fG, progress: %.2f%%", label, fileSize / (1024*1024*1024f), (t1-t0) / 1000000000.0,  loadedFileSize / (1024*1024*1024f), loadedFileSize / (double)totalFileSize * 100));
                }
            }
        } else {
            if (conf.getBoolean("data_load_prepare")) {
                // boolean[] exponential_distributions = {false, true};
                boolean[] exponential_distributions = {false};
                // double[] updateRatios = {0.1, 0.2, 0.5, 0.8};
                double[] updateRatios = {0.1};
                // long[] recordNums = {1000L, 10000L, 100000L, 1000000L};
                long[] recordNums = {100000L};
                List<String> table_lines = new ArrayList<>();
                table_lines.add(String.format("|distribution|upd col|row num|AVG time|op per sec|file size|"));
                for (int l = 0; l < exponential_distributions.length; ++l) {
                    boolean exponential_distribution = exponential_distributions[l];
                    for (int k = 0; k < updateRatios.length; ++k) {
                        double updateRatio = updateRatios[k];
                        for (int j = 0; j < recordNums.length; ++j) {
                            long generatedFileSize = 0L;
                            long recordPerLoad = recordNums[j];
                            // long updateRepeatTimes = 0L;
                            // if (recordPerLoad >= 100000) {
                            //     updateRepeatTimes = 3;
                            // } else {
                            //     updateRepeatTimes = 6;
                            // }
                            long updateRepeatTimes = conf.getLong("update_repeat_times");
                            double total_elapsed_second = 0;
                            for (int i = 0; i < updateRepeatTimes; ++i) {
                                String epochName = "partial-" + (exponential_distribution ? "exponential" : "random") + "-P" + (int)(updateRatio * 100) + "-" + recordPerLoad + "-" + "NO-" + String.valueOf(i);
                                handler.onEpochBegin(0, epochName);
                                long t0 = System.nanoTime();
                                partialUpdate.processEpoch(loadedRecordNum, (int)recordPerLoad, updateRatio, exponential_distribution);
                                long t1 = System.nanoTime();
                                handler.onEpochEnd(0, epochName);
                                double elapsed_second = (t1-t0) / 1000000000.0;
                                total_elapsed_second += elapsed_second;
                                long fileSize = handler.getFileSize();
                                generatedFileSize += fileSize;
                                String label = String.format("load-partial_update-%s", epochName);
                                LOG.info(String.format("%s.data generated, size: %.3fM, elapsed %.2fs: %s update %d records of leftmost %.0f%% columns %d times in %d records", label, fileSize / (1024*1024f), elapsed_second, exponential_distribution ? "exponential" : "random", recordPerLoad, updateRatio * 100, updateRepeatTimes, loadedRecordNum));
                            }
                            // natural language print
                            // LOG.info(String.format("%s update %d records of leftmost %.0f%% columns %d times in %d records, average elapsed %.2fs", exponential_distribution ? "exponential" : "random", recordPerLoad, updateRatio * 100, updateRepeatTimes, loadedRecordNum, total_elapsed_second / updateRepeatTimes));
                            // table print
                            double average_elapsed_second = total_elapsed_second / updateRepeatTimes;
                            table_lines.add(String.format("|%12s|%6d%%|%7d|%7.2fs|%10.0f|%8.2fM|", exponential_distribution ? "exponential" : "random", (int)(updateRatio * 100), recordPerLoad, average_elapsed_second, recordPerLoad / average_elapsed_second, generatedFileSize / (1024*1024f)));
                        }
                    }
                }
                for (String table_line : table_lines) {
                    LOG.info(table_line);
                }
            } else {
                // boolean[] exponential_distributions = {false, true};
                boolean[] exponential_distributions = {false};
                // double[] updateRatios = {0.1, 0.2, 0.5, 0.8};
                double[] updateRatios = {0.1};
                // long[] recordNums = {1000L, 10000L, 100000L, 1000000L};
                long[] recordNums = {100000L};
                List<String> table_lines = new ArrayList<>();
                table_lines.add(String.format("|distribution|upd col|row num|AVG time|op per sec|file size|"));
                for (int l = 0; l < exponential_distributions.length; ++l) {
                    boolean exponential_distribution = exponential_distributions[l];
                    for (int k = 0; k < updateRatios.length; ++k) {
                        double updateRatio = updateRatios[k];
                        for (int j = 0; j < recordNums.length; ++j) {
                            long loadedFileSize = 0L;
                            long recordPerLoad = recordNums[j];
                            // long updateRepeatTimes = 5L;
                            // if (recordPerLoad >= 100000) {
                            //     updateRepeatTimes = 3;
                            // } else {
                            //     updateRepeatTimes = 6;
                            // }
                            long updateRepeatTimes = conf.getLong("update_repeat_times");
                            double total_elapsed_second = 0;
                            for (int i = 0; i < updateRepeatTimes; ++i) {
                                String label = "load-partial_update-partial-" + (exponential_distribution ? "exponential" : "random") + "-P" + (int)(updateRatio * 100) + "-" + recordPerLoad + "-" + "NO-" + String.valueOf(i);

                                final HttpClientBuilder httpClientBuilder = HttpClients
                                .custom()
                                .setRedirectStrategy(new DefaultRedirectStrategy() {
                                    @Override
                                    protected boolean isRedirectable(String method) {
                                        return true;
                                    }
                                });
                                CloseableHttpClient client = httpClientBuilder.build();
                                String url = String.format("http://%s/api/%s/%s/_stream_load", conf.getString("handler.dorisdb.stream_load.addr"), dbName, partialUpdate.tableName);
                                HttpPut put = new HttpPut(url);
                                put.setHeader(HttpHeaders.EXPECT, "100-continue");
                                String username = conf.getString("handler.dorisdb.user");
                                String password = conf.getString("handler.dorisdb.password");
                                final String tobeEncode = username + ":" + password;
                                byte[] encoded = Base64.getEncoder().encode(tobeEncode.getBytes(StandardCharsets.UTF_8));
                                String authHeader = "Basic " + new String(encoded);
                                put.setHeader(HttpHeaders.AUTHORIZATION, authHeader);
                                String randLabelSuffix = "_" + Utils.newRandShortID(6);
                                put.setHeader("label", label + randLabelSuffix);
                                put.setHeader("format", "csv");
                                put.setHeader("column_separator", "\\x01");
                                put.setHeader("partial_update", "true");
                                put.setHeader("columns", partialUpdateColumns);
                                File outFile = new File(String.format("%s/%s.data", conf.getString("handler.dorisdb.tmpdir"), label));
                                put.setEntity(new FileEntity(outFile));
                                long t0 = System.nanoTime();
                                CloseableHttpResponse response = client.execute(put);
                                long t1 = System.nanoTime();
                                final int status = response.getStatusLine().getStatusCode();
                                String result = "null";
                                if (response.getEntity() != null) {
                                    result =  EntityUtils.toString(response.getEntity());
                                }
                                if (status != 200 || !(result.contains("OK") || (result.contains("Label Already Exists") && result.contains("FINISHED")))) {
                                    throw new Exception(String.format("doris stream load: db=%s,table=%s label=%s failed, err=%s", dbName, partialUpdate.tableName, label, result));
                                }

                                double elapsed_second = (t1-t0) / 1000000000.0;
                                total_elapsed_second += elapsed_second;
                                long fileSize = outFile.length();
                                loadedFileSize += fileSize;

                                LOG.info(String.format("%s.data loaded, size: %.3fM elapsed %.2fs, %s update %d records of leftmost %.0f%% columns %d times in %d records", label, fileSize / (1024*1024f), elapsed_second, exponential_distribution ? "exponential" : "random", recordPerLoad, updateRatio * 100, updateRepeatTimes, loadedRecordNum));
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
        }

        LOG.info("close workload");
        close();
        handler.onClose();
    }
}
