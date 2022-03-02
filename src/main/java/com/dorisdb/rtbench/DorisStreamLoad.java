package com.dorisdb.rtbench;

import java.io.File;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.concurrent.TimeUnit;

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

import com.dorisdb.rtbench.DataOperation.Op;
import com.typesafe.config.Config;

public class DorisStreamLoad implements DorisLoad {
    Logger LOG = LogManager.getLogger(DorisStreamLoad.class);
    String addr;
    String db;
    String table;
    String url;
    String label;
    boolean dryRun;
    boolean withDelete;
    boolean partial_update;
    boolean pureDataLoad;
    int[] numerousPartialColumnIdxes;
    String authHeader;
    String tmpDir;
    File outFile;
    long fileSize;
    boolean keepFile;
    PrintWriter out;
    long opCount;
    int retry = 0;
    long retrySleepSec = 3;

    static final String randLabelSuffix = "_" + Utils.newRandShortID(4);

    public DorisStreamLoad(Config conf, String db, String table, String label) {
        this.dryRun = conf.getBoolean("dry_run");
        this.withDelete = conf.getBoolean("with_delete");
        this.partial_update = conf.getBoolean("partial_update");
        this.pureDataLoad = conf.getBoolean("pure_data_load");
        String[] numerousPartialColumnStrIdxes = conf.getString("numerous_partial_columns").split(",");
        this.numerousPartialColumnIdxes = new int[numerousPartialColumnStrIdxes.length];
        for (int i = 0; i < numerousPartialColumnStrIdxes.length; i++) {
            this.numerousPartialColumnIdxes[i] = Integer.parseInt(numerousPartialColumnStrIdxes[i]);
        }
        this.addr = conf.getString("handler.dorisdb.stream_load.addr");
        this.keepFile = conf.getBoolean("handler.dorisdb.stream_load.keep_file");
        this.url = String.format("http://%s/api/%s/%s/_stream_load", addr, db, table);
        this.retry = conf.getInt("handler.dorisdb.load.retry");
        this.retrySleepSec = conf.getDuration("handler.dorisdb.load.retry_sleep", TimeUnit.SECONDS);
        String user = conf.getString("handler.dorisdb.user");
        String password = conf.getString("handler.dorisdb.password");
        this.authHeader = basicAuthHeader(user, password);
        this.db = db;
        this.table = table;
        this.label = label;
        this.tmpDir = conf.getString("handler.dorisdb.tmpdir");
        this.outFile = new File(String.format("%s/%s.data", tmpDir, label));
        this.opCount = 0;
    }

    public String getTable() {
        return table;
    }

    public String getLabel() {
        return label;
    }

    public long getOpCount() {
        return opCount;
    }

    public PrintWriter getWriter() throws Exception {
        if (out == null) {
            new File(tmpDir).mkdirs();
            out = new PrintWriter(this.outFile, StandardCharsets.UTF_8.name());
        }
        return out;
    }

    static final char FIELD_SEP = '\001';
    static final char [] NULL_FIELD = new char[] {'\\', 'N'};
    static String [] columnNames = null;
    static int [] updateFieldIdxs = null;

    public void addData(DataOperation op) throws Exception {
        opCount++;
        if (dryRun) {
            return;
        }
        PrintWriter out = getWriter();
        if (columnNames == null) {
            columnNames = op.fullFieldNames.clone();
        }
        if (!pureDataLoad) {
            updateFieldIdxs = op.updateFieldIdxs.clone();
        }
        if (op.op == Op.INSERT || op.op == Op.UPSERT || op.op == Op.DELETE) {
            for (int i=0;i<op.fullFields.length;i++) {
                if (i > 0) {
                    out.append(FIELD_SEP);
                }
                Object f = op.fullFields[i];
                if (f != null) {
                    if (f instanceof Number) {
                        out.append(f.toString());
                    } else if (f instanceof String) {
                        out.append((String)f);
                    }
                } else {
                    out.write(NULL_FIELD);
                }
            }
            if (!partial_update && withDelete) {
                out.append(FIELD_SEP);
                out.append(op.op == Op.DELETE ? '1' : '0');
            } else if (op.op == Op.DELETE) {
                throw new IllegalArgumentException("workload should not have delete ops");
            }
            out.append('\n');
        } else {
            throw new Exception("op type not support");
        }
    }

    static String getColumnMappingExpr(String[] colNames) {
        StringBuilder sb = new StringBuilder();
        for (int i=0;i<colNames.length+1;i++) {
            sb.append("srccol" + i);
            sb.append(',');
        }
        for (int i=0;i<colNames.length;i++) {
            sb.append(colNames[i]);
            sb.append('=');
            sb.append("srccol"+i);
            sb.append(',');
        }
        sb.append("__op=srccol" + colNames.length);
        return sb.toString();
    }

    static boolean resultOK(String result) {
        if (result.contains("OK")) {
            return true;
        }
        if (result.contains("Label Already Exists") && result.contains("FINISHED")) {
            return true;
        }
        return false;
    }

    private void sendInner() throws Exception {
        final HttpClientBuilder httpClientBuilder = HttpClients
                .custom()
                .setRedirectStrategy(new DefaultRedirectStrategy() {
                    @Override
                    protected boolean isRedirectable(String method) {
                        return true;
                    }
                });
        CloseableHttpClient client = httpClientBuilder.build();
        HttpPut put = new HttpPut(url);
        put.setHeader(HttpHeaders.EXPECT, "100-continue");
        put.setHeader(HttpHeaders.AUTHORIZATION, authHeader);
        put.setHeader("label", label + randLabelSuffix);
        put.setHeader("format", "csv");
        put.setHeader("column_separator", "\\x01");
        if (!pureDataLoad) {
            if (partial_update && numerousPartialColumnIdxes.length != 0) {
                put.setHeader("partial_update", "true");
                int updateColumnNum = updateFieldIdxs.length;
                String columnMapping = String.join(",", Arrays.copyOfRange(columnNames, 0, updateColumnNum));
                put.setHeader("columns", columnMapping);
            } else if (partial_update && columnNames != null) {
                put.setHeader("partial_update", "true");
                String columnMapping = String.join(",", columnNames[0], columnNames[15]);
                put.setHeader("columns", columnMapping);
            } else if (withDelete && columnNames != null) {
                String columnMapping = getColumnMappingExpr(columnNames);
                put.setHeader("columns", columnMapping);
            }
        }
        put.setEntity(new FileEntity(outFile));
        CloseableHttpResponse response = client.execute(put);
        final int status = response.getStatusLine().getStatusCode();
        String result = "null";
        if (response.getEntity() != null) {
            result =  EntityUtils.toString(response.getEntity());
        }
        if (status != 200 || !resultOK(result)) {
            throw new Exception(String.format("doris stream load: db=%s,table=%s label=%s failed, err=%s", db, table, label, result));
        }
    }

    public void send() throws Exception {
        if (out == null) {
            return;
        }
        try {
            out.close();
            while (true) {
                try {
                    sendInner();
                    break;
                } catch (Exception e) {
                    if (retry > 0) {
                        LOG.info(String.format("%s sleep %dsec and retry", e.getMessage(), retrySleepSec));
                        Thread.sleep(retrySleepSec * 1000);
                        retry--;
                    } else {
                        throw e;
                    }
                }
            }
        } finally {
            this.fileSize = outFile.length();
            if (!keepFile) {
                outFile.delete();
            }
        }
    }

    public long getFileSize() { return fileSize; }

    private static String basicAuthHeader(String username, String password) {
        final String tobeEncode = username + ":" + password;
        byte[] encoded = Base64.getEncoder().encode(tobeEncode.getBytes(StandardCharsets.UTF_8));
        return "Basic " + new String(encoded);
    }
}
