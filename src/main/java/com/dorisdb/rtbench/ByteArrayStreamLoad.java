package com.dorisdb.rtbench;

import com.dorisdb.rtbench.DataOperation.Op;
import com.typesafe.config.Config;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.FileEntity;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ByteArrayStreamLoad implements DorisLoad {
    Logger LOG = LogManager.getLogger(ByteArrayStreamLoad.class);
    String addr;
    String db;
    String table;
    String url;
    String label;
    boolean dryRun;
    boolean dumpFile;
    String authHeader;
    SharedByteArrayOutputStream arrayOutput;
    PrintWriter out;
    long opCount;
    int retry = 0;
    long retrySleepSec = 3;
    static final Pattern txnIdPattern = Pattern.compile("\"TxnId\": ([\\d]+),");
    long txnId = -1;
    int numPartialUpdateColumns;

    public ByteArrayStreamLoad(Config conf, String db, String table, String label) {
        this(conf, db, table, label, 0);
    }

    public ByteArrayStreamLoad(Config conf, String db, String table, String label, int numPartialUpdateColumns) {
        this.dryRun = conf.getBoolean("dry_run");
        this.dumpFile = conf.getBoolean("stream_load.dump_file");
        this.addr = conf.getString("stream_load.addr");
        this.url = String.format("http://%s/api/%s/%s/_stream_load", addr, db, table);
        this.retry = 3;
        this.retrySleepSec = 10;
        String user = conf.getString("db.user");
        String password = conf.getString("db.password");
        this.authHeader = basicAuthHeader(user, password);
        this.db = db;
        this.table = table;
        this.label = label;
        this.arrayOutput = new SharedByteArrayOutputStream(8*1024*1024);
        this.out = new PrintWriter(arrayOutput);
        this.opCount = 0;
        this.numPartialUpdateColumns = numPartialUpdateColumns;
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
        return out;
    }

    static final char FIELD_SEP = '\001';
    static final char [] NULL_FIELD = new char[] {'\\', 'N'};
    static String [] columnNames = null;

    public void addData(DataOperation op) throws Exception {
        opCount++;
        if (columnNames == null) {
            columnNames = op.fullFieldNames.clone();
        }
        PrintWriter out = getWriter();
        if (op.op == Op.INSERT || op.op == Op.UPSERT || op.op == Op.DELETE) {
            if (numPartialUpdateColumns > 0) {
                for (int i=0;i<numPartialUpdateColumns;i++) {
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
            } else {
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
            }
            out.append('\n');
        } else {
            throw new Exception("op type not support");
        }
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
        String randLabelSuffix = "_" + Utils.newRandShortID(6);
        put.setHeader("label", label + randLabelSuffix);
        put.setHeader("format", "csv");
        put.setHeader("column_separator", "\\x01");
        if (numPartialUpdateColumns > 0) {
            put.setHeader("partial_update", "true");
            String columnMapping = String.join(",", Arrays.copyOfRange(columnNames, 0, numPartialUpdateColumns));
            put.setHeader("columns", columnMapping);
        }
        put.setEntity(new ByteArrayEntity(arrayOutput.getBuffer(), 0, arrayOutput.size()));
        CloseableHttpResponse response = client.execute(put);
        final int status = response.getStatusLine().getStatusCode();
        String result = "null";
        if (response.getEntity() != null) {
            result =  EntityUtils.toString(response.getEntity());
        }
        Matcher matcher = txnIdPattern.matcher(result);
        if (matcher.find()) {
            txnId = Long.valueOf(matcher.group(1));
        } else {
            txnId = -1;
        }
        if (status != 200 || !resultOK(result)) {
            throw new Exception(String.format("stream load: db=%s,table=%s label=%s failed, err=%s", db, table, label, result));
        }
    }

    public void send() throws Exception {
        if (dryRun) {
            Thread.sleep(1000);
            return;
        }
        out.close();
        if (dumpFile) {
            File f = new File(label + ".csv");
            FileOutputStream fos = new FileOutputStream(f);
            fos.write(arrayOutput.getBuffer(), 0, arrayOutput.size());
            fos.close();
        }
        while (true) {
            try {
                sendInner();
                break;
            } catch (Exception e) {
                if (e.getMessage().contains("will be visible after a while")) {
                    LOG.warn(String.format("%s warning: %s", label, e.getMessage()));
                    Thread.sleep(retrySleepSec* 1000);
                    break;
                }
                if (retry > 0) {
                    LOG.warn(String.format("%s error: %s sleep %dsec and retry", label, e.getMessage(), retrySleepSec));
                    Thread.sleep(retrySleepSec * 1000);
                    retry--;
                } else {
                    throw e;
                }
            }
        }
    }

    public long getFileSize() {return arrayOutput.size();}

    public long getTxnId() { return txnId; }

    private static String basicAuthHeader(String username, String password) {
        final String tobeEncode = username + ":" + password;
        byte[] encoded = Base64.getEncoder().encode(tobeEncode.getBytes(StandardCharsets.UTF_8));
        return "Basic " + new String(encoded);
    }

    static class SharedByteArrayOutputStream extends ByteArrayOutputStream {
        public SharedByteArrayOutputStream(int size) {
            super(size);
        }

        public byte[] getBuffer() {
            return buf;
        }
    }
}
