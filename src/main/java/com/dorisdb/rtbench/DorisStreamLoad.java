package com.dorisdb.rtbench;

import java.io.File;
import java.io.PrintWriter;
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

import com.dorisdb.rtbench.DataOperation.Op;

public class DorisStreamLoad {
    Logger LOG = LogManager.getLogger(DorisStreamLoad.class);
    String addr;
    String db;
    String table;
    String url;
    String label;
    boolean dryRun;
    String authHeader;
    File outFile;
    boolean keepFile;
    PrintWriter out;
    long opCount;

    static final String randLabelSuffix = "_" + Utils.newRandShortID(4);

    public DorisStreamLoad(String addr, String db, String table, String label, String tmpDir, boolean keepFile, boolean dryRun) {
        this.addr = addr;
        this.db = db;
        this.table = table;
        this.url = String.format("http://%s/api/%s/%s/_stream_load", addr, db, table);
        this.authHeader = basicAuthHeader("root", "");
        this.label = label;
        this.outFile = new File(String.format("%s/%s.data", tmpDir, label));
        this.keepFile = keepFile;
        this.dryRun = dryRun;
        this.opCount = 0;
    }

    public PrintWriter getWriter() throws Exception {
        if (out == null) {
            out = new PrintWriter(this.outFile, StandardCharsets.UTF_8.name());
        }
        return out;
    }

    static final char FIELD_SEP = '\001';
    static final char [] NULL_FIELD = new char[] {'\\', 'N'};

    public void addData(DataOperation op) throws Exception {
        opCount++;
        if (dryRun) {
            return;
        }
        PrintWriter out = getWriter();
        if (op.op == Op.INSERT || op.op == Op.UPSERT) {
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
            out.append('\n');
        } else {
            throw new Exception("op type not support");
        }
    }

    public void send() throws Exception {
        if (out == null) {
            return;
        }
        try {
            out.close();
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
            put.setEntity(new FileEntity(outFile));
            CloseableHttpResponse response = client.execute(put);
            final int status = response.getStatusLine().getStatusCode();
            String result = "null";
            if (response.getEntity() != null) {
                result =  EntityUtils.toString(response.getEntity());
            }
            if (status != 200 || !result.contains("OK")) {
                throw new Exception(String.format("doris stream load: db=%s,table=%s label=%s failed, err=%s", db, table, label, result));
            }
        } finally {
            if (!keepFile) {
                outFile.delete();
            }
        }
    }

    private static String basicAuthHeader(String username, String password) {
        final String tobeEncode = username + ":" + password;
        byte[] encoded = Base64.getEncoder().encode(tobeEncode.getBytes(StandardCharsets.UTF_8));
        return "Basic " + new String(encoded);
    }
}