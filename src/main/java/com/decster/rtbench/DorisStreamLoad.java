package com.decster.rtbench;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DorisStreamLoad {
    Logger LOG = LogManager.getLogger(DorisStreamLoad.class);
    String addr;
    String db;
    String table;
    String url;
    String label;
    String authHeader;
    StringBuilder data;

    public DorisStreamLoad(String addr, String db, String table, String label) {
        this.addr = addr;
        this.db = db;
        this.table = table;
        this.url = String.format("htpp://%s/api/%s/%s/_stream_load");
        this.authHeader = basicAuthHeader("root", "");
        this.label = label;
    }

    public void addData(DataOperation op) throws Exception {

    }

    public void flush() throws Exception {
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
        put.setHeader("label", label);
        put.setHeader("format", "csv");
        put.setHeader("column_separator", ",");
        // TODO: put.setEntity(HttpEntity);
        CloseableHttpResponse response = client.execute(put);
        final int status = response.getStatusLine().getStatusCode();
        String result = "null";
        if (response.getEntity() != null) {
            result =  EntityUtils.toString(response.getEntity());
        }
        if (status != 200 || !result.contains("OK")) {
            throw new Exception(String.format("doris stream load: db=%s,table=%s label=%s failed, err=%s", db, table, label, result));
        }
    }

    private static String basicAuthHeader(String username, String password) {
        final String tobeEncode = username + ":" + password;
        byte[] encoded = Base64.getEncoder().encode(tobeEncode.getBytes(StandardCharsets.UTF_8));
        return "Basic " + new String(encoded);
    }
}
