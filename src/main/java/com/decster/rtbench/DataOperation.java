package com.decster.rtbench;

import com.google.gson.JsonArray;

public class DataOperation {
    public enum Op {
        UPSERT,
        DELETE
    }
    public Op op = Op.UPSERT;
    public String table;
    public String[] fieldNames;
    public JsonArray fields;
}
