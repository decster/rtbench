package com.decster.rtbench;

import com.google.gson.JsonObject;

public class DataOperation {
    enum Op {
        UPSERT,
        DELETE
    }
    String table;
    Op op;
    JsonObject fields;
}
