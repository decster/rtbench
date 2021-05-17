package com.decster.rtbench;

public class DataOperation {
    public enum Op {
        INSERT,
        UPDATE,
        DELETE
    }
    public Op op = Op.INSERT;
    public String table;
    public String[] fieldNames;
    public Object[] fields;
}
