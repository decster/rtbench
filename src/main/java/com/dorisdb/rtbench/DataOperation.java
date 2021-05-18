package com.dorisdb.rtbench;

public class DataOperation {
    public enum Op {
        INSERT,
        UPSERT,
        UPDATE,
        DELETE
    }
    public Op op = Op.INSERT;
    public String table;
    public String[] fullFieldNames;
    public Object[] fullFields;
    public int[] keyFieldIdxs;
    // only update op needs to setup this field
    public int[] updateFieldIdxs;
}
