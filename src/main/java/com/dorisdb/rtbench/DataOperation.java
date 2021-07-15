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

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(op.name());
        sb.append(": ");
        for (int i=0;i<fullFields.length;i++) {
            if (i > 0) {
                sb.append(",");
            }
            Object f = fullFields[i];
            if (f != null) {
                if (f instanceof Number) {
                    sb.append(f.toString());
                } else if (f instanceof String) {
                    sb.append((String)f);
                }
            } else {
                sb.append("\\N");
            }
        }
        return sb.toString();
    }
}
