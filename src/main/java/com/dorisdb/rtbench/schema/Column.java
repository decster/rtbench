package com.dorisdb.rtbench.schema;

public abstract class Column {
    public String name;
    public String type;
    public boolean isKey = false;
    public boolean nullable = false;
    public boolean updatable = false;

    abstract Object generate(long idx, long seed, long updateSeed);
}
