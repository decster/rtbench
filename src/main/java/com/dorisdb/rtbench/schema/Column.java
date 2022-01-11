package com.dorisdb.rtbench.schema;

public abstract class Column implements java.lang.Cloneable {
    public String name;
    public String type;
    public boolean isKey = false;
    public boolean nullable = false;
    public boolean updatable = false;

    @Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    public void setName(String name) {
        this.name = name;
    }

    abstract Object generate(long idx, long seed, long updateSeed);
}
