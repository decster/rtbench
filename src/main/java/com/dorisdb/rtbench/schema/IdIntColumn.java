package com.dorisdb.rtbench.schema;

public class IdIntColumn  extends Column {
    public IdIntColumn(String name) {
        this.name = name;
        this.type = "int";
        this.isKey = true;
        this.defaultStr = "\"21474\"";
    }

    @Override
    Object generate(long idx, long seed, long updateSeed) {
        return Integer.valueOf((int)idx);
    }
}
