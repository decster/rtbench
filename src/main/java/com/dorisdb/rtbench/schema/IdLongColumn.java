package com.dorisdb.rtbench.schema;

public class IdLongColumn extends Column {
    public IdLongColumn(String name) {
        this.name = name;
        this.type = "bigint";
        this.defaultStr = "\"922337\"";
        this.isKey = true;
    }

    @Override
    Object generate(long idx, long seed, long updateSeed) {
        return Long.valueOf(idx);
    }
}
