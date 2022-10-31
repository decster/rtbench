package com.dorisdb.rtbench.schema;

public class BigIntColumn extends Column {
    long min = 0;
    long cardinality = 100;

    public BigIntColumn(String name, long min, long cardinality) {
        this.name = name;
        this.type = "int";
        this.defaultStr = "\"2147\"";
        this.min = min;
        this.cardinality = cardinality;
    }

    @Override
    Object generate(long idx, long seed, long updateSeed) {
        long v;
        if (updatable) {
            v = Math.floorMod(seed + updateSeed, cardinality) + min;
        } else {
            v = Math.floorMod(seed, cardinality) + min;
        }
        return v;
    }
}
