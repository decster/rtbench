package com.dorisdb.rtbench.schema;

public class IntColumn extends Column {
    int min = 0;
    long cardinality = 100;

    public IntColumn(String name, int min, int cardinality) {
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
