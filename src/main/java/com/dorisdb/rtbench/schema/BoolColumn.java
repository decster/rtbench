package com.dorisdb.rtbench.schema;

public class BoolColumn extends Column {
    public BoolColumn(String name) {
        this.name = name;
        this.type = "boolean";
    }

    @Override
    Object generate(long idx, long seed, long updateSeed) {
        if (updatable) {
            return (seed + updateSeed) % 2 == 0;
        } else {
            return seed % 2 == 0;
        }
    }
}
