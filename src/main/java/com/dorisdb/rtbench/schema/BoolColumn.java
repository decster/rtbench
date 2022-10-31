package com.dorisdb.rtbench.schema;

public class BoolColumn extends Column {
    public BoolColumn(String name) {
        this.name = name;
        this.type = "boolean";
    }

    @Override
    Object generate(long idx, long seed, long updateSeed) {
        if (updatable) {
            return Math.floorMod(seed + updateSeed, 2L) == 0;
        } else {
            return Math.floorMod(seed, 2L) == 0;
        }
    }
}
