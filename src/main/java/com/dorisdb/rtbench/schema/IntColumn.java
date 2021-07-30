package com.dorisdb.rtbench.schema;

public class IntColumn extends Column {
    int min = 0;
    int cardinality = 100;

    public IntColumn(String name, int min, int cardinality) {
        this.name = name;
        this.type = "int";
        this.min = min;
        this.cardinality = cardinality;
    }

    @Override
    Object generate(long idx, long seed, long updateSeed) {
        if (updatable) {
            return (int)((seed + updateSeed) % cardinality + min);
        } else {
            return (int)(seed % cardinality + min);
        }
    }
}
