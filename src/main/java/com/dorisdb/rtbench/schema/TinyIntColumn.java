package com.dorisdb.rtbench.schema;

public class TinyIntColumn extends Column {
    int min = 0;
    int cardinality = 100;

    public TinyIntColumn(String name, int min, int cardinality) {
        this.name = name;
        this.type = "tinyint";
        this.defaultStr = "\"127\"";
        this.min = min;
        this.cardinality = cardinality;
    }

    @Override
    Object generate(long idx, long seed, long updateSeed) {
        if (updatable) {
            return (byte)((seed + updateSeed) % cardinality + min);
        } else {
            return (byte)(seed % cardinality + min);
        }
    }
}
