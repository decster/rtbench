package com.dorisdb.rtbench.schema;

public class StringColumn extends Column {
    String prefix;
    long min = 0;
    long cardinality = 100;

    public StringColumn(String name, String prefix, int min, int cardinality) {
        this.name = name;
        this.type = "varchar(255)";
        this.defaultStr = "\"ViKLUIhAjqbFbo\"";
        this.prefix = prefix;
        this.min = min;
        this.cardinality = cardinality;
    }

    @Override
    Object generate(long idx, long seed, long updateSeed) {
        long v;
        if (updatable) {
            v = (seed + updateSeed) % cardinality + min;
        } else {
            v = seed % cardinality + min;
        }
        return prefix + v;
    }
}
