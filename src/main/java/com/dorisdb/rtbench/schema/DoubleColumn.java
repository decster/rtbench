package com.dorisdb.rtbench.schema;

public class DoubleColumn extends Column {
    long min = 0;
    long cardinality = 100;
    double divide = 100.0;

    public DoubleColumn(String name, int min, int cardinality, double divide) {
        this.name = name;
        this.type = "double";
        this.defaultStr = "\"12345678901234567890123456789012345678901234567890.1234567890123456789\"";
        this.min = min;
        this.cardinality = cardinality;
        this.divide = divide;
    }

    @Override
    Object generate(long idx, long seed, long updateSeed) {
        long v;
        if (updatable) {
            v = (seed + updateSeed) % cardinality + min;
        } else {
            v = seed % cardinality + min;
        }
        return v/divide;
    }
}
