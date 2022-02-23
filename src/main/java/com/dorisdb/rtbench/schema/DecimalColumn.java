package com.dorisdb.rtbench.schema;

public class DecimalColumn extends Column {
    long min = 0;
    long cardinality = 100;
    int p = 9;
    int s = 2;
    long sf = 1;
    String fmt;

    public DecimalColumn(String name, int p, int s, int min, int cardinality) {
        this.name = name;
        this.type = String.format("decimal(%d,%d)", p, s);
        this.defaultStr = "\"1234567890123456789012345.12\"";
        this.p = p;
        this.s = s;
        for (int i = 0; i<s;i++) {
            this.sf *= 10;
        }
        this.min = min;
        this.cardinality = cardinality;
        this.fmt = String.format("%%d.%%0%dd", s);
    }

    @Override
    Object generate(long idx, long seed, long updateSeed) {
        long v;
        if (updatable) {
            v = (seed + updateSeed) % cardinality + min;
        } else {
            v = seed % cardinality + min;
        }
        return String.format(fmt, v/sf, v%sf);
    }
}
