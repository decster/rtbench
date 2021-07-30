package com.dorisdb.rtbench.schema;

import java.util.Date;

import com.dorisdb.rtbench.Utils;

public class DateTimeColumn extends Column {
    long min = 0;
    long cardinality = 100;

    public DateTimeColumn(String name, String minDateTime, int seconds) {
        this.name = name;
        this.type = "datetime";
        this.min = Utils.dateToTs(minDateTime);
        this.cardinality = seconds;
    }

    @Override
    Object generate(long idx, long seed, long updateSeed) {
        long d;
        if (updatable) {
            d = (seed + updateSeed) % cardinality;
        } else {
            d = seed % cardinality;
        }
        return Utils.dateTimeFormatter.format(new Date(this.min + d * 1000));
    }

}
