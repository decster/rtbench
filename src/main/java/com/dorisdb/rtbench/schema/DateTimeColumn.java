package com.dorisdb.rtbench.schema;

import java.util.Date;

import com.dorisdb.rtbench.Utils;

public class DateTimeColumn extends Column {
    long min = 0;
    long cardinality = 100;

    public DateTimeColumn(String name, String minDateTime, int seconds) {
        this.name = name;
        this.type = "datetime";
        this.defaultStr = "\"1970-01-01 15:36:00\"";
        this.min = Utils.dateToTs(minDateTime);
        this.cardinality = seconds;
    }

    @Override
    Object generate(long idx, long seed, long updateSeed) {
        long d;
        if (updatable) {
            d = Math.floorMod(seed + updateSeed, cardinality);
        } else {
            d = Math.floorMod(seed, cardinality);
        }
        return Utils.dateTimeFormatter.format(new Date(this.min + d * 1000));
    }

}
