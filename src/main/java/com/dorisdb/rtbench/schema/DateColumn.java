package com.dorisdb.rtbench.schema;

import java.util.Date;

import com.dorisdb.rtbench.Utils;

public class DateColumn extends Column {
    long min = 0;
    long days = 100;

    public DateColumn(String name, String minDate, int days) {
        this.name = name;
        this.type = "date";
        this.min = Utils.dateToTs(minDate);
        this.days = days;
    }

    @Override
    Object generate(long idx, long seed, long updateSeed) {
        long d;
        if (updatable) {
            d = (seed + updateSeed) % days;
        } else {
            d = seed % days;
        }
        return Utils.dateFormatter.format(new Date(this.min + d * 24L* 3600 * 1000));
    }
}
