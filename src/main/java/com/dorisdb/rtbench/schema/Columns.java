package com.dorisdb.rtbench.schema;

public class Columns {
    public static Column IDINT(String name) {
        Column ret = new IdIntColumn(name);
        return ret;
    }

    public static Column IDLONG(String name) {
        Column ret = new IdLongColumn(name);
        return ret;
    }

    public static Column BOOL(String name) {
        Column ret = new BoolColumn(name);
        return ret;
    }

    public static Column TINYINT(String name, int min, int num) {
        Column ret = new TinyIntColumn(name, min, num);
        return ret;
    }

    public static Column INT(String name, int min, int num) {
        Column ret = new IntColumn(name, min, num);
        return ret;
    }

    public static Column DOUBLE(String name, int min, int num, double divide) {
        Column ret = new DoubleColumn(name, min, num, divide);
        return ret;
    }

    public static Column DATE(String name, String min, int num) {
        Column ret = new DateColumn(name, min, num);
        return ret;
    }

    public static Column DATETIME(String name, String min, int num) {
        Column ret = new DateTimeColumn(name, min, num);
        return ret;
    }

    public static Column STRING(String name, String prefix, int min, int num) {
        Column ret = new StringColumn(name, prefix, min, num);
        return ret;
    }

    public static Column DECIMAL(String name, int p, int s, int min, int num) {
        Column ret = new DecimalColumn(name, p, s, min, num);
        return ret;
    }

    public static Column N(Column c) {
        c.nullable = true;
        return c;
    }

    public static Column U(Column c) {
        c.updatable = true;
        return c;
    }

    public static Column K(Column c) {
        c.isKey = true;
        c.updatable = false;
        c.nullable = false;
        return c;
    }
}
