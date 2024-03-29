package com.dorisdb.rtbench.schema;

public class IdStringColumn extends Column {
    public IdStringColumn(String name) {
        this.name = name;
        this.type = "string";
        this.defaultStr = "\"ViKLUIhAjqbFboC2OdxmW9g\"";
        this.isKey = true;
    }

    @Override
    Object generate(long idx, long seed, long updateSeed) {
        return "id" + idx;
    }
}
