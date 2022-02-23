package com.dorisdb.rtbench.schema;

public class StringColumn extends Column {
    String prefix;
    long min = 0;
    long cardinality = 100;

    public StringColumn(String name, String prefix, int min, int cardinality) {
        this.name = name;
        this.type = "varchar(255)";
        this.defaultStr = "\"ViKLUIhAjqbFboC2OdxmW9gJ4DB7N4taokerrJmky4lGLiwvElZ5gMxQB7Z6nZDX6X8KdmF0Ad2VFkxaXtXEP02vxruPdeCV0zRm4P7c4pBpWhKFgtKl3NjrVMICtomRlW4KbvDKsyS62bIUEyTD9g3vacVUmsZU6wrPvTny7q0ADvtmN9ccJIzWAVKJZjFBA1RD47vshT1fFWx8zawDSGSmWo2sfpSgjjTYi1DfFkETqvWBkhNJXtFA27hCMHY\"";
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
