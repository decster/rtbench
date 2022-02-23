package com.dorisdb.rtbench.schema;

public class IdStringColumn extends Column {
    public IdStringColumn(String name) {
        this.name = name;
        this.type = "string";
        this.defaultStr = "\"ViKLUIhAjqbFboC2OdxmW9gJ4DB7N4taokerrJmky4lGLiwvElZ5gMxQB7Z6nZDX6X8KdmF0Ad2VFkxaXtXEP02vxruPdeCV0zRm4P7c4pBpWhKFgtKl3NjrVMICtomRlW4KbvDKsyS62bIUEyTD9g3vacVUmsZU6wrPvTny7q0ADvtmN9ccJIzWAVKJZjFBA1RD47vshT1fFWx8zawDSGSmWo2sfpSgjjTYi1DfFkETqvWBkhNJXtFA27hCMHY\"";
        this.isKey = true;
    }

    @Override
    Object generate(long idx, long seed, long updateSeed) {
        return "id" + idx;
    }
}
