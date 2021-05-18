package com.dorisdb.rtbench;

import com.typesafe.config.Config;

public class Locations {
    public static class Location {
        public String city;
        public String province;
        public String country;
    }

    Location [] collection;
    public Locations(Config conf) {
        collection = new Location[1135];
        for (int i=0;i<collection.length-1;i++) {
            Location lc = new Location();
            lc.city = String.format("c%d", i);
            lc.province = String.format("p%d", i%43);
            lc.country = "China";
            collection[i] = lc;
        }
        Location other = new Location();
        other.city = "other";
        other.province = "other";
        other.country = "Other";
        collection[collection.length-1] = other;
    }

    public Location sample(long v) {
        return collection[(int)(v % collection.length)];
    }
}
