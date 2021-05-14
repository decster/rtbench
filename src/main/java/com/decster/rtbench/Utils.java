package com.decster.rtbench;

public class Utils {
    public static long nextRand(long v) {
        return (v * 0x5DEECE66DL + 0xBL) & ((1L << 48) - 1);
    }

    public static long nextRand(long v, long seed) {
        return (v * (seed ^ 0x5DEECE66DL) + 0xBL) & ((1L << 48) - 1);
    }
}
