package com.decster.rtbench;

public class Utils {
    public static long RAND_MAX = 1L << 48;

    public static long nextRand(long v) {
        return (v * 0x5DEECE66DL + 0xBL) & ((1L << 48) - 1);
    }

    public static long nextRand(long v, long seed) {
        return (v * (seed ^ 0x5DEECE66DL) + 0xBL) & ((1L << 48) - 1);
    }

    static public class PowerDist {
        double min;
        double a;
        double exp;
        /**
         * @param min
         * @param max
         * @param alpha ~2.5 range
         * y = min * ((max/min)^(alpha+1) + 1 - x)^(1/(alpha+1))
         */
        public PowerDist(int min, int max, float alpha) {
            double alpha_1 = -alpha + 1;
            this.min = min;
            this.a = Math.pow((double)max / (double)min, alpha_1);
            this.exp = 1 / alpha_1;
        }

        public PowerDist(int min, int max) {
            this(min, max, 2.5f);
        }

        /**
         * @param v uniform sample [0, 1)
         * @return power distribution sample
         */
        public int sample(double x) {
            return (int)(min * Math.pow(a + x, exp));
        }

        /**
         * @param v uniform sample [0, RAND_MAX)
         * @return power distribution sample
         */
        public int sample(long v) {
            double x = (double)v / RAND_MAX;
            return sample(x);
        }
    }
}
