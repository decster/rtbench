package com.dorisdb.rtbench;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Random;

import org.apache.commons.math3.distribution.PoissonDistribution;
import org.apache.commons.math3.random.Well19937c;

public class Utils {

    static SimpleDateFormat tsFormatter = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
    public static String tsToString(int ts) {
        return tsFormatter.format(new Date((long)ts * 1000));
    }

    public static long RAND_MAX = 1L << 48;

    public static long nextRand(long v) {
        return (v * 0x5DEECE66DL + 0xBL) & ((1L << 48) - 1);
    }

    public static long nextRand(long v, long seed) {
        return (v * (seed ^ 0x5DEECE66DL) + 0xBL) & ((1L << 48) - 1);
    }

    static public class PowerDist {
        int minInt;
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
            this.minInt = min;
            double alpha_1 = -alpha + 1;
            this.min = min;
            this.a = Math.pow((double)max / (double)min, alpha_1);
            this.exp = 1 / alpha_1;
        }

        public PowerDist(int min, int max) {
            this(min, max, 2.5f);
        }

        /**
         * @param x uniform sample [0, 1)
         * @return power distribution sample
         */
        public int sample(double x) {
            double v = min * Math.pow(a + x, exp);
            return Math.max((int)v, minInt);
        }

        /**
         * @param v uniform sample [0, RAND_MAX)
         * @return power distribution sample
         */
        public int sample(long v) {
            double x = (double)v / RAND_MAX;
            return sample(x);

        }

        public static void tests(int min, int max) {
            PowerDist d = new PowerDist(min, max);
            long f = 1111;
            for (int i = 0;i<20;i++) {
                f = nextRand(f);
                int v = d.sample(f);
                System.out.print(v);
                System.out.print(" ");
            }
            System.out.println();
        }
    }

    static public class Poisson {
        PoissonDistribution poisson;
        int factor = 1;

        public Poisson(double mean, long seed) {
            if (mean >= 10000) {
                mean /= 10;
                factor = 10;
            } else if (mean >= 100000) {
                mean /= 100;
                factor = 100;
            }
            poisson = new PoissonDistribution(new Well19937c(seed), mean, 1.0E-12D, 10000000);
        }

        public int next() {
            return poisson.sample() * factor;
        }
    }

    static final char[] IDCHARS = "abcdefghijklmnopqrstuvwxyzABSDEFGHIJKLMNOPQRSTUVWXYZ1234567890".toCharArray();
    static String newRandShortID(int len) {
        Random r = new Random(System.currentTimeMillis());
        char[] id = new char[len];
        for (int i = 0;  i < len;  i++) {
            id[i] = IDCHARS[r.nextInt(IDCHARS.length)];
        }
        return new String(id);
    }

    public static SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");
    public static long dateToTs(String date) {
        try {
            return dateFormatter.parse(date).getTime();
        } catch (Exception e) {
            return 0L;
        }
    }

    public static SimpleDateFormat dateTimeFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    public static long dateTimeToTs(String dateTime) {
        try {
            return dateTimeFormatter.parse(dateTime).getTime();
        } catch (Exception e) {
            return 0L;
        }
    }

}
