package com.dorisdb.rtbench;

import static org.junit.Assert.assertTrue;
import org.junit.Test;

import com.dorisdb.rtbench.Utils;

public class UtilsTest {
    void t(double mean, int t) {
        Utils.Poisson p = new Utils.Poisson(mean, 1);
        long start = System.nanoTime();
        double total = 0;
        for (int i=0;i<t;i++) {
            int c = p.next();
            total += c;
        }
        long end = System.nanoTime();
        System.out.printf("mean %.2f -> %.2f average time: %.1f ns\n", mean, total/t, (end - start) / (double)t);
    }
    @Test
    public void testPoisson() {
        t(0.1, 1000);
        t(0.5, 1000);
        t(1, 1000);
        t(10, 1000);
        t(100, 1000);
        t(1000, 1000);
        t(10000, 1000);
        t(50000, 1000);
    }

    @Test
    public void testPowerDist() {
        Utils.PowerDist p = new Utils.PowerDist(10, 60, 3);
        long r = Utils.nextRand(1, 1);
        for (int i=0;i<100000;i++) {
            r = Utils.nextRand(r);
            int v = p.sample(r);
            assertTrue(v >= 10 && v < 60);
        }
    }
}