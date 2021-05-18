package com.dorisdb.rtbench.ecom;

import static org.junit.Assert.*;

import org.junit.Test;

import com.dorisdb.rtbench.IntArray;
import com.dorisdb.rtbench.ecom.OrderArray;

public class OrderArrayTest {

    @Test
    public void testRemove() throws Exception {
        OrderArray oa = new OrderArray(100);
        IntArray dels = new IntArray(0);
        for (int i=0;i<100;i++) {
            oa.append(new long[] {i, (1L<<32L) | 1L}, 0, 2);
            if (i%5==0 || i%5==1 || i%5==4) {
                dels.append(i);
            }
        }
        oa.remove(dels);
        assertEquals(40, oa.getSize());
        for (int i=0;i<oa.getSize();i++) {
            long id = oa.getId(i);
            assertEquals(i/2 * 5 + 2 + i%2, id);
        }
    }

}
