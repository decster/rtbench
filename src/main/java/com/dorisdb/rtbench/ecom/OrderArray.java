package com.dorisdb.rtbench.ecom;

import com.dorisdb.rtbench.IntArray;
import com.dorisdb.rtbench.LongArray;

public class OrderArray {
    LongArray data;
    public OrderArray(int capacity) {
        data = new LongArray(0, capacity*2);
    }

    public int getSize() {
        return data.getSize()/2;
    }

    public void append(long [] src, int start, int len) throws Exception {
        if (len % 2 == 1) {
            throw new Exception("append len must multiple of 2");
        }
        data.append(src, start, len);
    }

    public int getStartTs(int i) {
        long d = data.get(i*2+1);
        return (int)(d >>> 32);
    }

    public int getNextEventTs(int i) {
        long d = data.get(i*2+1);
        return (int)(d & 0xffffffffL);
    }

    public void setNextEventTs(int i, int ts) {
        long d = data.get(i*2+1);
        d = (d & 0xffffffff00000000L) | ts;
        data.set(i*2+1, d);
    }

    public long getId(int i) {
        return data.get(i*2);
    }

    /**
     * @param idxs all elements must be sorted
     */
    public void remove(IntArray idxs) throws Exception {
        if (idxs.getSize() == 0) {
            return;
        }
        if (idxs.get(idxs.getSize()-1) >= getSize()) {
            throw new Exception("remove index out of range");
        }
        int sz = getSize() - idxs.getSize();
        int j = idxs.get(0);
        int ji= 1;
        for (int i = j; i < sz; i++) {
            j++;
            while (ji<idxs.getSize() && j == idxs.get(ji)) {
                ++j;
                ++ji;
            }
            data.set(i*2, data.get(j*2));
            data.set(i*2+1, data.get(j*2+1));
        }
        data.resize(sz * 2);
    }
}
