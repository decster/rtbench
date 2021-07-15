package com.dorisdb.rtbench;

import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

public class IntArray {
    int [] data;
    int size;
    public IntArray(int size) {
        this(size, size);
    }

    public IntArray(int size, int capacity) {
        data = new int[capacity];
        this.size = size;
    }

    public void append(int [] src, int start, int len) {
        int newSize = size + len;
        if (newSize > this.data.length) {
            int [] newData = new int[Math.max(newSize, newSize / 2 * 3)];
            System.arraycopy(data, 0, newData, 0, size);
            System.arraycopy(src, start, newData, size, len);
            data = newData;
        } else {
            System.arraycopy(src, start, this.data, size, len);
        }
        size = newSize;
    }

    public void append(int v) {
        int newSize = size + 1;
        if (newSize > data.length) {
            int [] newData = new int[Math.max(newSize, newSize / 2 * 3)];
            System.arraycopy(this.data, 0, newData, 0, size);
            newData[size] = v;
            data = newData;
        } else {
            data[size] = v;
        }
        size = newSize;
    }

    public int getSize() {
        return size;
    }

    public int [] getData() {
        return data;
    }

    public int get(int idx) {
        return data[idx];
    }

    public int[] sample(int n, long seed) throws Exception {
        if (n > size) {
            throw new IllegalArgumentException("sample size > array size");
        } else if (n == size) {
            int[] ret = new int[n];
            for (int i=0;i<n;i++) {
                ret[i] = data[i];
            }
            return ret;
        }
        int[] ret = new int[n];
        if (n < 3 || n < size/2) {
            Set<Integer> s = new TreeSet<>();
            while(s.size() < n) {
                int v = data[(int)(seed % size)];
                s.add(v);
                seed = Utils.nextRand(seed);
            }
            int idx = 0;
            for (Integer v : s) {
                ret[idx++] = v;
            }
        } else {
            Set<Integer> s = new HashSet<>();
            int r = size - n;
            while(s.size() < r) {
                int v = data[(int)(seed % size)];
                s.add(v);
                seed = Utils.nextRand(seed);
            }
            int idx = 0;
            for (int i=0;i<size;i++) {
                if (!s.contains(data[i])) {
                    ret[idx++] = data[i];
                }
            }
        }
        return ret;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (int i=0;i<size;i++) {
            if (i > 0) {
                sb.append(",");
            }
            sb.append(data[i]);
        }
        return sb.toString();
    }

    /**
     * remove values in vs[start:start+n) from this array
     * @param vs must be sorted
     * @param start
     * @param n
     */
    public void remove(int[] vs, int start, int n) {
        int src = 0;
        int dst = 0;
        int cmp = start;
        while (src < size) {
            if (cmp < start+n && data[src] == vs[cmp]) {
                // match
                cmp++;
                src++;
            } else {
                data[dst] = data[src];
                dst++;
                src++;
            }
        }
        size = dst;
    }

    public void remove(int[] vs) throws Exception {
        remove(vs, 0, vs.length);
    }
}
