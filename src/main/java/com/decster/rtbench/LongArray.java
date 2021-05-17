package com.decster.rtbench;

import java.util.Arrays;

public class LongArray {
    long [] data;
    int size;
    public LongArray(int size) {
        this(size, size);
    }

    public LongArray(int size, int capacity) {
        data = new long[capacity];
        this.size = size;
    }

    public void append(long [] src, int start, int len) {
        int newSize = size + len;
        if (newSize > data.length) {
            long [] newData = new long[Math.max(newSize, newSize / 2 * 3)];
            System.arraycopy(this.data, 0, newData, 0, size);
            System.arraycopy(src, start, newData, size, len);
            data = newData;
        } else {
            System.arraycopy(src, start, data, size, len);
        }
        size = newSize;
    }

    public void resize(int newSize) {
        if (newSize > data.length) {
            long [] newData = new long[newSize];
            System.arraycopy(data, 0, newData, 0, size);
            data = newData;
        } else if (newSize > size) {
            Arrays.fill(data, size, newSize, 0);
        }
        size = newSize;
    }

    public int getSize() {
        return size;
    }

    public long get(int idx) {
        return data[idx];
    }

    public void set(int idx, long v) {
        data[idx] = v;
    }

    public long [] getData() {
        return data;
    }
}
