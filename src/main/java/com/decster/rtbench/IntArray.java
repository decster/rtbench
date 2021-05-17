package com.decster.rtbench;

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
}
