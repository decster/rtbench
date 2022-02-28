package com.dorisdb.rtbench;

public interface DorisLoad {
    public void addData(DataOperation op) throws Exception;
    public void send() throws Exception;

    public String getTable();
    public String getLabel();

    public long getOpCount();

    public long getFileSize();
}
