package com.decster.rtbench;

public interface WorkloadHandler {
    void onSetupBegin();
    void onSqlOperation(SqlOperation op);
    void onSetupEnd();
    void onEpochBegin(int id, String name);
    void onDataOperation(DataOperation op);
    void onEpochEnd(int id, String name);
}
