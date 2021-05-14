package com.decster.rtbench;

public interface WorkloadHandler {
    void onSetupBegin();
    void onSqlOperation(SqlOperation op);
    void onSetupEnd();
    void onEpochBegin(long id, String name);
    void onDataOperation(DataOperation op);
    void onEpochEnd(long id, String name);
    void onClose();
}
