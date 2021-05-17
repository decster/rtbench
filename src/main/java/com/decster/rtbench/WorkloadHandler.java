package com.decster.rtbench;

import com.typesafe.config.Config;

public interface WorkloadHandler {
	void init(Config conf, Workload load) throws Exception;

	void onSqlOperation(SqlOperation op) throws Exception;

	void onDataOperation(DataOperation op) throws Exception;

	void onSetupBegin() throws Exception;

	void onSetupEnd() throws Exception;

	void onEpochBegin(long id, String name) throws Exception;

	void onEpochEnd(long id, String name) throws Exception;

	void onClose() throws Exception;
}
