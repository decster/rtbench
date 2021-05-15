package com.decster.rtbench;

import java.io.File;
import java.io.PrintWriter;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.typesafe.config.Config;

public class FileHandler implements WorkloadHandler {
    private static final Logger LOG = LogManager.getLogger(FileHandler.class);
    Config conf;
    Workload load;
    String outputDir;
    PrintWriter curSqlStream;

    @Override
    public void init(Config conf, Workload load) throws Exception {
        this.conf = conf;
        this.load = load;
        this.outputDir = conf.getString("handler.output_dir");
        File d = new File(outputDir);
        if (d.isFile()) {
            LOG.error("output_dir is a file: " + outputDir);
        }
        d.mkdirs();
    }

    @Override
    public void onSetupBegin() throws Exception {
        curSqlStream = new PrintWriter(outputDir + "/setup.sql");
    }

    @Override
    public void onSqlOperation(SqlOperation op) throws Exception {
        LOG.info("sql: " + op.sql);
        curSqlStream.append(op.sql);
        curSqlStream.append(";\n");
    }

    @Override
    public void onSetupEnd() throws Exception {
        if (curSqlStream != null) {
            curSqlStream.close();
            curSqlStream = null;
        }
    }

    @Override
    public void onEpochBegin(long id, String name) throws Exception {
    }

    @Override
    public void onDataOperation(DataOperation op) throws Exception {
    }

    @Override
    public void onEpochEnd(long id, String name) throws Exception {
    }

    @Override
    public void onClose() throws Exception {
    }
}
