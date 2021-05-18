package com.dorisdb.rtbench;

import java.io.File;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.typesafe.config.Config;

public class FileHandler implements WorkloadHandler {
    private static final Logger LOG = LogManager.getLogger(FileHandler.class);
    Config conf;
    Workload load;
    boolean dryRun;
    String outputDir;
    PrintWriter curSqlStream;
    String stageName;
    Map<String, PrintWriter> writerByTable;
    String fieldDelimiter;
    String fieldNull;

    PrintWriter getWriterForTable(String table) throws Exception {
        PrintWriter ret = writerByTable.get(table);
        if (ret == null) {
            ret = new PrintWriter(String.format("%s/%s_%s.csv", outputDir, table, stageName));
            writerByTable.put(table, ret);
        }
        return ret;
    }

    void closeTableWriters() throws Exception {
        for (Map.Entry<String, PrintWriter> kv : writerByTable.entrySet()) {
            kv.getValue().close();
        }
        writerByTable.clear();
    }

    @Override
    public void init(Config conf, Workload load) throws Exception {
        this.conf = conf;
        this.load = load;
        this.dryRun = conf.getBoolean("dry_run");
        this.outputDir = conf.getString("handler.file.output_dir");
        this.fieldDelimiter = conf.getString("handler.file.field_delimiter");
        this.fieldNull = conf.getString("handler.file.field_null");
        if (dryRun) {
            return;
        }
        File d = new File(outputDir);
        if (d.isFile()) {
            LOG.error("output_dir is a file: " + outputDir);
            throw new Exception("output_dir is a file");
        } else if (d.isDirectory()) {
            if (conf.getBoolean("cleanup")) {
                LOG.info("Cleanup directory: " + outputDir);
                FileUtils.deleteDirectory(d);
            }
        }
        d.mkdirs();
    }

    @Override
    public void onSetupBegin() throws Exception {
        if (!dryRun) {
            curSqlStream = new PrintWriter(outputDir + "/setup.sql");
        }
        stageName = "setup";
        writerByTable = new HashMap<>();
    }

    @Override
    public void onSetupEnd() throws Exception {
        if (curSqlStream != null) {
            curSqlStream.close();
            curSqlStream = null;
        }
        closeTableWriters();
    }

    @Override
    public void onEpochBegin(long id, String name) throws Exception {
        stageName = name;
    }

    @Override
    public void onEpochEnd(long id, String name) throws Exception {
        closeTableWriters();
    }

    @Override
    public void onSqlOperation(SqlOperation op) throws Exception {
        if (!dryRun) {
            curSqlStream.append(op.sql);
            curSqlStream.append(";\n");
        }
    }

    @Override
    public void onDataOperation(DataOperation op) throws Exception {
        if (dryRun) {
            return;
        }
        PrintWriter out = getWriterForTable(op.table);
        for (int i=0;i<op.fullFields.length;i++) {
            if (i > 0) {
                out.append(fieldDelimiter);
            }
            Object f = op.fullFields[i];
            if (f != null) {
                if (f instanceof Number) {
                    out.append(f.toString());
                } else if (f instanceof String) {
                    out.append((String)f);
                }
            } else {
                out.append(fieldNull);
            }
        }
        out.append('\n');
    }

    @Override
    public void flush() throws Exception {
        for (Map.Entry<String, PrintWriter> kv : writerByTable.entrySet()) {
            kv.getValue().flush();
        }
    }

    @Override
    public void onClose() throws Exception {
    }
}
