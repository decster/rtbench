package com.dorisdb.rtbench;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.typesafe.config.Config;

public class ConcurrentDorisStreamLoad implements DorisLoad {
    Logger LOG = LogManager.getLogger(ConcurrentDorisStreamLoad.class);
    String table;
    int numShard;
    String label;
    long opCount = 0;
    DorisStreamLoad[] loads;

    public ConcurrentDorisStreamLoad(Config conf, String db, String table, String label, int numShard) {
        this.numShard = numShard;
        this.label = label;
        this.table = table;
        loads = new DorisStreamLoad[numShard];
        for (int i=0;i<numShard;i++) {
            loads[i] = new DorisStreamLoad(conf, db, table, String.format("%s_%d", label, i));
        }
    }

    public void addData(DataOperation op) throws Exception {
        Object k = op.fullFields[0];
        int shard;
        if (k instanceof Integer) {
            shard = ((Integer) k).intValue() % numShard;
        } else if (k instanceof Long) {
            shard = ((Long) k).intValue() % numShard;
        } else if (k instanceof String) {
            shard = ((String) k).hashCode() % numShard;
        } else {
            throw new Exception("key type not support");
        }
        loads[shard].addData(op);
        opCount++;
    }

    public void send() throws Exception {
        Thread[] threads = new Thread[loads.length];
        Exception[] rets = new Exception[loads.length];
        LOG.info(String.format("send %d loads", loads.length));
        for (int i=0;i<threads.length;i++) {
            final int idx = i;
            threads[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        loads[idx].send();
                    } catch (Exception e) {
                        rets[idx] = e;
                    }
                }
            });
        }
        for (int i=0;i<threads.length;i++) {
            threads[i].start();
        }
        for (int i=0;i<threads.length;i++) {
            threads[i].join();
        }
        LOG.info(String.format("send %d loads done", loads.length));
        for (int i=0;i<threads.length;i++) {
            if (rets[i] != null) {
                throw rets[i];
            }
        }
    }

    public String getTable() {
        return table;
    }

    public String getLabel() {
        return label;
    }

    public long getOpCount() {
        return opCount;
    }

    @Override
    public long getFileSize() { return 0; }
}
