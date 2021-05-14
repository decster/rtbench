package com.decster.rtbench;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.typesafe.config.Config;

public abstract class Workload {
    private static final Logger LOG = LogManager.getLogger(Workload.class);
    protected Config conf;
    protected WorkloadHandler handler;

    long epochDuration;
    long startTs;
    long endTs;
    static SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd_HHmmss");

    static long stringToTs(String str) throws Exception {
        Date sd = formatter.parse(str);
        return sd.getTime() / 1000;
    }

    static String tsToString(long ts) {
        return formatter.format(new Date(ts*1000));
    }

    public void init(Config conf, WorkloadHandler handler) throws Exception {
        this.conf = conf;
        this.handler = handler;
        epochDuration = conf.getDuration("epoch_duration").getSeconds();
        String startTss = conf.getString("start_time");
        startTs = stringToTs(startTss);
        if (conf.hasPath("end_time")) {
            String endTss = conf.getString("end_time");
            endTs = stringToTs(endTss);
        } else {
            endTs = -1;
        }
    }

    abstract public void setup();

    abstract public void processEpoch(long id, long epochTs, long duration);

    abstract public void close();

    public void run() {
        handler.onSetupBegin();
        setup();
        handler.onSetupEnd();
        long curTs = startTs;
        long id = 0;
        while (true) {
            if (endTs > 0 && curTs >= endTs) {
                break;
            }
            String epochName = tsToString(curTs);
            LOG.info("start epoch " + id + " " + epochName);
            handler.onEpochBegin(id, epochName);
            processEpoch(id, curTs, epochDuration);
            LOG.info("end epoch " + id + " " + epochName);
            handler.onEpochEnd(id, epochName);
            curTs += epochDuration;
            id++;
        }
        LOG.info("close workload");
        close();
        handler.onClose();
    }
}
