package com.decster.rtbench;

import com.typesafe.config.Config;

public abstract class Workload {
    Config conf;
    WorkloadHandler handler;

    long epochDuration;

    public void init(Config conf, WorkloadHandler handler) {
        this.conf = conf;
        this.handler = handler;
        epochDuration = conf.getDuration("epoch_duration").getSeconds();
    }

    public void setup() {
    }

    public void run() {
        handler.onSetupBegin();
        setup();
        handler.onSetupEnd();

    }

}
