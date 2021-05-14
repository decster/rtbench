package com.decster.rtbench;
import com.typesafe.config.ConfigFactory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.decster.rtbench.ecom.EcomWorkload;
import com.typesafe.config.Config;

public class Main {
    private static final Logger LOG = LogManager.getLogger(Main.class);

    public static void main(String [] args) throws Exception {
        Config conf = ConfigFactory.load();
        EcomWorkload workload = new EcomWorkload();
        FileHandler handler = new FileHandler();
        workload.init(conf, handler);
        workload.run();
    }
}
