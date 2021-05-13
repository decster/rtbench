package com.decster.rtbench;
import com.typesafe.config.ConfigFactory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.typesafe.config.Config;

public class Main {
    private static final Logger LOG = LogManager.getLogger(Main.class);

    public static void main(String [] args) {
        Config conf = ConfigFactory.load();
        String ddd = conf.getString("name");
        System.out.println("sasdflaksjldk aslkj: " + ddd);
    }
}
