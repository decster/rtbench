package com.dorisdb.rtbench;
import com.typesafe.config.ConfigFactory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.dorisdb.rtbench.ecom.EcomWorkload;
import com.dorisdb.rtbench.xyd.XydWorkload;
import com.typesafe.config.Config;

public class Main {
    private static final Logger LOG = LogManager.getLogger(Main.class);

    public static void main(String [] args) throws Exception {
        Config conf = ConfigFactory.load();
        Workload workload;
        String workloadName = conf.getString("workload");
        if (workloadName.equals("ecom")) {
            workload = new EcomWorkload();
        } else if (workloadName.equals("xyd")) {
            workload = new XydWorkload();
        } else {
            throw new Exception("workload not found: " + workloadName);
        }
        WorkloadHandler handler;
        String handlerType = conf.getString("handler.type");
        LOG.info("Handler: " + handlerType);
        if (handlerType.equals("mysql")) {
            handler = new MysqlHandler();
        } else if (handlerType.equals("dorisdb")){
            handler = new DorisDBHandler();
        } else if (handlerType.equals("file")) {
            handler = new FileHandler();
        } else {
            throw new Exception("bad handler type: " + handlerType);
        }
        workload.init(conf, handler);
        handler.init(conf, workload);
        workload.run();
    }
}
