package com.decster.rtbench.ecom;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.decster.rtbench.Workload;

public class EcomWorkload extends Workload {
    private static final Logger LOG = LogManager.getLogger(EcomWorkload.class);

    private Users users;
    private Merchants merchants;
    private Goods goods;
    private Orders orders;

    public EcomWorkload() {
    }

    @Override
    public void setup() {
        users = new Users(conf);
        merchants = new Merchants(conf);
        goods = new Goods(merchants, conf);
        orders = new Orders(conf);
    }

    @Override
    public void processEpoch(long id, long epochTs, long duraton) {

    }

    @Override
    public void close() {
        // TODO Auto-generated method stub

    }
}
