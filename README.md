# rtbench: realtime OLAP workload benchmark tool

This tool simulate a ecommerce style realtime workload. It has 4 tables:

* users store all the users'(buyers') information
* merchants store all the merchants' information
* goods store all the goods' information
* orders store all the orders, after an order is created, it will change state multiple times in the near future days, and finally become fixed.

The workload has 2 stages: setup stage and real-time loading stage

Table users/merchants/goods is fixed, all data is prepared at setup stage.

Table orders is inserted&updated in real-time loading stage.

## Basic usage

1. prepare your database, mysql/dorisdb, or just use file handler

2. change some config in conf/application.conf,

3. then, run driver script

```
bin/rtbench.sh

```

## Basic config

```
dry_run=false               dry run only, only for test and validate config, do not load data
cleanup=true                cleanup tables before load
record_per_day=100000       load data size, like tcp-h's scale-factor,
                            100000 means there will be 100000 new orders generated per day,
                            each order will be updated several times in the near future,
                            so updates/operations per day is higher(currently ~3.6x) than record_per_day
start_time=20210501_000000  load data start timestamp
end_time  =20210520_000000  load data end timestmap
epoch_duration=1d           epoch duration, this tool will generate a load task per epoch,
                            e.g. 1d means load data once per day,
                                 each load task will create about record_per_day orders
                                 1h means load data once per hour,
                                 each load task will create about record_per_day/24 orders

db.type=doris               dest database type: doris/mysql
db.name=rtbench             dest database name
```


## Handlers

Currently 3 handler types are supported:

* file handler: write setup sql script and data to file, for latter usage
* mysql handler: load data to mysql directly using jdbc
* dorisdb handler: load data to dorisdb directly using jdbc and doris stream load


## Date sizes

All tables' size is based on record_per_day

* users     record_per_day * 5
* merchants record_per_day / 100
* goods     num_merchants * 10
* orders    record_per_day * days_in_config


