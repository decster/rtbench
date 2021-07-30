#!/bin/bash

java -Xmx2400m -cp conf:target/rtbench-0.0.1-SNAPSHOT-jar-with-dependencies.jar com.dorisdb.rtbench.MysqlTableSync
