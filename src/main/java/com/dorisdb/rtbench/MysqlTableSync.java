package com.dorisdb.rtbench;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;

public class MysqlTableSync {
    private static final Logger LOG = LogManager.getLogger(MysqlTableSync.class);

    public static void main(String [] args) throws Exception {
        BinaryLogClient client = new BinaryLogClient("localhost", 3306, "canal", "canal");
        EventDeserializer eventDeserializer = new EventDeserializer();
        eventDeserializer.setCompatibilityMode(
            EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG,
            EventDeserializer.CompatibilityMode.CHAR_AND_BINARY_AS_BYTE_ARRAY
        );
        client.setEventDeserializer(eventDeserializer);
        client.registerEventListener(new BinaryLogClient.EventListener() {
            @Override
            public void onEvent(Event event) {
                LOG.info(event.toString());
            }
        });
        client.connect();
    }
}
