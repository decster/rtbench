package com.dorisdb.rtbench;

import com.dorisdb.rtbench.schema.Column;
import com.dorisdb.rtbench.schema.Schema;
import com.typesafe.config.Config;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.dorisdb.rtbench.schema.Columns.BIGINT;
import static com.dorisdb.rtbench.schema.Columns.IDLONG;

public class RowStorePoc {
    static Logger LOG = LogManager.getLogger(RowStorePoc.class);
    Config config;
    boolean dryRun = false;
    long randSeed = 1;
    String jdbcUrl = "jdbc:mysql://127.0.0.1:9030/?autoReconnect=true&useSSL=false";
    String username = "root";
    String password = "";
    String streamLoadUrl = "127.0.0.1:8030";
    BasicDataSource dataSource;

    int numWorker = 1;
    ExecutorService workers;
    LinkedBlockingQueue<Throwable> errorQueue = new LinkedBlockingQueue<>();

    String dbName = "rtm";
    int numTable = 1;

    int tableReplicationMin = 1;
    int tableReplicationMax = 1;
    long tableIdStart = 0;
    int tableBucketMin = 1;
    int tableBucketMax = 1;
    boolean persistentIndex = false;

    Table[] tables;

    //stats
    static class Stats {
        long numLoad;
        long numOp;
        long numBytes;
        long runTime;
        long sendTime;
        long lastReportTs = System.nanoTime();

        static class TableStats {
            long numOp;
            long numLoad;
            long loadTime;

            void add(long op, long loadTime) {
                numLoad++;
                this.numOp += op;
                this.loadTime += loadTime;
            }
        }

        Map<String, TableStats> tableLoadStats = new TreeMap<>();

        synchronized void add(String tableName, long op, long bytes, long runTime, long sendTime) {
            numLoad++;
            numOp += op;
            numBytes += bytes;
            this.runTime += runTime;
            this.sendTime += sendTime;
            tableLoadStats.computeIfAbsent(tableName, k -> new TableStats()).add(op, sendTime);
        }

        void report() {
            long now = System.nanoTime();
            if (lastReportTs + 60000000000L < now) {
                long cLoad, cOp, cBytes, cRunTime, cSendTime;
                StringBuilder tableSB = new StringBuilder("table ");
                StringBuilder stxnSB = new StringBuilder("s/txn ");
                StringBuilder opsSB = new StringBuilder("ops   ");
                synchronized (this) {
                    cLoad = numLoad;
                    cOp = numOp;
                    cBytes = numBytes;
                    cRunTime = runTime;
                    cSendTime = sendTime;
                    numLoad = 0;
                    numOp = 0;
                    numBytes = 0;
                    runTime = 0;
                    sendTime = 0;
                    for (Map.Entry<String, TableStats> e : tableLoadStats.entrySet()) {
                        TableStats s = e.getValue();
                        tableSB.append(String.format("%10s", e.getKey()));
                        stxnSB.append(String.format("%10.1f", s.loadTime / 1000000000.0 / s.numLoad));
                        opsSB.append(String.format("%10d", s.numOp / s.numLoad));
                    }
                }
                double time = (now - lastReportTs) / 1000000000.0;
                LOG.info(String.format(
                        "txn/s: %.1f op/s: %.0f, MB/s: %.1fM, time: %.1fs, runTime: %.1fs, sendTime: %.1fs\n\t%s\n\t%s\n\t%s",
                        cLoad / time,
                        cOp / time,
                        cBytes / time / 1024 / 1024,
                        time,
                        cRunTime / 1000000000.0,
                        cSendTime / 1000000000.0,
                        tableSB.toString(),
                        stxnSB.toString(),
                        opsSB.toString()));
                lastReportTs = now;
            }
        }
    }

    Stats stats = new Stats();

    class Table {
        long seed;
        Random rand;
        int id;
        String tableName;
        String type;
        String storeType;
        Schema schema;

        AtomicInteger runningTask = new AtomicInteger(0);

        int nColumn;
        long totalRows;
        long totalRowsLoaded;
        int nUpdateColumn;
        int numUpdatePerLoad;
        boolean partialUpdatephrase;

        int replication;
        int numBucket;
        long loadPeriod;
        long nextLoadTime;
        long opPerLoad;
        AtomicLong idAllocator;

        Table(long seed, int id, String type) throws Exception {
            this.seed = seed;
            this.rand = new Random(seed);
            this.id = id;
            this.tableName = "table" + id;
            this.type = type;
            this.storeType = config.getString("store_type");
            this.partialUpdatephrase = config.getString("phrase").equals("update");
            nColumn = config.getInt("column.num");
            totalRows = config.getInt("row.num");
            nUpdateColumn = config.getInt("update.column.num");
            numUpdatePerLoad = config.getInt("update.per.load");
            Column[] columns = new Column[nColumn];
            columns[0] = IDLONG("id");
            for (int i = 1; i < nColumn; i++) {
                columns[i] = BIGINT("c" + i, i * 1000000L, (i + 1) * 1000000L);
            }
            this.schema = new Schema(columns);
            replication = rand.nextInt(tableReplicationMax - tableReplicationMin + 1) + tableReplicationMin;
            numBucket = (int) (rand.nextInt(tableBucketMax - tableBucketMin + 1) + tableBucketMin);
            idAllocator = new AtomicLong(tableIdStart);
            loadPeriod = config.getDuration("load.period", TimeUnit.SECONDS);
            opPerLoad = config.getLong("op.per.load");
            nextLoadTime = loadPeriod;
        }

        void streamload(long ts) throws Exception {
            long startTs = System.nanoTime();
            String label = "load-" + tableName + "-" + ts;
            ByteArrayStreamLoad load;
            long num;
            if (partialUpdatephrase) {
                load = new ByteArrayStreamLoad(config, dbName, tableName, label, nUpdateColumn);
                num = numUpdatePerLoad;
                DataOperation op = new DataOperation();
                Random rand = new Random(seed + ts);
                for (long i = 0; i < num; i++) {
                    schema.genPartialUpdateOp(rand.nextLong() % totalRows, rand.nextLong(), ts, op, nUpdateColumn);
                    load.addData(op);
                }
            } else {
                load = new ByteArrayStreamLoad(config, dbName, tableName, label);
                num = opPerLoad;
                DataOperation op = new DataOperation();
                long idStart = idAllocator.getAndAdd(num);
                Random rand = new Random(seed + ts);
                for (long i = 0; i < num; i++) {
                    schema.genOp(idStart + i, rand.nextLong(), ts, op);
                    load.addData(op);
                }
            }
            long startSendTs = System.nanoTime();
            load.send();
            long endTs = System.nanoTime();
            stats.add(tableName, num, load.getFileSize(), endTs - startTs, endTs - startSendTs);
            String fileSize = FileUtils.byteCountToDisplaySize(load.getFileSize());
            LOG.info(String.format("load table %s at %d period %d txnId: %d op: %d size: %s time: send: %.1fs total %.1fs%s",
                    tableName, ts, loadPeriod, load.getTxnId(), num, fileSize,
                    (endTs - startSendTs) / 1000000000.0, (endTs - startTs) / 1000000000.0,
                    partialUpdatephrase ? " partialUpdate" : ""));
            if (!partialUpdatephrase) {
                totalRowsLoaded += num;
                if (totalRowsLoaded >= totalRows) {
                    throw new Exception("all rows loaded");
                }
            }
        }

        Runnable getTaskForTime(final long ts) throws Exception {
            if (ts == 0) {
                if (config.getString("phrase").equals("load")) {
                    runningTask.incrementAndGet();
                    return new Runnable() {
                        @Override
                        public void run() {
                            try {
                                if (config.getBoolean("cleanup")) {
                                    runSql(dbName, "drop table if exists " + tableName + " force", true);
                                }
                                String createSql =
                                        schema.getCreateTable(tableName, numBucket, replication, persistentIndex, storeType);
                                runSql(dbName, createSql, true);
                            } catch (Throwable e) {
                                LOG.warn("run create table " + tableName + " failed", e);
                                errorQueue.add(e);
                            }
                            runningTask.decrementAndGet();
                        }
                    };
                }
                return null;
            } else if (ts >= nextLoadTime) {
                runningTask.incrementAndGet();
                nextLoadTime += loadPeriod;
                return new Runnable() {
                    @Override
                    public void run() {
                        try {
                            streamload(ts);
                        } catch (Throwable e) {
                            LOG.warn(String.format("load table %s at %d period %d failed", tableName, ts, loadPeriod), e);
                            errorQueue.add(e);
                        }
                        runningTask.decrementAndGet();
                    }
                };
            } else {
                return null;
            }
        }
    }

    private void buildDataSource() {
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setUrl(jdbcUrl);
        dataSource.setUsername(username);
        dataSource.setPassword(password);
        dataSource.setMaxTotal(40);
        dataSource.setMaxIdle(40);
        this.dataSource = dataSource;
    }

    public Connection getQueryConnection() throws SQLException {
        return dataSource.getConnection();
    }

    private static void runSingleSql(Statement stmt, String sql, boolean verbose) throws SQLException {
        while (true) {
            try {
                long start = System.nanoTime();
                stmt.execute(sql);
                if (verbose) {
                    long end = System.nanoTime();
                    System.out.printf("runSql(%.3fs): %s\n", (end - start) / 1e9, sql);
                }
                break;
            } catch (SQLSyntaxErrorException e) {
                if (e.getMessage().startsWith("rpc failed, host")) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ie) {
                    }
                    System.out.println("retry execute " + sql);
                    continue;
                }
                throw e;
            }
        }
    }

    public void runSql(String db, String sql, boolean verbose) throws SQLException {
        if (dryRun) {
            LOG.info("run sql: " + sql);
            return;
        }
        Connection connection = getQueryConnection();
        Statement stmt = connection.createStatement();
        try {
            if (db != null) {
                stmt.execute("use " + db);
            }
            runSingleSql(stmt, sql, verbose);
        } finally {
            stmt.close();
            connection.close();
        }
    }

    public void runSql(String db, String sql) throws SQLException {
        runSql(db, sql, false);
    }

    public void runSqlList(String db, List<String> sqls, boolean verbose) throws SQLException {
        if (dryRun) {
            for (String sql : sqls) {
                LOG.info("run sql: " + sql);
            }
            return;
        }
        Connection connection = getQueryConnection();
        Statement stmt = connection.createStatement();
        try {
            if (db != null) {
                stmt.execute("use " + db);
            }
            for (String sql : sqls) {
                runSingleSql(stmt, sql, verbose);
            }
        } finally {
            stmt.close();
            connection.close();
        }
    }

    public void runSqls(String db, String... sqls) throws SQLException {
        runSqlList(db, Arrays.stream(sqls).collect(Collectors.toList()), true);
    }

    public RowStorePoc(Config config) throws Exception {
        this.config = config;
        this.dryRun = config.getBoolean("dry_run");
        this.jdbcUrl = config.getString("jdbc.url");
        this.username = config.getString("db.user");
        this.password = config.getString("db.password");
        this.streamLoadUrl = config.getString("stream_load.addr");
        this.numWorker = config.getInt("worker.num");
        this.dbName = config.getString("db.name");
        this.numTable = config.getInt("table.num");
        this.randSeed = config.getLong("rand.seed");
        tableReplicationMin = config.getInt("table.replication.min");
        tableReplicationMax = config.getInt("table.replication.max");
        tableIdStart = config.getLong("table.id.start");
        tableBucketMin = (int) config.getLong("table.bucket.min");
        tableBucketMax = (int) config.getLong("table.bucket.max");
        persistentIndex = config.getBoolean("table.persistent_index");
        buildDataSource();
        ThreadFactory factory = new ThreadFactory() {
            AtomicInteger id = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                Thread ret = new Thread(r, "worker-" + id.incrementAndGet());
                ret.setDaemon(true);
                return ret;
            }
        };
        if (numWorker < 1) {
            workers = Executors.newCachedThreadPool(factory);
        } else {
            workers = Executors.newFixedThreadPool(numWorker, factory);
        }
        buildTables();
    }

    private void buildTables() throws Exception {
        tables = new Table[numTable];
        String type = config.getString("table.type");
        for (int i = 0; i < numTable; i++) {
            tables[i] = new Table(randSeed * (i + 1), i, type);
        }
    }

    private static void waitTo(long ts) throws InterruptedException {
        long cur = System.nanoTime();
        long wait = ts - cur;
        if (wait > 0) {
            Thread.sleep(wait / 1000000, (int) (wait % 1000000L));
        }
    }

    public void run() throws Throwable {
        if (config.getString("phrase").equals("load")) {
            if (config.getBoolean("cleanup")) {
                runSql(null, "drop database if exists " + dbName + " force", true);
            }
            runSql(null, "create database if not exists " + dbName, true);
        }
        long runStart = config.getDuration("run.start", TimeUnit.SECONDS);
        long runEnd = config.getDuration("run.end", TimeUnit.SECONDS);
        long tsStart = System.nanoTime();
        for (long i = runStart; i < runEnd; i++) {
            waitTo(tsStart + (i - runStart) * 1000000000L);
            if (errorQueue.size() > 0) {
                throw errorQueue.poll();
            }
            for (Table table : tables) {
                Runnable task = table.getTaskForTime(i);
                if (task != null) {
                    workers.submit(task);
                }
            }
            stats.report();
        }
    }
}
