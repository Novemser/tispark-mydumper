package com.pingcap;

import org.apache.commons.cli.Options;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class Transformer {
  private static final Object CTX_LOCK = new Object();
  private static Map<String, int[]> tableTimestampMap = new HashMap<>();

  static class Context {
    enum Status {
      INITIAL,
      READING,
      FINISHED
    }

    private final String inputFileName;
    private String currentWriteFile;
    private AtomicInteger writeFileIdx;
    private final long MAX_BYTES_PER_FILE;
    private volatile AtomicLong bytesWrite;
    private volatile BlockingQueue<String[]> dataQueue;
    private volatile Status status;
    private String dbName;
    private List<Integer> literalNullCols;

    Context(String inputFileName, long max_bytes_per_file, BlockingQueue<String[]> queue, Status status, String dbName) {
      this.inputFileName = inputFileName;
      MAX_BYTES_PER_FILE = max_bytes_per_file;
      this.dataQueue = queue;
      this.status = status;
      writeFileIdx = new AtomicInteger(1);
      bytesWrite = new AtomicLong(0);
      this.dbName = dbName;
    }

    void setCurrentWriteFile(String currentWriteFile) {
      this.currentWriteFile = currentWriteFile;
    }

    void putData(String[] data) throws InterruptedException {
      dataQueue.put(data);
    }

    BlockingQueue<String[]> getDataQueue() {
      return dataQueue;
    }

    Status getStatus() {
      return status;
    }

    void setStatus(Status status) {
      this.status = status;
    }

    String getDBName() {
      return dbName;
    }

    public void setDbName(String dbName) {
      this.dbName = dbName;
    }

    String getTableName() {
      return inputFileName.toUpperCase();
    }

    String nextFileName() {
      return String.format("%s.%s.%09d.sql",
          getDBName(),
          inputFileName.toUpperCase(),
          writeFileIdx.getAndIncrement());
    }

    boolean isEmpty() {
      return dataQueue.isEmpty() && status == Status.FINISHED;
    }

    boolean needProceedNextFile() {
      boolean needProceed = bytesWrite.get() >= MAX_BYTES_PER_FILE;
      if (needProceed) {
        bytesWrite.set(0);
      }
      return needProceed;
    }

    void incBytesWrite(long val) {
      bytesWrite.addAndGet(val);
    }
  }

  public String OUTPUT_DIR = "";
  public int MAX_ROWS_COUNT = 10000;
  public static final long MB = 1024 * 1024;
  public long MAX_BYTES_PER_FILE = 100 * MB; // Default to 100MB
  private int numReaders;
  private int numWriters;

  private Collection<String> tables;
  private SparkSession spark;
  private Map<String, Context> contextMap = new ConcurrentHashMap<>();
  private BlockingQueue<Context> readingCtxQueue = new LinkedBlockingDeque<>();
  private CountDownLatch writerLatch;

  private ExecutorService readers;
  private ExecutorService writers;

  private Options options = new Options();
  private Date startTime;
  public String dbName = "tpch";

  public Transformer(SparkSession spark) {
    this.spark = spark;
    initOptions();
  }

  public void setTables(String[] tables) {
    this.tables = new ArrayList<>();
    this.tables.addAll(Arrays.asList(tables));
  }

  public void run(int reader, int writer) {
    verifyData();
    initReaderWriter(reader, writer);
    transferToSQLFiles();
  }

  private void initOptions() {
    options.addOption("help", "Print this help");
    options.addOption("outputDir", true, "Directory where the transformed sql files will be placed in.");
    options.addOption("rowCount", true, "How many rows per `INSERT INTO` statement.");
    options.addOption("chunkFileSize", true, "Split tables into chunks of this output file size. This value is in MB.");
    options.addOption("dbName", true, "Database name:tpch/tpch_idx");
    options.addOption("tables", true, "Tables in dbName to convert, separated by `,`. e.g. \"t1,t2,t3\"");
  }

  public Options getOptions() {
    return options;
  }

  public void initReaderWriter(int reader, int writer) {
    readers = Executors.newFixedThreadPool(reader);
    writers = Executors.newFixedThreadPool(writer);
    numReaders = reader;
    numWriters = writer;
  }

  private void verifyData() {
    if (tables == null) {
      throw new IllegalStateException("Data has not been loaded properly");
    } else {
      tables.forEach(System.out::println);
      writerLatch = new CountDownLatch(tables.size());
      System.out.println("Successfully detected " + tables.size() + " tables.");
      System.out.println("------------------------------------------------");

      tables.forEach(name -> contextMap
          .putIfAbsent(
              name,
              new Context(
                  name,
                  MAX_BYTES_PER_FILE,
                  new LinkedBlockingQueue<>(Math.min(MAX_ROWS_COUNT, 100000)),
                  Context.Status.INITIAL,
                  dbName
              )
          ));
    }
  }

  private void transferToSQLFiles() {
    startTime = new Date();
    startReader();
    startWriter();
    close();
  }

  private void close() {
    readers.shutdown();
    writers.shutdown();
    try {
      while (!readers.awaitTermination(1, TimeUnit.SECONDS) ||
          !writers.awaitTermination(1, TimeUnit.SECONDS)) {
      }
      writeMetaData();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void writeMetaData() {
    try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(OUTPUT_DIR + "metadata"))) {
      SimpleDateFormat format = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");
      String startStr = "Started dump at: " + format.format(startTime);
      String endStr = "Finished dump at: " + format.format(new Date());
      System.out.println("Writing metadata:");
      System.out.println(startStr);
      System.out.println(endStr);
      writer.write(startStr);
      writer.newLine();
      writer.write(endStr);
      writer.newLine();
      writer.flush();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void startReader() {
    // TODO
    tables.forEach(table -> readers.submit(()->{
      Dataset<Row> df = spark.sql("select * from " + table);
      Iterator<Row> localIter = df.toLocalIterator();
      long readCnt = 0;
      Context ctx = contextMap.get(table);
      Objects.requireNonNull(ctx);
      try {
        ctx.setStatus(Context.Status.READING);
        readingCtxQueue.add(ctx);
        long start = System.currentTimeMillis();
        while (localIter.hasNext()) {
          Row row = localIter.next();
          readCnt++;
          String[] data = new String[row.length()];
          for (int i = 0; i < data.length; i++) {
            data[i] = row.get(i).toString();
          }
          ctx.putData(data);
        }
        System.out.println("Finished reading " + readCnt +
            " lines from " + table + " using time(ms):" + (System.currentTimeMillis() - start));
      } catch (Exception e) {
        e.printStackTrace();
        exit("Reading table " + table + " failed due to " + e.getMessage(), 2);
      } finally {
        ctx.setStatus(Context.Status.FINISHED);
      }
    }));
  }

  private void exit(String msg, int status) {
    System.err.println("Internal error:" + msg + ", exiting.");
    System.exit(status);
  }

  private Context nextCtx2Write() {
    synchronized (CTX_LOCK) {
      if (writerLatch.getCount() > 0) {
        writerLatch.countDown();
        try {
          return readingCtxQueue.take();
        } catch (InterruptedException e) {
          e.printStackTrace();
          exit(e.getMessage(), 1);
        }
      }
      return null;
    }
  }

  private void startWriter() {
    for (int num = 0; num < numWriters; num++) {
      writers.submit(() -> {
        Context ctx;
        while ((ctx = nextCtx2Write()) != null) {
          while (!ctx.isEmpty()) {
            try (BufferedWriter writer = getFileBufferedWriter(ctx)) {
              writer.write("/*!40101 SET NAMES binary*/;\n" +
                  "/*!40014 SET FOREIGN_KEY_CHECKS=0*/;\n" +
                  "/*!40103 SET TIME_ZONE='+00:00' */;\n");

              while (true) {
                if (ctx.isEmpty() || ctx.needProceedNextFile()) {
                  break;
                }
                String result = getWriteContext(ctx, writer);
                if (result == null) continue;

                ctx.incBytesWrite(result.length());
                writer.write(result);
                writer.newLine();
                writer.flush();
              }
              writer.flush();
              System.out.println("Finished writing file " + ctx.currentWriteFile);
            } catch (Exception e) {
              e.printStackTrace();
              exit("Convert " + ctx.currentWriteFile + " failed due to " + e.getMessage(), 3);
            }
          }
          System.out.println("Successfully processed " + ctx.getTableName());
        }
      });
    }
  }

  private String getWriteContext(Context ctx, BufferedWriter writer) throws IOException, InterruptedException {
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < MAX_ROWS_COUNT; i++) {
      // Check whether data queue is pending to finish.
      while (ctx.getDataQueue().isEmpty()) {
        if (ctx.status == Context.Status.FINISHED) {
          if (builder.length() > 0) {
            String res = builder.toString();
            if (res.endsWith(",")) {
              // Replace the last
              return res.substring(0, res.length() - 1) + ";";
            } else {
              // Should never reach here
              throw new RuntimeException("Constructed insert [" + res + "] stmt is corrupted due to unknown reason");
            }
          } else {
            return null;
          }
        }
      }
      if (i == 0) {
        writer.write("INSERT INTO `" + ctx.getTableName() + "` VALUES\n");
      }
      // append insert values
      String[] data = ctx.getDataQueue().take();
      String[] rewriteData = new String[data.length];
      if (ctx.literalNullCols != null) {
        for (int j = 0; j < data.length; j++) {
          if (ctx.literalNullCols.contains(j)) {
            if (data[j].equalsIgnoreCase("null")) {
              // we do not add quota when data is literal null
              rewriteData[j] = data[j];
            } else {
              rewriteData[j] = "\"" + data[j] + "\"";
            }
          } else {
            rewriteData[j] = "\"" + data[j] + "\"";
          }
        }
        builder
            .append("(")
            .append(String.join(",", rewriteData))
            .append(")");
      } else {
        builder
            .append("(\"")
            .append(String.join("\",\"", data))
            .append("\")");
      }

      if (i == MAX_ROWS_COUNT - 1 || ctx.isEmpty()) {
        builder.append(";");
        break;
      } else {
        builder.append(",");
      }
    }
    if (builder.length() <= 0) {
      return null;
    }
    return builder.toString();
  }

  private BufferedWriter getFileBufferedWriter(Context context)
      throws IOException {
    String nextFile = context.nextFileName();
    context.setCurrentWriteFile(nextFile);
    Path path = Paths.get(OUTPUT_DIR + nextFile);
    return Files.newBufferedWriter(path);
  }
}
