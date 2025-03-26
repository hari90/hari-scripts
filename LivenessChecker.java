import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class LivenessChecker {
    private static final String  CREATE_TABLE = "CREATE TABLE IF NOT EXISTS test_table (data TEXT)";
    private static final String INSERT_SQL = "INSERT INTO test_table (data) VALUES (?)";
    private static final int RECONNECT_DELAY_MS = 1000;
    
    private final String conn;
    private final int port;
    private final String db;
    private final String user;
    private final String password;
    private final int threadCount;
    private final int tps;
    private final long runTimeMillis;
    private final long endTime;
    private final AtomicInteger transactionCounter = new AtomicInteger(0);
    private final String logFile;

    public LivenessChecker(String conn, int port, String db, String user, String password,
        int threadCount, int tps, long runTimeMin) {
      String params = String.format("conn: %s%nport: %d%ndb: %s%nuser: %s%npassword: "
                                    + "%s%nthreadCount: %d%ntps: %d%nrunTimeMin: %d%n",
          conn, port, db, user, password, threadCount, tps, runTimeMin);
      System.out.println(params);


      this.conn = conn;
      this.port = port;
      this.db = db;
      this.user = user;
      this.password = password;
      this.threadCount = threadCount;
      this.tps = tps;
      this.runTimeMillis = runTimeMin * 60 * 1000;
      endTime = System.currentTimeMillis() + runTimeMillis;
    logFile = "logs/tps_log_"
        + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss")) + ".txt";

      try {
        java.nio.file.Files.createDirectories(java.nio.file.Paths.get("logs"));
        try (FileWriter writer = new FileWriter(logFile, false)) {
          writer.write(params);
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    public void start() throws Exception {
        Class.forName("org.postgresql.Driver");

        try (Connection conn = getConn();
             PreparedStatement stmt = conn.prepareStatement(CREATE_TABLE)) {
            stmt.executeUpdate();
        }

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        ScheduledExecutorService tpsLogger = Executors.newSingleThreadScheduledExecutor();
        
        // Start TPS logging every second
        tpsLogger.scheduleAtFixedRate(this::logTps, 1, 1, TimeUnit.SECONDS);
        
        
        for (int i = 0; i < threadCount; i++) {
          executor.submit(() -> { InsertWorker(); });
        }
        
        executor.shutdown();
        try {
            executor.awaitTermination(runTimeMillis + 5000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        tpsLogger.shutdown();
    }

    private boolean keepRunning() {
        return System.currentTimeMillis() < endTime;
    }
    
    private Connection getConn() throws SQLException {
      return DriverManager.getConnection(
          String.format("jdbc:postgresql://%s:%s/%s", conn, port, db), user, password);
    }
    
    private void handleConnFailure() {
        while (keepRunning()) {
            try {
                Thread.sleep(RECONNECT_DELAY_MS);
                getConn().close();
                return;
            } catch (Exception e) {
                // Retry
            }
        }
    }

    private void InsertWorker() {
        Connection conn = null;
        while (keepRunning()) {
          try {
            if (conn == null || conn.isClosed()) {
              conn = getConn();
            }

            PreparedStatement stmt = conn.prepareStatement(INSERT_SQL);
            stmt.setString(
                1, LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));

            long startTime = System.currentTimeMillis();
            stmt.executeUpdate();
            long duration = System.currentTimeMillis() - startTime;

            transactionCounter.incrementAndGet();

            long tps_per_thread = tps / threadCount;
            long sleepTime = Math.max(0, (1000 / tps_per_thread - duration));

            Thread.sleep(sleepTime);

          } catch (Exception e) {
            conn = null;

            System.out.println("Error during database operation: " + e.getMessage());
            handleConnFailure();
          }
        }
    }

    private void logTps() {
        int tpsValue = transactionCounter.getAndSet(0);
        try (FileWriter writer = new FileWriter(logFile, true)) {
            String logEntry = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")) + 
                             ", " + tpsValue + "\n";
            writer.write(logEntry);
            System.out.println(logEntry);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    public static void main(String[] args) {
        if (args.length != 8) {
            System.out.println("Usage: java -cp .:postgresql-42.7.5.jar LivenessChecker <conn> <port> <db> <user> <password> <threads> <tps> <run_time_min>");
            return;
        }
        
        String conn = args[0];
        int port = Integer.parseInt(args[1]);
        String db = args[2];
        String user = args[3];
        String password = args[4];
        int threadCount = Integer.parseInt(args[5]);
        int tps = Integer.parseInt(args[6]);
        long runTimeMin = Long.parseLong(args[7]);
        
        LivenessChecker tester = new LivenessChecker(conn, port, db, user, password, threadCount, tps, runTimeMin);

        try {
          tester.start();
        } catch (Exception e) {
          e.printStackTrace();
        }
    }
}
