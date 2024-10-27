import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;
import java.io.Serializable;
import java.util.List;

public class LogAnalysis {

    public static class LogEntry implements Serializable {
        private String timestamp;
        private String logLevel;
        private String userId;
        private String action;
        private String details;

        public LogEntry() {}

        public LogEntry(String timestamp, String logLevel, String userId, String action, String details) {
            this.timestamp = timestamp;
            this.logLevel = logLevel;
            this.userId = userId;
            this.action = action;
            this.details = details;
        }

        public String getTimestamp() { return timestamp; }
        public String getLogLevel() { return logLevel; }
        public String getUserId() { return userId; }
        public String getAction() { return action; }
        public String getDetails() { return details; }
    }

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Log Analysis").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        // Read log file
        JavaRDD<String> logLines = sc.textFile("/app/logfile.txt"); // Change for your path

        // LogEntry Object
        JavaRDD<LogEntry> logEntryRDD = logLines.map(line -> {
            String[] parts = line.split(",");
            String timestamp = parts[0]; // Mantenha como String
            String logLevel = parts[1];
            String userId = parts[2];
            String action = parts[3];
            String details = parts[4];
            return new LogEntry(timestamp, logLevel, userId, action, details);
        });

        List<LogEntry> logEntryList = logEntryRDD.collect();
        Dataset<Row> logEntryDF = spark.createDataFrame(logEntryList, LogEntry.class);

        // Repartition by userId
        Dataset<Row> partitionedDF = logEntryDF.repartition(col("userId"));

        // Store in cache
        partitionedDF.cache();

        // Temporary View
        partitionedDF.createOrReplaceTempView("logs");

        // Hourly Aggregation
        Dataset<Row> hourlyAggregation = partitionedDF
            .withColumn("hour", date_format(to_timestamp(col("timestamp")), "yyyy-MM-dd HH:00:00"))
            .groupBy("hour")
            .count()
            .orderBy("hour");

        System.out.println("Hourly Aggregation:");
        hourlyAggregation.show();

        // Daily Aggregation
        Dataset<Row> dailyAggregation = partitionedDF
            .withColumn("day", date_format(to_timestamp(col("timestamp")), "yyyy-MM-dd"))
            .groupBy("day")
            .count()
            .orderBy("day");

        System.out.println("Daily Aggregation:");
        dailyAggregation.show();

        // Total number of actions and categories
        Dataset<Row> userActionCount = partitionedDF
            .groupBy("userId", "action")
            .agg(count("action").alias("action_count"))
            .orderBy("userId", "action");

        System.out.println("Total number of actions and categories:");
        userActionCount.show();

        // Filter error messages
        Dataset<Row> errorMessages = partitionedDF.filter(col("logLevel").equalTo("ERROR"));

        // Identify the 5 most common error messages
        Dataset<Row> frequentErrors = errorMessages
            .groupBy("details")
            .agg(count("*").alias("error_count"))
            .orderBy(desc("error_count"))
            .limit(5);

        System.out.println("5 most common ERROR messages:");
        frequentErrors.show();

        // Calculate the average time between consecutive occurrences of each error
        Dataset<Row> errorTimeGap = errorMessages
            .withColumn("timestamp", to_timestamp(col("timestamp")))
            .orderBy("details", "timestamp")
            .withColumn("prev_timestamp", lag(col("timestamp"), 1).over(Window.partitionBy("details").orderBy("timestamp")))
            .withColumn("time_diff", unix_timestamp(col("timestamp")).minus(unix_timestamp(col("prev_timestamp"))))
            .groupBy("details")
            .agg(
                avg("time_diff").alias("average_time_difference")
            );

        System.out.println("Average time between consecutive occurrences of error messages:");
        errorTimeGap.show();

        // User sessions
        Dataset<Row> sessionData = partitionedDF
            .withColumn("timestamp", to_timestamp(col("timestamp")))
            .orderBy("userId", "timestamp")
            .withColumn("prev_timestamp", lag("timestamp", 1).over(Window.partitionBy("userId").orderBy("timestamp")))
            .withColumn("time_diff", (unix_timestamp(col("timestamp")).minus(unix_timestamp(col("prev_timestamp")))))
            .withColumn("new_session", when(col("time_diff").gt(30 * 60), 1).otherwise(0))
            .withColumn("session_id", sum("new_session").over(Window.partitionBy("userId").orderBy("timestamp").rowsBetween(Window.unboundedPreceding(), Window.currentRow())));

        // Calculate session duration and session count per user
        Dataset<Row> sessionSummary = sessionData
            .groupBy("userId", "session_id")
            .agg(
                min("timestamp").alias("session_start"),
                max("timestamp").alias("session_end"),
                (unix_timestamp(max("timestamp")).minus(unix_timestamp(min("timestamp")))).alias("session_duration")
            )
            .groupBy("userId")
            .agg(
                count("session_id").alias("total_sessions"),
                avg("session_duration").alias("average_session_duration")
            );

        System.out.println("Total de sessões e duração média por usuário:");
        sessionSummary.show();

        sc.stop();
        spark.stop();
    }
}
