
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.broadcast;
import static org.apache.spark.sql.functions.col;


public class SparkDataFrameOptimize {
    public static void main(String[] args) {
        SparkSession ss = SparkSession.builder()
                .appName("Spark-DataFrame")
                //.master("") path to Master (Spark Standalone)
                .master("local[*]")
                .config("spark.eventLog.enabled", "true")
                .config("spark.eventLog.dir", "")
                .config("spark.sql.shuffle.partitions", "25")
                .getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(ss.sparkContext());

        Dataset<Row> comments = ss.read()
                .option("header", true)
                .option("mode", "DROPMALFORMED")
                .schema(SparkCore.Comment.commentStructType())
                .csv("comments.csv")
                .select(col("id"), col("user_id"), col("post_id"), col("score"))
                .filter(SparkCore.Comment::isNotNullValue);

        Dataset<Row> users = ss.read()
                .option("header", true)
                .option("mode", "DROPMALFORMED")
                .schema(SparkCore.User.userStructType())
                .csv("users.csv")
                .select(col("id"), col("reputation"))
                .filter(SparkCore.User::isNotNullValue);

        Dataset<Row> posts = ss.read()
                .option("header", true)
                .option("mode", "DROPMALFORMED")
                .schema(SparkCore.Post.postStructType())
                .csv("posts.csv")
                .select(col("id"), col("owner_user_id"), col("score"))
                .filter(SparkCore.Post::isNotNullValue);

        Broadcast<Integer> filterValue = sc.broadcast(1);
        Dataset<Row> usersWGR = users.filter(users.col("reputation").$greater(filterValue.getValue()));
        broadcast(usersWGR)
                .join(comments, users.col("id").equalTo(comments.col("user_id")))
                .join(posts, users.col("id").equalTo(posts.col("owner_user_id")))
                .filter(posts.col("id").equalTo(comments.col("post_id")))
                .distinct()
                .groupBy(users.col("id"))
                .count().orderBy(col("count").desc()).show();

    }
}
