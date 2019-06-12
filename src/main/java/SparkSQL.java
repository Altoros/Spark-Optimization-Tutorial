import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSQL {
    public static void main(String[] args) {
        SparkSession ss = SparkSession.builder()
                .appName("Spark-SQL")
                //.master("") path to Master (Spark Standalone)
                .master("local[*]")
                .config("spark.eventLog.enabled", "true")
                .config("spark.eventLog.dir", "")
                .getOrCreate();

        Dataset<Row> comments = ss.read()
                .option("header", true)
                .option("mode", "DROPMALFORMED")
                .schema(SparkCore.Comment.commentStructType())
                .csv("comments.csv")
                .filter(SparkCore.Comment::isNotNullValue);

        Dataset<Row> users = ss.read()
                .option("header", true)
                .option("mode", "DROPMALFORMED")
                .schema(SparkCore.User.userStructType())
                .csv("users.csv")
                .filter(SparkCore.User::isNotNullValue);

        Dataset<Row> posts = ss.read()
                .option("header", true)
                .option("mode", "DROPMALFORMED")
                .schema(SparkCore.Post.postStructType())
                .csv("posts.csv")
                .filter(SparkCore.Post::isNotNullValue);

        users.createOrReplaceTempView("users");
        comments.createOrReplaceTempView("comments");
        posts.createOrReplaceTempView("posts");

        ss.sqlContext().sql("SELECT u.id, count(distinct c.id) FROM users AS u " +
                "INNER JOIN comments AS c ON u.id = c.user_id " +
                "INNER JOIN posts AS p ON p.owner_user_id = u.id " +
                "WHERE u.reputation > 1 AND c.post_id = p.id GROUP BY u.id ORDER BY count(distinct c.id) DESC").show();

    }
}
