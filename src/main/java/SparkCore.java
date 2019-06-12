import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Objects;

public class SparkCore {
    public static void main(String[] args) {
        SparkSession ss = SparkSession.builder()
                .appName("Spark-Core")
                //.master("") path to Master (Spark Standalone)
                .master("local[*]")
                .config("spark.eventLog.enabled", "true")
                .config("spark.eventLog.dir", "")
                .getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(ss.sparkContext());

        JavaRDD<Comment> comments = ss.read()
                .option("header", true)
                .option("mode", "DROPMALFORMED")
                .schema(Comment.commentStructType())
                .csv("/Users/ayudovin/job/sparkoptmizationtutorial/src/main/resources/comments1.csv")
                //.csv("gs://stack-overflow/comments/")
                .filter(Comment::isNotNullValue)
                .javaRDD()
                .map(Comment::toComment);

        JavaRDD<Post> posts = ss.read()
                .option("header", true)
                .option("mode", "DROPMALFORMED")
                .schema(Post.postStructType())
                .csv("/Users/ayudovin/job/sparkoptmizationtutorial/src/main/resources/posts1.csv")
                //.csv("gs://stack-overflow/posts/")
                .filter(Post::isNotNullValue)
                .javaRDD()
                .map(Post::toPost);

        JavaRDD<User> users = ss.read()
                .option("header", true)
                .option("mode", "DROPMALFORMED")
                .schema(User.userStructType())
                .csv("/Users/ayudovin/job/sparkoptmizationtutorial/src/main/resources/users1.csv")
                //.csv("gs://stack-overflow/users/")
                .filter(User::isNotNullValue)
                .javaRDD()
                .map(User::toUser);

        JavaPairRDD<Long, User> usersWithGoodReputation = users
                .filter(user -> user.getReputation() > 1)
                .mapToPair(user -> new Tuple2<>(user.getId(), user));

        JavaPairRDD<Long, Comment> commentByUserID = comments
                .mapToPair(c -> new Tuple2<>(c.getUserId(), c));

        JavaPairRDD<Long, Post> postByUserId = posts
                .mapToPair(p -> new Tuple2<>(p.getOwnerUserId(), p));

        JavaPairRDD<Long, Comment> commentsByUsersWGRId = usersWithGoodReputation.join(commentByUserID)
                .mapValues(Tuple2::_2);

        commentsByUsersWGRId.join(postByUserId)
                .filter(t -> t._2()._1().getPostId().equals(t._2()._2().getId()))
                .mapValues(t -> 1)
                .reduceByKey((a,b) -> a + b)
                .mapToPair(Tuple2::swap)
                .sortByKey(false)
                .mapToPair(Tuple2::swap)
                .collect().forEach(t -> System.out.println("id = " + t._1() + " " + "value = " + t._2()));

    }

    @Builder
    @AllArgsConstructor
    @EqualsAndHashCode
    @Getter
    static class User implements Serializable {

        private final Long id;
        private final Integer reputation;

        public static User toUser(Row row) {
            return User.builder()
                    .id(row.getLong(0))
                    .reputation(row.getInt(7))
                    .build();
        }

        public static StructType userStructType() {
            return new StructType(new StructField[]{
                    StructField.apply("id", DataTypes.LongType, false, Metadata.empty()),
                    StructField.apply("display_name", DataTypes.StringType, true, Metadata.empty()),
                    StructField.apply("about_me", DataTypes.StringType, true, Metadata.empty()),
                    StructField.apply("age", DataTypes.StringType, true, Metadata.empty()),
                    StructField.apply("creation_date", DataTypes.StringType, true, Metadata.empty()),
                    StructField.apply("last_access_date", DataTypes.StringType, true, Metadata.empty()),
                    StructField.apply("location", DataTypes.StringType, true, Metadata.empty()),
                    StructField.apply("reputation", DataTypes.IntegerType, false, Metadata.empty()),
                    StructField.apply("up_votes", DataTypes.StringType, true, Metadata.empty()),
                    StructField.apply("down_votes", DataTypes.StringType, true, Metadata.empty()),
                    StructField.apply("views", DataTypes.StringType, true, Metadata.empty()),
                    StructField.apply("profile_image_url", DataTypes.StringType, true, Metadata.empty()),
                    StructField.apply("website_url", DataTypes.StringType, true, Metadata.empty())
            });
        }

        public static boolean isNotNullValue(Row row) {
            if (Objects.nonNull(row.get(0)) && Objects.nonNull(row.get(7))) {
                return Boolean.TRUE;
            }

            return Boolean.FALSE;
        }

    }

    @Builder
    @AllArgsConstructor
    @EqualsAndHashCode
    @Getter
    static class Comment implements Serializable {
        private final Long id;
        private final String text;
        private final String creationDate;
        private final Long postId;
        private final Long userId;
        private final String userDisplayName;
        private final Integer score;

        public static Comment toComment(Row row) {
            return Comment.builder()
                    .id(row.getLong(0))
                    .postId(row.getLong(3))
                    .userId(row.getLong(4))
                    .score(row.getInt(6))
                    .build();
        }

        public static StructType commentStructType() {
            return new StructType(new StructField[]{
                    StructField.apply("id", DataTypes.LongType, false, Metadata.empty()),
                    StructField.apply("text", DataTypes.StringType, true, Metadata.empty()),
                    StructField.apply("creation_date", DataTypes.StringType, true, Metadata.empty()),
                    StructField.apply("post_id", DataTypes.LongType, false, Metadata.empty()),
                    StructField.apply("user_id", DataTypes.LongType, false, Metadata.empty()),
                    StructField.apply("user_display_name", DataTypes.StringType, true, Metadata.empty()),
                    StructField.apply("score", DataTypes.IntegerType, true, Metadata.empty())

            });

        }

        public static boolean isNotNullValue(Row row) {
            if (Objects.nonNull(row.get(0)) && Objects.nonNull(row.get(3))
                    && Objects.nonNull(row.get(4)) && Objects.nonNull(row.get(6))) {
                return Boolean.TRUE;
            }

            return Boolean.FALSE;
        }
    }

    @Builder
    @AllArgsConstructor
    @EqualsAndHashCode
    @Getter
    static class Post implements Serializable {
        private final Long id;
        private final Long ownerUserId;
        private final Integer score;

        public static Post toPost(Row row) {
            return Post.builder()
                    .id(row.getLong(0))
                    .ownerUserId(row.getLong(14))
                    .score(row.getInt(17))
                    .build();
        }

        public static StructType postStructType() {
            return new StructType(new StructField[]{
                    StructField.apply("id", DataTypes.LongType, false, Metadata.empty()),
                    StructField.apply("title", DataTypes.StringType, true, Metadata.empty()),
                    StructField.apply("body", DataTypes.StringType, true, Metadata.empty()),
                    StructField.apply("accepted_answer_id", DataTypes.StringType, true, Metadata.empty()),
                    StructField.apply("answer_count", DataTypes.StringType, true, Metadata.empty()),
                    StructField.apply("comment_count", DataTypes.StringType, true, Metadata.empty()),
                    StructField.apply("community_owned_date", DataTypes.StringType, true, Metadata.empty()),
                    StructField.apply("creation_date", DataTypes.StringType, true, Metadata.empty()),
                    StructField.apply("favorite_count", DataTypes.StringType, true, Metadata.empty()),
                    StructField.apply("last_activity_date", DataTypes.StringType, true, Metadata.empty()),
                    StructField.apply("last_edit_date", DataTypes.StringType, true, Metadata.empty()),
                    StructField.apply("last_editor_display_name", DataTypes.StringType, true, Metadata.empty()),
                    StructField.apply("last_editor_user_id", DataTypes.StringType, true, Metadata.empty()),
                    StructField.apply("owner_display_name", DataTypes.StringType, true, Metadata.empty()),
                    StructField.apply("owner_user_id", DataTypes.LongType, false, Metadata.empty()),
                    StructField.apply("parent_id", DataTypes.StringType, true, Metadata.empty()),
                    StructField.apply("post_type_id", DataTypes.LongType, false, Metadata.empty()),
                    StructField.apply("score", DataTypes.IntegerType, false, Metadata.empty()),
                    StructField.apply("tags", DataTypes.StringType, true, Metadata.empty()),
                    StructField.apply("view_count", DataTypes.StringType, true, Metadata.empty())
            });
        }

        public static boolean isNotNullValue(Row row) {
            if (Objects.nonNull(row.get(0)) && Objects.nonNull(row.get(14))
                    && Objects.nonNull(row.get(17))) {
                return Boolean.TRUE;
            }

            return Boolean.FALSE;
        }

    }
}
