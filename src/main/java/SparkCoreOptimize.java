import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import scala.Function1;
import scala.Tuple2;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

public class SparkCoreOptimize {
    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf()
                .registerKryoClasses(new Class[]{User.class, Post.class, Comment.class});

        SparkSession ss = SparkSession.builder()
                .appName("Spark-Core-Optimize")
                //.master("") path to Master (Spark Standalone)
                .master("local[*]")
                .config("spark.eventLog.enabled", "true")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.kryoserializer.buffer.max", "128m")
                .config("spark.kryoserializer.buffer", "64m")
                .config(sparkConf)
                .config("spark.eventLog.dir", "")
                .getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(ss.sparkContext());

        JavaRDD<Comment> comments = ss.read()
                .option("header", true)
                .option("mode", "DROPMALFORMED")
                .schema(Comment.commentStructType())
                .csv("comments.csv")
                .filter(Comment::isNotNullValue)
                .javaRDD()
                .map(Comment::toComment);

        JavaRDD<Post> posts = ss.read()
                .option("header", true)
                .option("mode", "DROPMALFORMED")
                .schema(Post.postStructType())
                .csv("posts.csv")
                .filter(Post::isNotNullValue)
                .javaRDD()
                .map(Post::toPost);

        JavaRDD<User> users = ss.read()
                .option("header", true)
                .option("mode", "DROPMALFORMED")
                .schema(User.userStructType())
                .csv("users.csv")
                .filter(User::isNotNullValue)
                .javaRDD()
                .map(User::toUser);

        Broadcast<Integer> one = sc.broadcast(1);

        JavaPairRDD<Long, User> usersWithGoodReputation = users
                .filter(user -> user.getReputation() > one.getValue())
                .mapToPair(user -> new Tuple2<>(user.getId(), user));

        JavaPairRDD<Long, Comment> commentByUserID = comments
                .mapToPair(c -> new Tuple2<>(c.getUserId(), c));

        JavaPairRDD<Long, Post> postByUserId = posts
                .mapToPair(p -> new Tuple2<>(p.getOwnerUserId(), p))
                .partitionBy(new HashPartitioner(25));

        Map<Long, User> userMap = usersWithGoodReputation.collectAsMap();
        Map<Long, User> userHashMap = new HashMap<>(userMap);
        Broadcast<Map<Long, User>> broadcast = sc.broadcast(userHashMap);

        JavaPairRDD<Long, Comment> commentsByUsersWGRId = commentByUserID
                .map((t) -> new Tuple2<>(t._1(),  new Tuple2<>(broadcast.getValue().get(t._1()), t._2())))
                .filter(t -> Objects.nonNull(t._2()._1()))
                .mapToPair(t -> new Tuple2<>(t._1(), t._2()._2()));

       commentsByUsersWGRId
                .join(postByUserId)
                .filter(t -> t._2()._1().getPostId().equals(t._2()._2().getId()))
                .aggregateByKey(0,  (v1, v2) -> v1 + 1, (a, b) -> a + b)
                .map(t -> t)
                .persist(StorageLevel.MEMORY_AND_DISK())
                .sortBy(Tuple2::_2, false, 25)
                .collect().stream().limit(20)
                .forEach(t -> System.out.println("id = " + t._1() + " " + "value = " + t._2()));

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
