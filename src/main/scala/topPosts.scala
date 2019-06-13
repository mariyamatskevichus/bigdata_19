import java.io.File

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{collect_list, collect_set, concat, concat_ws, explode, first, lit, sum, udf}

object topPosts {


  def topLiked(spark: SparkSession): Unit ={
    import spark.implicits._
    val posts_likes = ".\\data\\posts_likes.parquet"
    val df = spark.read.parquet( posts_likes)
    df.withColumn("col_post_key",concat($"ownerId", lit(" "), $"itemId"))
      .groupBy("col_post_key")
      .count()
      .orderBy($"count".desc)
      .limit(100)
      .show()
     // .write.mode(SaveMode.Overwrite).parquet(".\\data\\topLikedPosts.parquet")
  }

  def topCommented(spark: SparkSession): Unit ={
    import spark.implicits._
    val posts_comm = ".\\data\\posts_comments.parquet"

    val df = spark.read.parquet(posts_comm)

    df.withColumn("col_post_key",concat($"post_id", lit(" "), $"post_owner"))
      .groupBy("col_post_key")
      .count()
      .orderBy($"count".desc)
      .limit(100)
      .show()
    //  .write.mode(SaveMode.Overwrite).parquet(".\\data\\topCommentedPosts.parquet")
  }

  def topReposted(spark: SparkSession): Unit ={
    import spark.implicits._
    val posts_api = ".\\data\\posts_api.parquet"

    val df = spark.read.parquet(posts_api)

    df.withColumn("col_post_key",concat($"id", lit(" "), $"owner_id"))
      .groupBy("col_post_key")
      .agg(sum($"reposts.count").alias("reposts_tot"))
      .orderBy($"reposts_tot".desc)
      .limit(100)
      .show()
     // .write.mode(SaveMode.Overwrite).parquet(".\\data\\topRepostedPosts.parquet")
  }
  def recInfo(spark: SparkSession): Unit ={

    import spark.implicits._
    val posts_api = ".\\data\\posts_api.parquet"

    val df = spark.read.parquet(posts_api)
    df.withColumn("col_post_key",concat($"owner_id", lit(" "), $"id"))
      .select("col_post_key", "copy_history")
      .filter("copy_history is not null")
      .withColumn("orig_post_key",concat($"copy_history.owner_id", $"copy_history.id"))
      .withColumn("originalpost", explode($"orig_post_key"))
      .groupBy("originalpost")
      .agg(collect_set("col_post_key").alias("posts"))
      .limit(100)
      .show(100)
  //    .write.mode(SaveMode.Overwrite).parquet(".\\data\\recInfo.parquet")

  }

}
