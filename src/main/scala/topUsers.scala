import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{concat, lit, sum, avg, round}
import org.apache.spark.sql.catalyst.expressions.In

object topUsers {

  def topLiked(spark: SparkSession): Unit ={
   //who have left more "like"
    import spark.implicits._
    val posts_likes = ".\\data\\posts_likes.parquet"

    val df = spark.read.parquet(posts_likes)

    df.groupBy("likerId").count().orderBy($"count".desc).limit(100)
      .show()
      //.write.mode(SaveMode.Overwrite).parquet(".\\data\\topLikeUsers.parquet")
  }

  def topCommented(spark: SparkSession): Unit ={
    import spark.implicits._
    val posts_comm = ".\\data\\posts_comments.parquet"
    val df = spark.read.parquet(posts_comm)
    df.groupBy("from_id").count().orderBy($"count".desc).limit(100)
      .show()
      //.write.mode(SaveMode.Overwrite).parquet(".\\data\\topCommentUsers.parquet")
  }

  def topReposted(spark: SparkSession): Unit = {
    import spark.implicits._
    val posts_comm = ".\\data\\posts_api.parquet"
    val df = spark.read.parquet(posts_comm)
    df.filter("copy_history is not null") // - repost
      .groupBy("owner_id") //copy_history.owner_id *(если наоборот)
      .count()
      .orderBy($"count".desc)
      .limit(100)
      .show()
      //.write.mode(SaveMode.Overwrite).parquet(".\\data\\topRepostUsers.parquet")
  }

    def itmoFriendsStatistics(spark: SparkSession): Unit ={
      import spark.implicits._
      val friendzz = ".\\data\\followers_friends.parquet"
      val followerzz = ".\\data\\followers.parquet"
      val df = spark.read.parquet(friendzz)
      val df2 = spark.read.parquet(followerzz)
      df2.createOrReplaceTempView("followers")
      //spark.sql("show tables").show()
      var frzStat = df.filter("friend IN (select follower from followers)")
                  .groupBy("profile")
                  .count()
      frzStat.createOrReplaceTempView("frzStat")
      frzStat.agg(round(avg("count"))).show()
      frzStat.orderBy($"count".desc).show(20)
      frzStat.orderBy($"count".asc).show(20)
    }
}
