import java.io.File
import scala.util.matching.Regex

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.util.SizeEstimator

object Statistics {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\H")
    val spark = SparkSession.builder()
      .appName("Statistics")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    /**create parquete & delete duplicates in followers* **/
    /**show statistics**/

    new File(".\\data").listFiles().filter(_.toString().contains("json")).foreach(path => {
       var df = spark.read.json(path.toString())
       println(path.getName)
       if (path.toString().contains("followers")) {
         println("size with duplicates: " + df.count())
         df = df.dropDuplicates("key")
       }
       df.write.mode(SaveMode.Overwrite).parquet(path.toString().dropRight(5)+".parquet")
       println("size: " + df.count())
       //schema
       df.printSchema()
    })


    /**topPosts **/
    topPosts.topLiked(spark)
    topPosts.topCommented(spark)
    topPosts.topReposted(spark)

    /**topUsers**/
    topUsers.topLiked(spark)
    topUsers.topCommented(spark)
    topUsers.topReposted(spark)

    /**recInfo**/
    topPosts.recInfo(spark)

    /**itmo friends statistics (avg, top with min, max)**/
    topUsers.itmoFriendsStatistics(spark)

    /**emojii statistics**/
    emoji.totalStatistics(spark)

  }
}
