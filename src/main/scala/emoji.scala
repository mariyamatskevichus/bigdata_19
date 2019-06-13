import java.io.File

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf
import org.apache.spark.broadcast.Broadcast
import collection.JavaConverters._
import com.vdurmont.emoji._
import com.vdurmont.emoji.Emoji
import com.vdurmont.emoji.EmojiManager
import java.util
import org.apache.spark.sql.functions.{concat, lit}
import org.apache.spark.sql.functions.{collect_list, udf, lit, sum,first, concat_ws}

object emoji {

  def totalStatistics(spark: SparkSession): Unit = {

    val emoji_pos = List("smile", "grinning", "smiley", "blush", "heart_eyes", "wink", "joy", "laughing")
      .map(x => EmojiManager.getForAlias(x).getUnicode).toSet

    val emoji_neg = List("cry", "sob", "rage", "angry", "confounded", "weary", "unamused", "persevere", "disappointed")
      .map(x => EmojiManager.getForAlias(x).getUnicode).toSet

    val negBcast = spark.sparkContext.broadcast[Set[String]](emoji_neg)
    val posBcast = spark.sparkContext.broadcast[Set[String]](emoji_pos)

    import spark.implicits._
    val posts_api = ".\\data\\posts_api.parquet"
    val posts_comments = ".\\data\\posts_comments.parquet"

    val df = spark.read.parquet(posts_api)
    val df2 = spark.read.parquet(posts_comments)


    val negUDF = udf { text: String =>
      EmojiParser.extractEmojis(text).toArray.map(_.toString).toSet
        .intersect(posBcast.value).size}

    val posUDF = udf { text: String =>
      EmojiParser.extractEmojis(text).toArray.map(_.toString).toSet
        .intersect(negBcast.value).size}

    val otherUDF = udf { text: String =>
      EmojiParser.extractEmojis(text).toArray.map(_.toString).toSet
        .diff(posBcast.value.union(negBcast.value)).size}


    val negEmPosts = df.select("text").withColumn("neg_emojii_tot_in_Posts", negUDF('text))
      .select(sum("neg_emojii_tot_in_Posts")).show()
    val negEmComm = df2.select("text").withColumn("neg_emojii_tot_in_Comments", negUDF('text))
      .select(sum("neg_emojii_tot_in_Comments")).show()

    val posEmPosts = df.select("text").withColumn("pos_emojii_tot_in_Posts", posUDF('text))
      .select(sum("pos_emojii_tot_in_Posts")).show()
    val posEmComm = df2.select("text").withColumn("pos_emojii_tot_in_Comments", posUDF('text))
      .select(sum("pos_emojii_tot_in_Comments")).show()

    val otherPosts = df.select("text").withColumn("other_emojii_tot_in_Posts", otherUDF('text))
      .select(sum("other_emojii_tot_in_Posts")).show()
    val otherComm = df2.select("text").withColumn("other_emojii_tot_in_Comments", otherUDF('text))
      .select(sum("other_emojii_tot_in_Comments")).show()

  }

}