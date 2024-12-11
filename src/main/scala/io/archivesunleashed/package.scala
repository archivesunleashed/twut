/*
 * Copyright © 2019 The Archives Unleashed Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io

import scala.util.Try
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{array_contains, col, desc, explode, max}
import org.apache.spark.sql.types.{StructField, StructType}

/**
  * Package object which supplies implicits to augment generic DataFrames with twut-specific transformations.
  */
package object archivesunleashed {

  /** Implicit class to add Twitter-specific transformations to DataFrames. */
  implicit class Twut(df: DataFrame) {

    /** Checks to see if a column exists.
      *
      * @return boolean.
      */
    private def hasColumn(column: String): Boolean =
      Try(df.col(column)).isSuccess

    /** Checks if v2 data exists (like the "attachments" column).

      * @return boolean.
      */
    private def isV2Data: Boolean = {
      Try(hasColumn("attachments.media") || hasColumn("attachments.media_keys"))
        .getOrElse(false)
    }

    /** Creates a DataFrame of Tweet IDs.
      *
      * @return a single-column DataFrame containing Twitter IDs.
      */
    def ids: DataFrame = {
      df.select(
        "id_str"
      )
    }

    /** Creates a DataFrame of the BCP 47 language identifier corresponding
      *  to the machine-detected language.
      *
      * @return a single-column DataFrame containing the BCP 47 language identifier
      *   corresponding to the machine-detected language.
      */
    def language: DataFrame = {
      df.select(
        "lang"
      )
    }

    /** Creates a DataFrame of Twitter User Info.
      *
      * Handles both v1 and v2 Twitter data by checking the appropriate user information structures.
      *
      * @return a multi-column DataFrame containing Twitter user info.
      */
    def userInfo: DataFrame = {
      val columnsV1 = Seq(
        "user.favourites_count",
        "user.followers_count",
        "user.friends_count",
        "user.id_str",
        "user.location",
        "user.name",
        "user.screen_name",
        "user.statuses_count",
        "user.verified"
      )

      val columnsV2 = Seq(
        "author.favourites_count",
        "author.public_metrics.followers_count",
        "author.public_metrics.following_count",
        "author.id",
        "author.location",
        "author.name",
        "author.username",
        "author.public_metrics.tweet_count",
        "author.verified"
      )

      val existingColumnsV1 = columnsV1.filter(hasColumn)
      val existingColumnsV2 = columnsV2.filter(hasColumn)

      if (existingColumnsV1.nonEmpty) {
        df.select(existingColumnsV1.map(df.col): _*)
      } else if (existingColumnsV2.nonEmpty) {
        df.select(existingColumnsV2.map(df.col): _*)
      } else {
        df.sparkSession.emptyDataFrame
      }
    }

    /** Creates a DataFrame of Tweet text.
      *
      * Handles both v1 and v2 Twitter data by checking for the appropriate text fields.
      *
      * @return a single column or two columns (text, and full-text) containing Tweet text.
      */
    def text: DataFrame = {
      val columnsV1 = Seq("full_text", "text")
      val columnsV2 = Seq("data.full_text", "data.text")

      val existingColumnsV1 = columnsV1.filter(hasColumn)
      val existingColumnsV2 = columnsV2.filter(hasColumn)

      if (existingColumnsV1.nonEmpty) {
        df.select(existingColumnsV1.map(df.col): _*)
      } else if (existingColumnsV2.nonEmpty) {
        df.select(existingColumnsV2.map(df.col): _*)
      } else {
        df.sparkSession.emptyDataFrame
      }
    }

    /** Creates a DataFrame of Tweet times.
      *
      * @param df.DataFrame of line-oriented Twitter JSON
      * @return a single-column DataFrame containing UTC Tweet times.
      */
    def times: DataFrame = {
      df.select(
        "created_at"
      )
    }

    /** Creates a DataFrame of Hashtags.
      *
      * Handles both v1 and v2 Twitter data by checking for hashtags in the appropriate structures.
      *
      * @return a single-column DataFrame containing Hashtags.
      */
    def hashtags: DataFrame = {
      if (isV2Data) {
        df.select(
          explode(col("entities.hashtags.tag"))
            .as("hashtags")
        )
      } else {
        df.select(
          explode(col("entities.hashtags.text"))
            .as("hashtags")
        )
      }
    }

    /** Creates a DataFrame of Urls.
      *
      * @return a single-column DataFrame containg Urls.
      */
    def urls: DataFrame = {
      if (hasColumn("entities.urls.unshortened_url")) {
        val tweetUrls = df.select(
          explode(col("entities.urls.url"))
            .as("url")
        )
        val expandedUrls = df.select(
          explode(col("entities.urls.expanded_url"))
            .as("expanded_url")
        )
        val unshrtnUrls = df.select(
          explode(col("entities.urls.unshortened_url"))
            .as("unshortened_url")
        )
        tweetUrls.unionAll(expandedUrls).unionAll(unshrtnUrls)
      } else {
        val tweetUrls = df.select(
          explode(col("entities.urls.url"))
            .as("url")
        )
        val expandedUrls = df.select(
          explode(col("entities.urls.expanded_url"))
            .as("expanded_url")
        )
        tweetUrls.union(expandedUrls)
      }
    }

    /** Creates a DataFrame of tweet sources.
      *
      * @return a single-column DataFrame containing tweet sources.
      */
    def sources: DataFrame = df.select("source")

    /** Creates a DataFrame of animated GIF URLs.
      *
      * Handles both v1 and v2 Twitter data by checking for animated GIFs in
      * the appropriate structures.
      *
      * @return a single-column DataFrame containing animated GIF URLs.
      */
    def animatedGifUrls: DataFrame = {
      if (isV2Data) {
        val explodedMedia = df
          .withColumn("media_key", explode(col("attachments.media_keys")))
          .withColumn("media", explode(col("attachments.media")))
          .filter(col("media_key") === col("media.media_key"))

        explodedMedia
          .filter(col("media.type") === "animated_gif")
          .select(col("media.url").alias("animated_gif_url"))
      } else {
        df.where(
            col("extended_entities").isNotNull
              && col("extended_entities.media").isNotNull
              && array_contains(
                col("extended_entities.media.type"),
                "animated_gif"
              )
          )
          .select(
            explode(col("extended_entities.media.media_url_https"))
              .alias("animated_gif_url")
          )
      }
    }

    /** Creates a DataFrame of image URLs.
      *
      * Handles both v1 and v2 Twitter data by checking for the presence of media
      * in the appropriate structures.
      *
      * @return a single-column DataFrame containing image URLs.
      */
    def imageUrls: DataFrame = {
      if (isV2Data) {
        val explodedMedia = df
          .withColumn("media_key", explode(col("attachments.media_keys")))
          .withColumn("media", explode(col("attachments.media")))
          .filter(col("media_key") === col("media.media_key"))

        explodedMedia
          .filter(col("media.type") === "photo")
          .select(col("media.url").alias("image_url"))
      } else {
        df.where(
            col("extended_entities").isNotNull
              && col("extended_entities.media").isNotNull
              && array_contains(col("extended_entities.media.type"), "photo")
          )
          .select(
            explode(col("extended_entities.media.media_url_https"))
              .alias("image_url")
          )
      }
    }

    /** Creates a DataFrame of video URLs.
      *
      * Handles both v1 and v2 Twitter data by checking for video content in
      * the appropriate structures.
      *
      * @return a single-column DataFrame containing video URLs.
      */
    def videoUrls: DataFrame = {
      if (isV2Data) {
        val explodedMedia = df
          .withColumn("media_key", explode(col("attachments.media_keys")))
          .withColumn("media", explode(col("attachments.media")))
          .filter(col("media_key") === col("media.media_key"))

        explodedMedia
          .filter(col("media.type") === "video")
          .select(col("media.url").alias("video_url"))
      } else {
        val videoUrlsV1 = df
          .where(
            col("extended_entities").isNotNull
              && col("extended_entities.media").isNotNull
              && col("extended_entities.media.video_info").isNotNull
              && array_contains(col("extended_entities.media.type"), "video")
          )
          .select(
            explode(col("extended_entities.media.video_info.variants"))
              .alias("video_info")
          )
          .filter(col("video_info").isNotNull)
          .select(explode(col("video_info")))
          .withColumn("video_url", col("col.url"))
          .drop(col("col"))

        videoUrlsV1
      }
    }

    /** Creates a DataFrame of media URLs (images, videos, animated GIFs).
      *
      * Handles both v1 and v2 Twitter data by checking for media in
      * the appropriate structures for GIFs, images, and videos.
      *
      * @return a single-column DataFrame containing media URLs.
      */
    def mediaUrls: DataFrame = {
      val animatedGifUrls = this.animatedGifUrls
        .withColumnRenamed("animated_gif_url", "media_url")
      val imageUrls = this.imageUrls
        .withColumnRenamed("image_url", "media_url")
      val videoUrls = this.videoUrls
        .withColumnRenamed("video_url", "media_url")

      imageUrls
        .union(videoUrls)
        .union(animatedGifUrls)
    }

    /** Creates a DataFrame that filters out sensitives tweets.
      *
      * @return original DataFrame with sensitive tweets removed.
      */
    def removeSensitive: DataFrame = {
      df.filter(col("possibly_sensitive").isNull)
        .filter(col("retweeted_status.possibly_sensitive").isNull)
    }

    /** Creates a DataFrame that filters out retweets.
      *
      * @return original DataFrame with retweets removed.
      */
    def removeRetweets: DataFrame = {
      if (isV2Data) {
        df.filter(col("data.referenced_tweets").isNull)
      } else {
        df.filter(col("retweeted_status").isNull)
      }
    }

    /** Creates a DataFrame that filters out tweets from non-verified users.
      *
      * @return original DataFrame with non-verified users' tweets removed.
      */
    def removeNonVerified: DataFrame = {
      df.filter(col("user.verified") === true)
    }

    /** Extracts the most retweeted tweet IDs and their counts.
      *
      * Handles both Twitter v1 and v2 schemas.
      *
      * @return a DataFrame containing tweet IDs and their retweet counts.
      */
    def mostRetweeted: DataFrame = {
      if (isV2Data) {
        df.filter(array_contains(col("referenced_tweets.type"), "retweeted"))
          .select(
            explode(col("referenced_tweets")).alias("referenced"),
            col("public_metrics.retweet_count").alias("retweet_count")
          )
          .filter(col("referenced.type") === "retweeted")
          .select(
            col("referenced.id").alias("tweet_id"),
            col("retweet_count")
          )
          .groupBy("tweet_id")
          .agg(max("retweet_count").alias("max_retweet_count"))
          .orderBy(desc("max_retweet_count"))
      } else {
        df.filter(col("retweeted_status").isNotNull)
          .select(
            col("retweeted_status.id_str").alias("tweet_id"),
            col("retweeted_status.retweet_count").alias("retweet_count")
          )
          .groupBy("tweet_id")
          .agg(max("retweet_count").alias("max_retweet_count"))
          .orderBy(desc("max_retweet_count"))
      }
    }
  }
}
