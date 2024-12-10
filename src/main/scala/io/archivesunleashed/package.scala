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
import org.apache.spark.sql.functions.{array_contains, col, explode}

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
      Try(df(column)).isSuccess

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
      * @return a multi-column DataFrame containing Twitter user info.
      */
    def userInfo: DataFrame = {
      df.select(
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
    }

    /** Creates a DataFrame of Tweet text.
      *
      * @return a single column or two columns (text, and full-text) containing Tweet text.
      */
    def text: DataFrame = {
      if (hasColumn("full_text")) {
        df.select(
          "full_text",
          "text"
        )
      } else {
        df.select(
          "text"
        )
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
      * @return a single-column DataFrame containg Hashtags.
      */
    def hashtags: DataFrame = {
      df.select(
        explode(col("entities.hashtags.text"))
          .as("hashtags")
      )
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

    /** Creates a DataFrame of Animated GIF urls.
      *
      * @return a single-column DataFrame containing Animated GIF urls.
      */
    def animatedGifUrls: DataFrame = {
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

    /** Creates a DataFrame of image urls.
      *
      * @return a single-column DataFrame containing Animated GIF urls.
      */
    def imageUrls: DataFrame = {
      df.where(
          col("entities.media").isNotNull
            && array_contains(col("extended_entities.media.type"), "photo")
        )
        .select(
          explode(col("entities.media.media_url_https"))
            .alias("image_url")
        )
    }

    /** Creates a DataFrame of video urls.
      *
      * @return a single-column DataFrame containing Animated GIF urls.
      */
    def videoUrls: DataFrame = {
      df.where(
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
    }

    /** Creates a DataFrame of media urls.
      *
      * @return a single-column DataFrame containing Animated GIF urls.
      */
    def mediaUrls: DataFrame = {
      val animated_gif_urls = df
        .where(
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

      val image_urls = df
        .where(
          col("entities.media").isNotNull
            && array_contains(col("extended_entities.media.type"), "photo")
        )
        .select(
          explode(col("entities.media.media_url_https"))
            .alias("image_url")
        )

      val video_urls = df
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

      image_urls.union(video_urls.union(animated_gif_urls))
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
    def removeRetweets: DataFrame = df.filter(col("retweeted_status").isNull)

    /** Creates a DataFrame that filters out tweets from non-verified users.
      *
      * @return original DataFrame with non-verified users' tweets removed.
      */
    def removeNonVerified: DataFrame = df.filter(col("user.verified") === true)
  }
}
