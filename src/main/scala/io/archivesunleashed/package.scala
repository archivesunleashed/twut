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

  /** Checks to see if a column exists.
   *
   * @param tweets DataFrame of line-oriented Twitter JSON
   * @return boolean.
   */
  def hasColumn(tweets: DataFrame, column: String) = Try(tweets(column)).isSuccess

  /** Creates a DataFrame of Tweet IDs.
   *
   * @param tweets DataFrame of line-oriented Twitter JSON
   * @return a single-column DataFrame containing Twitter IDs.
   */
  def ids(tweets: DataFrame): DataFrame = {
    tweets.select(
      "id_str"
    )
  }

  /** Creates a DataFrame of Twitter User Info.
   *
   * @param tweets DataFrame of line-oriented Twitter JSON
   * @return a multi-column DataFrame containing Twitter user info.
   */
  def userInfo(tweets: DataFrame): DataFrame = {
    tweets.select(
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
   * @param tweets DataFrame of line-oriented Twitter JSON
   * @return a two columns (text, and full-text) containing Tweet text.
   */
  def text(tweets: DataFrame): DataFrame = {
    if (hasColumn(tweets, "full_text") == true) {
      tweets.select(
        "full_text",
        "text"
      )
    } else {
      tweets.select(
        "text"
      )
    }
  }

  /** Creates a DataFrame of Tweet times.
   *
   * @param tweets DataFrame of line-oriented Twitter JSON
   * @return a single-column DataFrame containing UTC Tweet times.
   */
  def times(tweets: DataFrame): DataFrame = {
    tweets.select(
      "created_at"
    )
  }

  /** Creates a DataFrame of Hashtags.
   *
   * @param tweets DataFrame of line-oriented Twitter JSON
   * @return a single-column DataFrame containg Hashtags.
   */
  def hashtags(tweets: DataFrame): DataFrame = {
    tweets.select(
      explode(col("entities.hashtags.text"))
        .as("hashtags")
      )
  }

  /** Creates a DataFrame of Urls.
   *
   * @param tweets DataFrame of line-oriented Twitter JSON
   * @return a single-column DataFrame containg Urls.
   */
  def urls(tweets: DataFrame): DataFrame = {
    val tweetUrls = tweets.select(
      explode(col("entities.urls.url"))
        .as("url"))
    val expandedUrls = tweets.select(
      explode(col("entities.urls.expanded_url"))
        .as("expanded_url"))
    tweetUrls.union(expandedUrls)
  }

  /** Creates a DataFrame of tweet sources.
   *
   * @param tweets DataFrame of line-oriented Twitter JSON
   * @return a single-column DataFrame containing tweet sources.
   */
  def sources(tweets: DataFrame): DataFrame = {
    tweets.select(
      "source"
    )
  }

  /** Creates a DataFrame of Animated GIF urls.
   *
   * @param tweets DataFrame of line-oriented Twitter JSON
   * @return a single-column DataFrame containing Animated GIF urls.
   */
  def animatedGifUrls(tweets: DataFrame): DataFrame = {
    tweets.where(
      col("extended_entities").isNotNull
        && col("extended_entities.media").isNotNull
        && array_contains(col("extended_entities.media.type"), "animated_gif")
      )
        .select(
          explode(col("extended_entities.media.media_url_https"))
            .alias("animated_gif_url"))
  }

  /** Creates a DataFrame of image urls.
   *
   * @param tweets DataFrame of line-oriented Twitter JSON
   * @return a single-column DataFrame containing Animated GIF urls.
   */
  def imageUrls(tweets: DataFrame): DataFrame = {
    tweets.where(
      col("entities.media").isNotNull
        && array_contains(col("extended_entities.media.type"), "photo")
      )
        .select(
          explode(col("entities.media.media_url_https"))
            .alias("image_url"))
  }

  /** Creates a DataFrame of video urls.
   *
   * @param tweets DataFrame of line-oriented Twitter JSON
   * @return a single-column DataFrame containing Animated GIF urls.
   */
  def videoUrls(tweets: DataFrame): DataFrame = {
    tweets.where(
      col("extended_entities").isNotNull
        && col("extended_entities.media").isNotNull
        && col("extended_entities.media.video_info").isNotNull
        && array_contains(col("extended_entities.media.type"), "video")
      )
        .select(
          explode(col("extended_entities.media.video_info.variants"))
              .alias("video_info"))
            .filter(col("video_info").isNotNull)
            .select(explode(col("video_info")))
            .withColumn("video_url", col("col.url"))
            .drop(col("col"))
  }

  /** Creates a DataFrame of media urls.
   *
   * @param tweets DataFrame of line-oriented Twitter JSON
   * @return a single-column DataFrame containing Animated GIF urls.
   */
  def mediaUrls(tweets: DataFrame): DataFrame = {
    val animated_gif_urls = tweets.where(
      col("extended_entities").isNotNull
        && col("extended_entities.media").isNotNull
        && array_contains(col("extended_entities.media.type"), "animated_gif")
      )
        .select(
          explode(col("extended_entities.media.media_url_https"))
            .alias("animated_gif_url"))

    val image_urls = tweets.where(
      col("entities.media").isNotNull
        && array_contains(col("extended_entities.media.type"), "photo")
      )
        .select(
          explode(col("entities.media.media_url_https"))
            .alias("image_url"))

    val video_urls = tweets.where(
      col("extended_entities").isNotNull
        && col("extended_entities.media").isNotNull
        && col("extended_entities.media.video_info").isNotNull
        && array_contains(col("extended_entities.media.type"), "video")
      )
        .select(
          explode(col("extended_entities.media.video_info.variants"))
              .alias("video_info"))
            .filter(col("video_info").isNotNull)
            .select(explode(col("video_info")))
            .withColumn("video_url", col("col.url"))
            .drop(col("col"))

  image_urls.union(video_urls.union(animated_gif_urls))
  }
}
