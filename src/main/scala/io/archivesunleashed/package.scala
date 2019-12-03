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
}
