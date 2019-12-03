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

package io.archivesunleashed

import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame

class TwutPy(sc: SparkContext) {

  /** Creates a DataFrame of Tweet IDs. **/
  def ids(tweets: DataFrame): DataFrame = {
    ids(tweets)
  }

  /** Creates a DataFrame of Twitter User Info. */
  def userInfo(tweets: DataFrame): DataFrame = {
    userInfo(tweets)
  }

  /** Creates a DataFame of tweeted urls. */
  def urls(tweets: DataFrame): DataFrame = {
    urls(tweets)
  }
}
