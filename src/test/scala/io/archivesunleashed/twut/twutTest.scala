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

package io.archivesunleashed.twut

import com.google.common.io.Resources
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.desc
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}

@RunWith(classOf[JUnitRunner])
class TwutTest extends FunSuite with BeforeAndAfter {
  private val tweets = Resources.getResource("10-sample.jsonl").getPath
  private val master = "local[4]"
  private val appName = "twut-test"
  private var sc: SparkContext = _

  before {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)
    conf.set("spark.driver.allowMultipleContexts", "true");
    sc = new SparkContext(conf)
  }

  test("ID Extraction") {
    val spark = SparkSession.builder().master("local").getOrCreate()
    // scalastyle:off
    import spark.implicits._
    // scalastyle:on
    val tweetsDF = spark.read.json(tweets)

    val ids = twut
      .ids(tweetsDF)
      .orderBy(desc("id_str"))
      .head(3)
    assert(ids.size == 3)
    assert("1201505319286984705" == ids(0).get(0))
    assert("1201505319286755328" == ids(1).get(0))
    assert("1201505319282565121" == ids(2).get(0))
  }

  test("User Info Extraction") {
    val spark = SparkSession.builder().master("local").getOrCreate()
    // scalastyle:off
    import spark.implicits._
    // scalastyle:on
    val tweetsDF = spark.read.json(tweets)

    val userInfo = twut
      .userInfo(tweetsDF)
      .orderBy(desc("id_str"))
      .head(1)
    assert(userInfo.size == 1)
    assert(2331 == userInfo(0).get(0))
    assert(91 == userInfo(0).get(1))
    assert(83 == userInfo(0).get(2))
    assert("973424490934714368" == userInfo(0).get(3))
    assert("日本 山口" == userInfo(0).get(4))
    assert("イサオ(^^)最近ディスクにハマル🎵" == userInfo(0).get(5))
    assert("isao777sp2" == userInfo(0).get(6))
    assert(2137 == userInfo(0).get(7))
    assert(false == userInfo(0).get(8))
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }
}
