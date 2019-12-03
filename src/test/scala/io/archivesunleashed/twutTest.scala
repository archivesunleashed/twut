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

  test("Column check") {
    val spark = SparkSession.builder().master("local").getOrCreate()
    // scalastyle:off
    import spark.implicits._
    // scalastyle:on
    val tweetsDF = spark.read.json(tweets)

    val hasFullText = hasColumn(tweetsDF, "full_text")
    val hasText = hasColumn(tweetsDF, "text")

    assert(hasFullText == false)
    assert(hasText == true)
  }

  test("ID Extraction") {
    val spark = SparkSession.builder().master("local").getOrCreate()
    // scalastyle:off
    import spark.implicits._
    // scalastyle:on
    val tweetsDF = spark.read.json(tweets)

    val idsTest = ids(tweetsDF)
      .orderBy(desc("id_str"))
      .head(3)
    assert(idsTest.size == 3)
    assert("1201505319286984705" == idsTest(0).get(0))
    assert("1201505319286755328" == idsTest(1).get(0))
    assert("1201505319282565121" == idsTest(2).get(0))
  }

  test("User Info Extraction") {
    val spark = SparkSession.builder().master("local").getOrCreate()
    // scalastyle:off
    import spark.implicits._
    // scalastyle:on
    val tweetsDF = spark.read.json(tweets)

    val userInfoTest = userInfo(tweetsDF)
      .orderBy(desc("id_str"))
      .head(1)
    assert(userInfoTest.size == 1)
    assert(2331 == userInfoTest(0).get(0))
    assert(91 == userInfoTest(0).get(1))
    assert(83 == userInfoTest(0).get(2))
    assert("973424490934714368" == userInfoTest(0).get(3))
    assert("日本 山口" == userInfoTest(0).get(4))
    assert("イサオ(^^)最近ディスクにハマル🎵" == userInfoTest(0).get(5))
    assert("isao777sp2" == userInfoTest(0).get(6))
    assert(2137 == userInfoTest(0).get(7))
    // scalastyle:off
    assert(false == userInfoTest(0).get(8))
    // scalastyle:on
  }

  test("Text Extraction") {
    val spark = SparkSession.builder().master("local").getOrCreate()
    // scalastyle:off
    import spark.implicits._
    // scalastyle:on
    val tweetsDF = spark.read.json(tweets)

    val textTest = text(tweetsDF)
      .head(3)
    assert(textTest.size == 3)
    assert("Baket ang pogi mo???" == textTest(0).get(0))
    assert("今日すげぇな！#安元江口と夜あそび" == textTest(1).get(0))
    assert("@flower_1901 عسى الله يوفقنا 🙏🏻" == textTest(2).get(0))
  }

  test("Times Extraction") {
    val spark = SparkSession.builder().master("local").getOrCreate()
    // scalastyle:off
    import spark.implicits._
    // scalastyle:on
    val tweetsDF = spark.read.json(tweets)

    val timesTest = times(tweetsDF)
      .head(5)
    assert(timesTest.size == 5)
    assert("Mon Dec 02 14:16:05 +0000 2019" == timesTest(0).get(0))
    assert("Mon Dec 02 14:16:05 +0000 2019" == timesTest(1).get(0))
    assert("Mon Dec 02 14:16:05 +0000 2019" == timesTest(2).get(0))
    assert("Mon Dec 02 14:16:05 +0000 2019" == timesTest(3).get(0))
    assert("Mon Dec 02 14:16:05 +0000 2019" == timesTest(4).get(0))
  }


  after {
    if (sc != null) {
      sc.stop()
    }
  }
}
