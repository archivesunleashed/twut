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
  private val bigTweets = Resources.getResource("500-sample.jsonl").getPath
  private val v2Tweets = Resources.getResource("10-v2-sample.jsonl").getPath
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

    val textDF = tweetsDF.text
    val columns = textDF.columns

    assert(columns.contains("text"))
    assert(!columns.contains("full_text"))
  }

  test("ID Extraction") {
    val spark = SparkSession.builder().master("local").getOrCreate()
    // scalastyle:off
    import spark.implicits._
    // scalastyle:on
    val tweetsDF = spark.read.json(tweets)

    val idsTest = tweetsDF.ids
      .orderBy(desc("id_str"))
      .head(3)
    assert(idsTest.size == 3)
    assert("1201505319286984705" == idsTest(0).get(0))
    assert("1201505319286755328" == idsTest(1).get(0))
    assert("1201505319282565121" == idsTest(2).get(0))
  }

  test("Language Extraction") {
    val spark = SparkSession.builder().master("local").getOrCreate()
    // scalastyle:off
    import spark.implicits._
    // scalastyle:on
    val tweetsDF = spark.read.json(tweets)

    val languageTest = tweetsDF.language
      .head(5)
    assert(languageTest.size == 5)
    assert("tl" == languageTest(0).get(0))
    assert("ja" == languageTest(1).get(0))
    assert("ar" == languageTest(2).get(0))
    assert("ja" == languageTest(3).get(0))
    assert("ja" == languageTest(4).get(0))
  }

  test("User Info Extraction") {
    val spark = SparkSession.builder().master("local").getOrCreate()
    // scalastyle:off
    import spark.implicits._
    // scalastyle:on
    val tweetsDF = spark.read.json(tweets)

    val userInfoTest = tweetsDF.userInfo
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

    val textTest = tweetsDF.text
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

    val timesTest = tweetsDF.times
      .head(5)
    assert(timesTest.size == 5)
    assert("Mon Dec 02 14:16:05 +0000 2019" == timesTest(0).get(0))
    assert("Mon Dec 02 14:16:05 +0000 2019" == timesTest(1).get(0))
    assert("Mon Dec 02 14:16:05 +0000 2019" == timesTest(2).get(0))
    assert("Mon Dec 02 14:16:05 +0000 2019" == timesTest(3).get(0))
    assert("Mon Dec 02 14:16:05 +0000 2019" == timesTest(4).get(0))
  }

  test("Hashtag Extraction") {
    val spark = SparkSession.builder().master("local").getOrCreate()
    // scalastyle:off
    import spark.implicits._
    // scalastyle:on
    val tweetsDF = spark.read.json(tweets)

    val hashtagsTest = tweetsDF.hashtags
      .head(1)
    assert(hashtagsTest.size == 1)
    assert("安元江口と夜あそび" == hashtagsTest(0).get(0))
  }

  test("Url Extraction") {
    val spark = SparkSession.builder().master("local").getOrCreate()
    // scalastyle:off
    import spark.implicits._
    // scalastyle:on
    val tweetsDF = spark.read.json(tweets)

    val urlsTest = tweetsDF.urls
      .head(2)
    assert(urlsTest.size == 2)
    assert("https://t.co/hONLvNozJg" == urlsTest(0).get(0))
    assert(
      "https://twitter.com/komsakaddams/status/1198868305668296705" == urlsTest(
        1
      ).get(0)
    )
  }

  test("Source Extraction") {
    val spark = SparkSession.builder().master("local").getOrCreate()
    // scalastyle:off
    import spark.implicits._
    // scalastyle:on
    val tweetsDF = spark.read.json(tweets)

    val sourcesTest = tweetsDF.sources
      .head(5)
    assert(sourcesTest.size == 5)
    assert(
      "<a href=\"http://twitter.com/download/android\" rel=\"nofollow\">Twitter for Android</a>" == sourcesTest(
        0
      ).get(0)
    )
    assert(
      "<a href=\"http://twitter.com/download/android\" rel=\"nofollow\">Twitter for Android</a>" == sourcesTest(
        1
      ).get(0)
    )
    assert(
      "<a href=\"http://twitter.com/download/iphone\" rel=\"nofollow\">Twitter for iPhone</a>" == sourcesTest(
        2
      ).get(0)
    )
    assert(
      "<a href=\"http://twitter.com/download/iphone\" rel=\"nofollow\">Twitter for iPhone</a>" == sourcesTest(
        3
      ).get(0)
    )
    assert(
      "<a href=\"http://twitter.com/download/iphone\" rel=\"nofollow\">Twitter for iPhone</a>" == sourcesTest(
        4
      ).get(0)
    )
  }

  test("Animated Gif Url Extraction") {
    val spark = SparkSession.builder().master("local").getOrCreate()
    // scalastyle:off
    import spark.implicits._
    // scalastyle:on
    val tweetsDF = spark.read.json(bigTweets)

    val animatedGifsTest = tweetsDF.animatedGifUrls
      .head(3)
    assert(animatedGifsTest.size == 3)
    assert(
      "https://pbs.twimg.com/tweet_video_thumb/EKyat33U4AEpVFf.jpg" == animatedGifsTest(
        0
      ).get(0)
    )
    assert(
      "https://pbs.twimg.com/tweet_video_thumb/EKyQ1fAU8AM7r1I.jpg" == animatedGifsTest(
        1
      ).get(0)
    )
    assert(
      "https://pbs.twimg.com/tweet_video_thumb/EKyau1OU8AAD_OZ.jpg" == animatedGifsTest(
        2
      ).get(0)
    )
  }

  test("Image Url Extraction") {
    val spark = SparkSession.builder().master("local").getOrCreate()
    // scalastyle:off
    import spark.implicits._
    // scalastyle:on
    val tweetsDF = spark.read.json(bigTweets)

    val imageUrlsTest = tweetsDF.imageUrls
      .head(3)
    assert(imageUrlsTest.size == 3)
    assert(
      "https://pbs.twimg.com/media/EKjNNRFXsAANHyQ.jpg" == imageUrlsTest(0)
        .get(0)
    )
    assert(
      "https://pbs.twimg.com/media/EKvWq8LXsAE_HhV.jpg" == imageUrlsTest(1)
        .get(0)
    )
    assert(
      "https://pbs.twimg.com/media/EKx9va5XUAEKcry.jpg" == imageUrlsTest(2)
        .get(0)
    )
  }

  test("Video Url Extraction") {
    val spark = SparkSession.builder().master("local").getOrCreate()
    // scalastyle:off
    import spark.implicits._
    // scalastyle:on
    val tweetsDF = spark.read.json(bigTweets)

    val videoUrlsTest = tweetsDF.videoUrls
      .head(3)
    assert(videoUrlsTest.size == 3)
    assert(
      "https://video.twimg.com/ext_tw_video/1201113203125583872/pu/pl/mLQJE9rIBSE6DaQ_.m3u8?tag=10" == videoUrlsTest(
        0
      ).get(0)
    )
    assert(
      "https://video.twimg.com/ext_tw_video/1201113203125583872/pu/vid/460x258/o5wbkNtC_yVBiGvM.mp4?tag=10" == videoUrlsTest(
        1
      ).get(0)
    )
    assert(
      "https://video.twimg.com/ext_tw_video/1200729524045901825/pu/pl/1LRDIgIbWofMDpOa.m3u8?tag=10" == videoUrlsTest(
        2
      ).get(0)
    )
  }

  test("Media Url Extraction") {
    val spark = SparkSession.builder().master("local").getOrCreate()
    // scalastyle:off
    import spark.implicits._
    // scalastyle:on
    val tweetsDF = spark.read.json(bigTweets)

    val mediaUrlsTest = tweetsDF.mediaUrls
      .head(3)
    assert(mediaUrlsTest.size == 3)
    assert(
      "https://pbs.twimg.com/media/EKjNNRFXsAANHyQ.jpg" == mediaUrlsTest(0)
        .get(0)
    )
    assert(
      "https://pbs.twimg.com/media/EKvWq8LXsAE_HhV.jpg" == mediaUrlsTest(1)
        .get(0)
    )
    assert(
      "https://pbs.twimg.com/media/EKx9va5XUAEKcry.jpg" == mediaUrlsTest(2)
        .get(0)
    )
  }

  test("Media Url Extraction v2") {
    val spark = SparkSession.builder().master("local").getOrCreate()
    // scalastyle:off
    import spark.implicits._
    // scalastyle:on
    val tweetsDF = spark.read.json(v2Tweets)

    val mediaUrlsTest = tweetsDF.mediaUrls
      .head(3)
    assert(mediaUrlsTest.size == 1)
    assert(
      "https://pbs.twimg.com/media/FgJqofEXEAAdQgA.jpg" == mediaUrlsTest(0)
        .get(0)
    )
  }

  test("Remove Sensistive Tweets") {
    val spark = SparkSession.builder().master("local").getOrCreate()
    // scalastyle:off
    import spark.implicits._
    // scalastyle:on
    val tweetsDF = spark.read.json(bigTweets)

    val removeSensitiveTest = tweetsDF.removeSensitive

    assert(tweetsDF.count == 500)
    assert(removeSensitiveTest.count == 246)
  }

  test("Remove Retweets") {
    val spark = SparkSession.builder().master("local").getOrCreate()
    // scalastyle:off
    import spark.implicits._
    // scalastyle:on
    val tweetsDF = spark.read.json(bigTweets)

    val removeRetweetsTest = tweetsDF.removeRetweets

    assert(tweetsDF.count == 500)
    assert(removeRetweetsTest.count == 230)
  }

  test("Remove non-verified user tweets") {
    val spark = SparkSession.builder().master("local").getOrCreate()
    // scalastyle:off
    import spark.implicits._
    // scalastyle:on
    val tweetsDF = spark.read.json(bigTweets)

    val removeNonVerifiedTest = tweetsDF.removeNonVerified

    assert(tweetsDF.count == 500)
    assert(removeNonVerifiedTest.count == 5)
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }
}
