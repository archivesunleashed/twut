# twut usage

## How do I...

- [Extract tweet ids](usage.md#extract-tweet-ids)
- [Extract user information](usage.md#extract-user-information)
- [Extract tweet text](usage.md#extract-tweet-text)
- [Extract tweet times](usage.md#extract-tweet-times)
- [Extract tweet sources](usage.md#extract-tweet-sources)
- [Extract hashtags](usage.md#extract-hashtags)
- [Extract urls](usage.md#extract-urls)
- [Extract animated gif urls](usage.md#extract-animated-gif-urls)
- [Extract image urls](usage.md#extract-image-urls)
- [Extract media urls](usage.md#extract-media-urls)
- [Extract video urls](usage.md#extract-video-urls)
- [Work with DataFrame Results](usage.md#work-with-dataframe-results)

## Extract Tweet IDs

Single-column DataFrame containing Tweet IDs.

### Scala DF

```scala
import io.archivesunleashed._

val tweets = "src/test/resources/10-sample.jsonl"
val tweetsDF = spark.read.json(tweets)

ids(tweetsDF).show(2, false)

+-------------------+
|id_str             |
+-------------------+
|1201505319257403392|
|1201505319282565121|
+-------------------+
only showing top 2 rows
```

### Python DF

**TODO**

## Extract User Information

Multi-column DataFrame containing the following columns: `favourites_count`, `followers_count`, `friends_count`, `id_str`, `location`, `name`, `screen_name`, `statuses_count`, and `verified`.

### Scala DF

```scala
import io.archivesunleashed._

val tweets = "src/test/resources/10-sample.jsonl"
val tweetsDF = spark.read.json(tweets)

userInfo(tweetsDF).show(2, false)

+----------------+---------------+-------------+-------------------+--------+-------------------+------------+--------------+--------+
|favourites_count|followers_count|friends_count|id_str             |location|name               |screen_name |statuses_count|verified|
+----------------+---------------+-------------+-------------------+--------+-------------------+------------+--------------+--------+
|8302            |101            |133          |1027887558032732161|nct🌱   |车美               |M_chemei    |3720          |false   |
|2552            |73             |218          |2548066344         |null    |ひーこ☆禿げても愛せ|heeko_gr_029|15830         |false   |
+----------------+---------------+-------------+-------------------+--------+-------------------+------------+--------------+--------+
only showing top 2 rows
```

### Python DF

**TODO**

## Extract Tweet Text

Multi-column DataFrame containing the following columns: `full_text` (if it is available), and `text`.

### Scala DF

```scala
import io.archivesunleashed._

val tweets = "src/test/resources/10-sample.jsonl"
val tweetsDF = spark.read.json(tweets)

text(tweetsDF).show(2, false)

+---------------------------------+
|text                             |
+---------------------------------+
|Baket ang pogi mo???             |
|今日すげぇな！#安元江口と夜あそび|
+---------------------------------+
only showing top 2 rows
```

### Python DF

**TODO**

## Extract Tweet Times

Single-column DataFrame containing the tweet time.

### Scala DF

```scala
import io.archivesunleashed._

val tweets = "src/test/resources/10-sample.jsonl"
val tweetsDF = spark.read.json(tweets)

times(tweetsDF).show(2, false)

+------------------------------+
|created_at                    |
+------------------------------+
|Mon Dec 02 14:16:05 +0000 2019|
|Mon Dec 02 14:16:05 +0000 2019|
+------------------------------+
only showing top 2 rows
```

### Python DF

**TODO**

## Extract Tweet Sources

### Scala DF

```scala
import io.archivesunleashed._

val tweets = "src/test/resources/10-sample.jsonl"
val tweetsDF = spark.read.json(tweets)

sources(tweetsDF).show(10, false)

+------------------------------------------------------------------------------------+
|source                                                                              |
+------------------------------------------------------------------------------------+
|<a href="http://twitter.com/download/android" rel="nofollow">Twitter for Android</a>|
|<a href="http://twitter.com/download/android" rel="nofollow">Twitter for Android</a>|
|<a href="http://twitter.com/download/iphone" rel="nofollow">Twitter for iPhone</a>  |
|<a href="http://twitter.com/download/iphone" rel="nofollow">Twitter for iPhone</a>  |
|<a href="http://twitter.com/download/iphone" rel="nofollow">Twitter for iPhone</a>  |
|<a href="https://mobile.twitter.com" rel="nofollow">Twitter Web App</a>             |
|<a href="http://twitter.com/download/iphone" rel="nofollow">Twitter for iPhone</a>  |
|<a href="http://twitter.com/download/iphone" rel="nofollow">Twitter for iPhone</a>  |
|<a href="http://twitter.com/download/android" rel="nofollow">Twitter for Android</a>|
|<a href="http://twitter.com/download/iphone" rel="nofollow">Twitter for iPhone</a>  |
+------------------------------------------------------------------------------------+
```

### Python DF

**TODO**

## Extract Hashtags

### Scala DF

```scala
import io.archivesunleashed._

val tweets = "src/test/resources/10-sample.jsonl"
val tweetsDF = spark.read.json(tweets)

hashtags(tweetsDF).show

+------------------+
|          hashtags|
+------------------+
|安元江口と夜あそび|
+------------------+
```

### Python DF

**TODO**

## Extract Urls

### Scala DF

```scala
import io.archivesunleashed._

val tweets = "src/test/resources/10-sample.jsonl"
val tweetsDF = spark.read.json(tweets)

urls(tweetsDF).show(10, false)

+-----------------------------------------------------------+
|url                                                        |
+-----------------------------------------------------------+
|https://t.co/hONLvNozJg                                    |
|https://twitter.com/komsakaddams/status/1198868305668296705|
+-----------------------------------------------------------+
```

### Python DF

**TODO**

## Extract Animated Gif Urls

### Scala DF

```scala
import io.archivesunleashed._

val tweets = "src/test/resources/500-sample.jsonl"
val tweetsDF = spark.read.json(tweets)

animatedGifUrls(tweetsDF).show(10, false)

+-----------------------------------------------------------+
|animated_gif_url                                           |
+-----------------------------------------------------------+
|https://pbs.twimg.com/tweet_video_thumb/EKyat33U4AEpVFf.jpg|
|https://pbs.twimg.com/tweet_video_thumb/EKyQ1fAU8AM7r1I.jpg|
|https://pbs.twimg.com/tweet_video_thumb/EKyau1OU8AAD_OZ.jpg|
+-----------------------------------------------------------+
```

### Python DF

**TODO**

## Extract Image Urls

### Scala DF

```scala
import io.archivesunleashed._

val tweets = "src/test/resources/500-sample.jsonl"
val tweetsDF = spark.read.json(tweets)

imageUrls(tweetsDF).show(10, false)

+-----------------------------------------------+
|image_url                                      |
+-----------------------------------------------+
|https://pbs.twimg.com/media/EKjNNRFXsAANHyQ.jpg|
|https://pbs.twimg.com/media/EKvWq8LXsAE_HhV.jpg|
|https://pbs.twimg.com/media/EKx9va5XUAEKcry.jpg|
|https://pbs.twimg.com/media/EKyNK0-WoAMDou3.jpg|
|https://pbs.twimg.com/media/EKyHOyZVUAE3GX6.jpg|
+-----------------------------------------------+
```

### Python DF

**TODO**

## Extract Media Urls

### Scala DF

```scala
import io.archivesunleashed._

val tweets = "src/test/resources/500-sample.jsonl"
val tweetsDF = spark.read.json(tweets)

mediaUrls(tweetsDF).show(5, false)

+-----------------------------------------------+
|image_url                                      |
+-----------------------------------------------+
|https://pbs.twimg.com/media/EKjNNRFXsAANHyQ.jpg|
|https://pbs.twimg.com/media/EKvWq8LXsAE_HhV.jpg|
|https://pbs.twimg.com/media/EKx9va5XUAEKcry.jpg|
|https://pbs.twimg.com/media/EKyNK0-WoAMDou3.jpg|
|https://pbs.twimg.com/media/EKyHOyZVUAE3GX6.jpg|
+-----------------------------------------------+
```

### Python DF

**TODO**

## Extract Video Urls

### Scala DF

```scala
import io.archivesunleashed._

val tweets = "src/test/resources/500-sample.jsonl"
val tweetsDF = spark.read.json(tweets)

videoUrls(tweetsDF).show(5, false)

+---------------------------------------------------------------------------------------------------+
|video_url                                                                                          |
+---------------------------------------------------------------------------------------------------+
|https://video.twimg.com/ext_tw_video/1201113203125583872/pu/pl/mLQJE9rIBSE6DaQ_.m3u8?tag=10        |
|https://video.twimg.com/ext_tw_video/1201113203125583872/pu/vid/460x258/o5wbkNtC_yVBiGvM.mp4?tag=10|
|https://video.twimg.com/ext_tw_video/1200729524045901825/pu/pl/1LRDIgIbWofMDpOa.m3u8?tag=10        |
|https://video.twimg.com/ext_tw_video/1200729524045901825/pu/vid/360x638/KrMl6qgy_8ugHBW-.mp4?tag=10|
|https://video.twimg.com/ext_tw_video/1200729524045901825/pu/vid/320x568/lMOyqZH6fnCoDGzI.mp4?tag=10|
+---------------------------------------------------------------------------------------------------+
```

### Python DF

**TODO**

## Work with DataFrame Results

Most script snippets in the documentation end with `.show(20, false)` (in Scala) and `.show(20, False)` (in Python). This prints a table of results with 20 rows and _doesn't_ truncate the columns (the `false` or `False`). You can change the second parameter to `true` (Scala) or `True` (Python) if the columns get too wide for better display. If you want more or fewer results, change the first number.

## Scala

If you want to return a set of results, the counterpart of `.take(10)` with RDDs is `.head(10)`.
So, something like (in Scala):

```scala
  ids(tweetDF)
  // more transformations here...
  .head(10)
```

In the Scala console, the results are automatically assigned to a variable, like the following:

```scala
res1: Array[org.apache.spark.sql.Row] = Array(...)
```

Scala automatically numbers the variables, starting at 0, so that the number will increment with each statement.
You can then manipulate the variable, for example `res0(0)` to access the first element in the array.

Don't like the variable name Scala gives you?

You can do something like this:

```scala
val r = ids(tweetDF)
  // more transformations here...
  .head(10)
```

Scala assigns the results to `r` is this case, which you can then subsequently manipulate, like `r(0)` to access the first element.

If you want _all_ results, replace `.take(10)` with `.collect()`.
This will return _all_ results to the console.

**WARNING**: Be careful with `.collect()`! If your results contain ten million records, AUT will try to return _all of them_  to your console (on your physical machine).
Most likely, your machine won't have enough memory!

Alternatively, if you want to save the results to disk, replace `.show(20, false)` with the following:

```scala
  .write.csv("/path/to/export/directory/")
```

Replace `/path/to/export/directory/` with your desired location.
Note that this is a _directory_, not a _file_.

Depending on your intended use of the output, you may want to include headers in the CSV file, in which case:

```scala
  .write.option("header","true").csv("/path/to/export/directory/")
```

If you want to store the results with the intention to read the results back later for further processing, then use Parquet format:

```scala
  .write.parquet("/path/to/export/directory/")
```

Replace `/path/to/export/directory/` with your desired location.
Note that this is a _directory_, not a _file_.

Later, as in a completely separate session, you can read the results back in and continuing processing, as follows:

```
val results = spark.read.parquet("/path/to/export/directory/")

results.show(20, false)
```

Parquet encodes metadata such as the schema and column types, so you can pick up exactly where you left off.
Note that this works even across languages (e.g., export to Parquet from Scala, read back in Python) or any system that supports Parquet.

## Python

TODO: Python basically the same, but with Python syntax. However, we should be explicit and lay out the steps.