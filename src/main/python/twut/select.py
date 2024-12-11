from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import array_contains, col, desc, explode, max
from pyspark.sql.types import StructType
from pyspark.sql.utils import AnalysisException


def has_column(df: DataFrame, column_name: str) -> bool:
    """Check if a column exists in DataFrame."""
    try:
        df[column_name]
        return True
    except AnalysisException:
        return False


def is_v2_data(df: DataFrame) -> bool:
    """Check if v2 data exists (e.g., 'attachments.media' or 'attachments.media_keys')."""
    return has_column(df, "attachments.media") or has_column(
        df, "attachments.media_keys"
    )


def ids(df: DataFrame) -> DataFrame:
    """Creates a DataFrame of Tweet IDs."""
    return df.select("id_str")


def language(df: DataFrame) -> DataFrame:
    """
    Creates a DataFrame of the BCP 47 language identifier corresponding
    to the machine-detected language.
    """
    return df.select("lang")


from pyspark.sql import DataFrame, SparkSession


def userInfo(df: DataFrame) -> DataFrame:
    """
    Creates a DataFrame of Twitter User Info.

    Handles both v1 and v2 Twitter data by checking the appropriate user information structures.
    """
    columns_v1 = [
        "user.favourites_count",
        "user.followers_count",
        "user.friends_count",
        "user.id_str",
        "user.location",
        "user.name",
        "user.screen_name",
        "user.statuses_count",
        "user.verified",
    ]
    columns_v2 = [
        "author.favourites_count",
        "author.public_metrics.followers_count",
        "author.public_metrics.following_count",
        "author.id_str",
        "author.location",
        "author.name",
        "author.screen_name",
        "author.public_metrics.tweet_count",
        "author.verified",
    ]

    existing_columns_v1 = [col for col in columns_v1 if has_column(df, col)]
    existing_columns_v2 = [col for col in columns_v2 if has_column(df, col)]

    if existing_columns_v1:
        return df.select(*existing_columns_v1)
    elif existing_columns_v2:
        return df.select(*existing_columns_v2)
    else:
        spark = SparkSession.builder.getOrCreate()
        return spark.createDataFrame([], df.schema)


def text(df: DataFrame) -> DataFrame:
    """
    Creates a DataFrame of Tweet text.

    Handles both v1 and v2 Twitter data by checking for the appropriate text fields.
    """
    columns_v1 = ["full_text", "text"]
    columns_v2 = ["data.full_text", "data.text"]

    existing_columns_v1 = [col for col in columns_v1 if has_column(df, col)]
    existing_columns_v2 = [col for col in columns_v2 if has_column(df, col)]

    if existing_columns_v1:
        return df.select(*existing_columns_v1)
    elif existing_columns_v2:
        return df.select(*existing_columns_v2)
    else:
        spark = SparkSession.builder.getOrCreate()
        return spark.createDataFrame([], StructType([]))


def times(df: DataFrame) -> DataFrame:
    """Creates a DataFrame of Tweet times."""
    return df.select("created_at")


def hashtags(df: DataFrame) -> DataFrame:
    """
    Creates a DataFrame of Hashtags.

    Handles both v1 and v2 Twitter data by checking for hashtags in the appropriate structures.
    """
    columns_v1 = ["entities.hashtags.text"]
    columns_v2 = ["entities.hashtags.tag"]

    existing_columns_v1 = [col for col in columns_v1 if has_column(df, col)]
    existing_columns_v2 = [col for col in columns_v2 if has_column(df, col)]

    if existing_columns_v1:
        return df.select(explode(col(existing_columns_v1[0])).alias("hashtags"))
    elif existing_columns_v2:
        return df.select(explode(col(existing_columns_v2[0])).alias("hashtags"))
    else:
        spark = SparkSession.builder.getOrCreate()
        return spark.createDataFrame([], df.schema)


def sources(df: DataFrame) -> DataFrame:
    """Creates a DataFrame of tweet sources."""
    return df.select("source")


def urls(df: DataFrame) -> DataFrame:
    """Creates a DataFrame of Urls."""
    tweetUrls = df.select(explode(col("entities.urls.url")).alias("url"))
    expandedUrls = df.select(
        explode(col("entities.urls.expanded_url")).alias("expanded_url")
    )
    return tweetUrls.union(expandedUrls)


def animatedGifUrls(df: DataFrame) -> DataFrame:
    """
    Creates a DataFrame of Animated GIF URLs.

    Handles both v1 and v2 Twitter data by checking for animated GIFs in
    the appropriate structures.
    """
    if is_v2_data(df):
        exploded_media = (
            df.withColumn("media_key", explode(col("attachments.media_keys")))
            .withColumn("media", explode(col("attachments.media")))
            .filter(col("media_key") == col("media.media_key"))
        )

        return exploded_media.filter(col("media.type") == "animated_gif").select(
            col("media.url").alias("animated_gif_url")
        )
    else:
        return df.where(
            col("extended_entities").isNotNull()
            & col("extended_entities.media").isNotNull()
            & array_contains(col("extended_entities.media.type"), "animated_gif")
        ).select(
            explode(col("extended_entities.media.media_url_https")).alias(
                "animated_gif_url"
            )
        )


def imageUrls(df: DataFrame) -> DataFrame:
    """
    Creates a DataFrame of image URLs.

    Handles both v1 and v2 Twitter data by checking for the presence of media
    in the appropriate structures.
    """
    if is_v2_data(df):
        exploded_media = (
            df.withColumn("media_key", explode(col("attachments.media_keys")))
            .withColumn("media", explode(col("attachments.media")))
            .filter(col("media_key") == col("media.media_key"))
        )

        return exploded_media.filter(col("media.type") == "photo").select(
            col("media.url").alias("image_url")
        )
    else:
        return df.where(
            col("extended_entities").isNotNull()
            & col("extended_entities.media").isNotNull()
            & array_contains(col("extended_entities.media.type"), "photo")
        ).select(
            explode(col("extended_entities.media.media_url_https")).alias("image_url")
        )


def videoUrls(df: DataFrame) -> DataFrame:
    """
    Creates a DataFrame of video URLs.

    Handles both v1 and v2 Twitter data by checking for video content in
    the appropriate structures.
    """
    if is_v2_data(df):
        exploded_media = (
            df.withColumn("media_key", explode(col("attachments.media_keys")))
            .withColumn("media", explode(col("attachments.media")))
            .filter(col("media_key") == col("media.media_key"))
        )

        return exploded_media.filter(col("media.type") == "video").select(
            col("media.url").alias("video_url")
        )
    else:
        return (
            df.where(
                col("extended_entities").isNotNull()
                & col("extended_entities.media").isNotNull()
                & col("extended_entities.media.video_info").isNotNull()
                & array_contains(col("extended_entities.media.type"), "video")
            )
            .select(
                explode(col("extended_entities.media.video_info.variants")).alias(
                    "video_info"
                )
            )
            .filter(col("video_info").isNotNull())
            .select(explode(col("video_info")))
            .withColumn("video_url", col("col.url"))
            .drop(col("col"))
        )


def mediaUrls(df: DataFrame) -> DataFrame:
    """
    Creates a DataFrame of media URLs (images, videos, animated GIFs).

    Handles both v1 and v2 Twitter data by checking for media in
    the appropriate structures for GIFs, images, and videos.
    """
    animated_gif_urls = animatedGifUrls(df).withColumnRenamed(
        "animated_gif_url", "media_url"
    )

    image_urls = imageUrls(df).withColumnRenamed("image_url", "media_url")
    video_urls = videoUrls(df).withColumnRenamed("video_url", "media_url")

    return image_urls.union(video_urls).union(animated_gif_urls)


def mostRetweeted(df: DataFrame) -> DataFrame:
    """
    Extracts the most retweeted tweet IDs and their counts.

    Handles both Twitter v1 and v2 schemas.
    """
    if is_v2_data(df):
        return (
            df.filter(array_contains(col("referenced_tweets.type"), "retweeted"))
            .select(
                explode(col("referenced_tweets")).alias("referenced"),
                col("public_metrics.retweet_count").alias("retweet_count"),
            )
            .filter(col("referenced.type") == "retweeted")
            .select(col("referenced.id").alias("tweet_id"), col("retweet_count"))
            .groupBy("tweet_id")
            .agg(max("retweet_count").alias("max_retweet_count"))
            .orderBy(desc("max_retweet_count"))
        )
    else:
        return (
            df.filter(col("retweeted_status").isNotNull())
            .select(
                col("retweeted_status.id_str").alias("tweet_id"),
                col("retweeted_status.retweet_count").alias("retweet_count"),
            )
            .groupBy("tweet_id")
            .agg(max("retweet_count").alias("max_retweet_count"))
            .orderBy(desc("max_retweet_count"))
        )
