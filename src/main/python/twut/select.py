from pyspark.sql import DataFrame
from pyspark.sql.functions import array_contains, col, explode
from pyspark.sql.utils import AnalysisException


# Check if a column exists in DataFrame.
def has_column(df: DataFrame, column_name: str) -> bool:
    try:
        df[column_name]
        return True
    except AnalysisException:
        return False


def ids(df: DataFrame) -> DataFrame:
    """Creates a DataFrame of Tweet IDs."""
    return df.select("id_str")


def language(df: DataFrame) -> DataFrame:
    """
    Creates a DataFrame of the BCP 47 language identifier corresponding
    to the machine-detected language.
    """
    return df.select("lang")


def userInfo(df: DataFrame) -> DataFrame:
    """Creates a DataFrame of Twitter User Info."""
    return df.select(
        "user.favourites_count",
        "user.followers_count",
        "user.friends_count",
        "user.id_str",
        "user.location",
        "user.name",
        "user.screen_name",
        "user.statuses_count",
        "user.verified",
    )


def text(df: DataFrame) -> DataFrame:
    """Creates a DataFrame of Tweet text."""
    if has_column(df, "full_text"):
        return df.select("full_text", "text")
    else:
        return df.select("text")


def times(df: DataFrame) -> DataFrame:
    """Creates a DataFrame of Tweet times."""
    return df.select("created_at")


def hashtags(df: DataFrame) -> DataFrame:
    """Creates a DataFrame of Hashtags."""
    return df.select(explode(col("entities.hashtags.text")).alias("hashtags"))


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
    """Creates a DataFrame of Animated GIF URLs."""
    return df.where(
        col("extended_entities").isNotNull()  # Notice the parentheses after isNotNull()
        & col("extended_entities.media").isNotNull()
        & array_contains(col("extended_entities.media.type"), "animated_gif")
    ).select(
        explode(col("extended_entities.media.media_url_https")).alias(
            "animated_gif_url"
        )
    )


def imageUrls(df: DataFrame) -> DataFrame:
    """Creates a DataFrame of image URLs."""
    return df.where(
        col("entities.media").isNotNull()
        & array_contains(col("extended_entities.media.type"), "photo")
    ).select(explode(col("entities.media.media_url_https")).alias("image_url"))


def videoUrls(df: DataFrame) -> DataFrame:
    """Creates a DataFrame of video URLs."""
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
        .filter("video_info is not NULL")
        .select(explode(col("video_info")))
        .withColumn("video_url", col("col.url"))
        .drop(col("col"))
    )


def mediaUrls(df: DataFrame) -> DataFrame:
    """Creates a DataFrame of media URLs."""

    animated_gif_urls = df.where(
        col("extended_entities").isNotNull()
        & col("extended_entities.media").isNotNull()
        & array_contains(col("extended_entities.media.type"), "animated_gif")
    ).select(
        explode(col("extended_entities.media.media_url_https")).alias(
            "animated_gif_url"
        )
    )

    image_urls = df.where(
        col("entities.media").isNotNull()
        & array_contains(col("extended_entities.media.type"), "photo")
    ).select(explode(col("entities.media.media_url_https")).alias("image_url"))

    video_urls = (
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
        .filter("video_info is not NULL")
        .select(explode(col("video_info")))
        .withColumn("video_url", col("col.url"))
        .drop(col("col"))
    )

    return animated_gif_urls.union(image_urls).union(video_urls)
