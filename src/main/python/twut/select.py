from pyspark.sql import DataFrame
from pyspark.sql.functions import array_contains, col, explode
from pyspark.sql.utils import AnalysisException


class SelectTweet:
    def __init__(self, sc, sqlContext, df):
        self.sc = sc
        self.sqlContext = sqlContext
        self.df = df

    def has_column(df, col):
        try:
            df[col]
            return True
        except AnalysisException:
            return False

    def ids(df):
        return df.select("id_str")

    def userInfo(df):
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

    def text(df):
        if SelectTweet.has_column(df, "full_text"):
            return df.select("full_text", "text")
        else:
            return df.select("text")

    def times(df):
        return df.select("created_at")

    def hashtags(df):
        return df.select(explode(col("entities.hashtags.text")).alias("hashtags"))

    def sources(df):
        return df.select("source")

    def urls(df):
        tweetUrls = df.select(explode(col("entities.urls.url")).alias("url"))

        expandedUrls = df.select(
            explode(col("entities.urls.expanded_url")).alias("expanded_url")
        )

        return tweetUrls.union(expandedUrls)

    def animatedGifUrls(df):
        return df.where(
            col("extended_entities").isNotNull
            and col("extended_entities.media").isNotNull
            and array_contains(col("extended_entities.media.type"), "animated_gif")
        ).select(
            explode(col("extended_entities.media.media_url_https")).alias(
                "animated_gif_url"
            )
        )

    def imageUrls(df):
        return df.where(
            col("entities.media").isNotNull
            and array_contains(col("extended_entities.media.type"), "photo")
        ).select(explode(col("entities.media.media_url_https")).alias("image_url"))

    def videoUrls(df):
        return (
            df.where(
                col("extended_entities").isNotNull
                and col("extended_entities.media").isNotNull
                and col("extended_entities.media.video_info").isNotNull
                and array_contains(col("extended_entities.media.type"), "video")
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

    def mediaUrls(df):
        animated_gif_urls = df.where(
            col("entities.media").isNotNull
            and array_contains(col("extended_entities.media.type"), "photo")
        ).select(explode(col("entities.media.media_url_https")).alias("image_url"))

        image_urls = df.where(
            col("entities.media").isNotNull
            and array_contains(col("extended_entities.media.type"), "photo")
        ).select(explode(col("entities.media.media_url_https")).alias("image_url"))

        video_urls = (
            df.where(
                col("extended_entities").isNotNull
                and col("extended_entities.media").isNotNull
                and col("extended_entities.media.video_info").isNotNull
                and array_contains(col("extended_entities.media.type"), "video")
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

        return image_urls.union(video_urls.union(animated_gif_urls))
