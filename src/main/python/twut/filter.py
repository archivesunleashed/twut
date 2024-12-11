from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def removeRetweets(df: DataFrame) -> DataFrame:
    """
    Creates a DataFrame that filters out retweets.

    Handles both v1 and v2 Twitter data by checking for the appropriate field
    indicating a retweet.
    """
    if isV2Data(df):
        return df.filter(col("data.referenced_tweets").isNull())
    else:
        return df.filter(col("retweeted_status").isNull())


def removeSensitive(df: DataFrame) -> DataFrame:
    """Creates a DataFrame that filters out sensitive tweets."""
    return df.filter(col("possibly_sensitive").isNull()).filter(
        col("retweeted_status.possibly_sensitive").isNull()
    )


def removeNonVerified(df: DataFrame) -> DataFrame:
    """Creates a DataFrame that filters out tweets from non-verified users."""
    return df.filter(col("user.verified") == True)
