from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def removeRetweets(df: DataFrame) -> DataFrame:
    """Creates a DataFrame that filters out retweets."""
    return df.filter(col("retweeted_status").isNull())


def removeSensitive(df: DataFrame) -> DataFrame:
    """Creates a DataFrame that filters out sensitive tweets."""
    return df.filter(col("possibly_sensitive").isNull()).filter(
        col("retweeted_status.possibly_sensitive").isNull()
    )


def removeNonVerified(df: DataFrame) -> DataFrame:
    """Creates a DataFrame that filters out tweets from non-verified users."""
    return df.filter(col("user.verified") == True)
