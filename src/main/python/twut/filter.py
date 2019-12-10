from pyspark.sql import DataFrame
from pyspark.sql.functions import array_contains, col, explode


class FilterTweet:
    def __init__(self, sc, sqlContext, df):
        self.sc = sc
        self.sqlContext = sqlContext
        self.df = df

    def removeRetweets(df):
        return df.filter("retweeted_status is NULL")

    def removeSensitive(df):
        return df.filter("possibly_sensitive is NULL").filter(
            "retweeted_status.possibly_sensitive is NULL"
        )

    def removeNonVerified(df):
        return df.filter(df["user.verified"] == "true")
