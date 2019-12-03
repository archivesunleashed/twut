from pyspark.sql import DataFrame

class Tweets:
  def __init__(self, sc, sqlContext, tweets):
    self.sc = sc
    self.sqlContext = sqlContext
    self.loader = sc._jvm.io.archivesunleashed.TwutPy(sc._jsc.sc())
    self.tweets = tweets

    def ids(self):
      return DataFrame(self.loader.ids(self.tweets), self.sqlContext)

    def userInfo(self):
      return DataFrame(self.loader.userInfo(self.tweets), self.sqlContext)
