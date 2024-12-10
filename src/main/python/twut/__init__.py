from pyspark.sql import DataFrame

from twut.filter import (removeNonVerified, removeRetweets, removeSensitive)
from twut.select import (animatedGifUrls, hashtags, ids, imageUrls, language,
                         mediaUrls, sources, text, times, urls, userInfo,
                         videoUrls)


def add_twut_methods():
    """Add methods to DataFrame class."""

    def get_removeRetweets(self):
        return removeRetweets(self)

    def get_removeSensitive(self):
        return removeSensitive(self)

    def get_removeNonVerified(self):
        return removeNonVerified(self)

    def get_ids(self):
        return ids(self)

    def get_language(self):
        return language(self)

    def get_userInfo(self):
        return userInfo(self)

    def get_text(self):
        return text(self)

    def get_times(self):
        return times(self)

    def get_hashtags(self):
        return hashtags(self)

    def get_sources(self):
        return sources(self)

    def get_urls(self):
        return urls(self)

    def get_animatedGifUrls(self):
        return animatedGifUrls(self)

    def get_imageUrls(self):
        return imageUrls(self)

    def get_videoUrls(self):
        return videoUrls(self)

    def get_mediaUrls(self):
        return mediaUrls(self)

    DataFrame.removeRetweets = removeRetweets
    DataFrame.removeSensitive = removeSensitive
    DataFrame.removeNonVerified = removeNonVerified
    DataFrame.ids = get_ids
    DataFrame.language = get_language
    DataFrame.userInfo = get_userInfo
    DataFrame.text = get_text
    DataFrame.times = get_times
    DataFrame.hashtags = get_hashtags
    DataFrame.sources = get_sources
    DataFrame.urls = get_urls
    DataFrame.animatedGifUrls = get_animatedGifUrls
    DataFrame.imageUrls = get_imageUrls
    DataFrame.videoUrls = get_videoUrls
    DataFrame.mediaUrls = get_mediaUrls


add_twut_methods()
