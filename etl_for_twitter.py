import tweepy
import pandas as pd
import json
from datetime import datetime
import s3fs

def twitter_etl_func():
    access_key = "XAgXR5hATrffcc40l5c5tCjIK"
    access_secret = "89LI2NuRpYQ0i3E2P14vmw4tDC1cdqYt1MxDvJsOiHSpoQRhKH"
    consumer_key = "1106977853579497472-oTJFxdOYyUnjX4gjjOyRIXZ2ZmCO0I"
    consumer_secret = "ywNzbfciRYlRfOxOfBT7oKfb4hr4bPSc08jRYx7lgP4gJ"

    # Twitter authentication
    auth = tweepy.OAuthHandler(access_key, access_secret)
    auth.set_access_token(consumer_key, consumer_secret)

    #Creating object for API
    api = tweepy.API(auth)

    tweets = api.user_timeline(screen_name='@MKBHD', 
                                # 200 - max allowed count
                                count=5, 
                                include_rts = False,
                                # necessary to keep full text , otherwise only the first 140 words are shown
                                tweet_mode='extended')

    tweets_li=[]
    for tweet in tweets:
        txt = tweet._json["full_text"]

        tweet_classifications={
        "user":tweet.user.screen_name,
        "text":txt,
        "favorite_count":tweet.favorite_count,
        "retweet_count":tweet.retweet_count,
        "created_at":tweet.created_at
        }

        tweet_li.append(tweet_classifications)

    df = pd.DataFrame(tweet_li)
    df.to_csv("s3://####yours3bucketurl###/MKBHD_tweets.csv")