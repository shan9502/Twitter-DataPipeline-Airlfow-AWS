import tweepy
import pandas as pd
import json
from datetime import datetime
import s3fs

def twitter_etl_func():
    #apply twitter api keys here
    access_key = "#####"
    access_secret = "####"
    consumer_key = "####"
    consumer_secret = "####"

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
