import json

from textblob import TextBlob
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener

from question4 import twitter_credentials


class twitter_streamer():
    '''
    Class for streaming and processing live tweets.
    '''

    def __init__(self):
        pass

    def stream_tweets(self, fetched_tweets_filename, hashtag_list):
        # This handles Twitter authetification and the connection to Twitter Streaming API
        listener = stdout_listener(fetched_tweets_filename)
        auth = OAuthHandler(twitter_credentials.CONSUMER_KEY, twitter_credentials.CONSUMER_SECRET)
        auth.set_access_token(twitter_credentials.ACCESS_TOKEN, twitter_credentials.ACCESS_TOKEN_SECRET)
        stream = Stream(auth, listener)

        # This line filter Twitter Streams to capture data by the keywords:
        stream.filter(track=hashtag_list)


class stdout_listener(StreamListener):
    '''
    This is a basic listener that just prints received tweets to stdout.
    '''

    def __init__(self, fetched_tweets_filename):
        self.fetched_tweets_filename = fetched_tweets_filename

    def on_data(self, data):
        try:
            # analysis = TextBlob(data)
            # print(analysis.sentiment)
            tweet_data = json.loads(data)
            tweet_text = tweet_data['retweeted_status']['extended_tweet']['full_text']
            analysis = TextBlob(tweet_text).sentiment[0]

            sentiment = ''
            if analysis > 0:
                sentiment = 'Positive'
            elif analysis < 0:
                sentiment = 'Negative'
            else:
                sentiment = 'Neutral'

            print(tweet_text + ' --> ' + sentiment)

            with open(self.fetched_tweets_filename, 'a') as tf:
                tf.write(tweet_text + ' --> ' + sentiment)
            return True
        except BaseException as e:
            print('Error ' + str(e))
        return True

    def on_error(self, status):
        print(status)


if __name__ == '__main__':
    # Authenticate using config.py and connect to Twitter Streaming API.
    hashtag_list = ['donal trump', 'hillary clinton', 'barack obama', 'bernie sanders']
    fetched_tweets_filename = 'tweets.txt'

    twitter_streamer = twitter_streamer()
    twitter_streamer.stream_tweets(fetched_tweets_filename, hashtag_list)
