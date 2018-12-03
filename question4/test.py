import tweepy
from textblob import TextBlob

access_token = '772698281013022720-HoK537yK57ffa98nJdd20SQyFEvGl0B'
access_secret = 'PcpvEbYtJUjEniXNiwK3jD7p7RwcUQpcBFdAsRzpJwAjR'
api_key = 'UG7fHrIuXBg6akW5pcd7sQPb0'
api_secret = 'XO2djG2a86wMv2TT536IEZjUQbCZJAFs0CpVYJ7bKAbQnNiIV5'

auth = tweepy.OAuthHandler(api_key, api_secret)
auth.set_access_token(access_token, access_secret)
api = tweepy.API(auth)

tweets = api.search('Trump')

for tweet in tweets:
    # print(type(tweet))
    # print(tweet.text)
    analysis = TextBlob(tweet.text)
    print(analysis.sentiment)
    print(type(analysis.sentiment[0]))
