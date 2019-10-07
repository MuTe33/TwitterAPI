from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from textblob import TextBlob
import simplejson as json

# Class Imports
import DBconnection as dbcon
import Twitter_Account as ta

# Authenticator
class TwitterAuthentification():
    """
    This class authenticates to the Twitter API with CONSUMER and ACCESS Tokens
    """
    def __init__(self):
        self.auth = OAuthHandler(ta.CONSUMER_KEY, ta.CONSUMER_SECRET)
        self.auth.set_access_token(ta.ACCESS_TOKEN, ta.ACCESS_TOKEN_SECRET)

    def getAuth(self):
        return self.auth

#Tweet streamer
class TwitterStreamer():
    """
    This class is streaming the tweets
    """
    def stream_tweets(self, auth, hash_tag_list, db_name):
        listener = TwitterListener()
        listener.createDBConnection(db_name)
        stream = Stream(auth, listener)
        stream.filter(track=hash_tag_list)

# Tweet Listener
class TwitterListener(StreamListener):
    """
    This class is a listener - listening for life Tweets, printing and storing in the database
    """
    def createDBConnection(self, dbname):
        try:
            self.db = dbcon.MongoDBconnection(dbname)
            print("Database connection successful.")
            print("")
        except BaseException as e:
            print("Program terminated - Error while connecting to the database: " + str(e))
            exit(-1)

    def on_connect(self):
        print("You're now connected to the Twitter Streaming API.")
        print("")

    def on_status(self, status):
        str_created_at = str(status.created_at)
        str_tweet = status.text
        str_author = status.author.screen_name

        # polarity [-1.0, 1.0] --> -1.0 negative, 0.0 neutral, 1.0 positive
        str_sentiment_polarity = str(TextBlob(str_tweet).sentiment.polarity)
        # subjectivity [0.0, 1.0] --> 0.0 very objective, 1.0 very subjective
        str_sentiment_subjectivity = str(TextBlob(str_tweet).sentiment.subjectivity)

        try:
            # convert to JSON Format
            raw_data = "[{'created_at'" + ":'" + str_created_at + "', 'Tweet'" + ":'" + str_tweet + "', 'Author'" + ":'" + str_author + "', 'Polarity'" + ":'" + str_sentiment_polarity + "', 'Subjectivity'" + ":'" + str_sentiment_subjectivity + "'}]"
            data = json.loads(raw_data.replace("'", '"'))
            # Insert into Database and print
            self.db.insertInDB("twitter", data)
            print(data)
        except BaseException as e:
            print("skipped: " + e.message)

    def on_error(self, status):
        if status == 420:
            # Return False on_data method in case tweet limit occurs
            return False
        print(status)


if __name__ == '__main__':
    database_name = 'bitcointest'
    access = TwitterAuthentification()

    hash_tag_list = ['Bitcoin', 'bitcoin']

    streamer = TwitterStreamer()
    streamer.stream_tweets(access.getAuth(), hash_tag_list, database_name)
