import tweepy
import textblob
import json
from datetime import datetime


class TwitterStreamListener(tweepy.StreamListener):
    def __init__(self, listener=None):
        self.twitter_secrets = {
            'consumer_key': 'key'
            , 'consumer_secret': 'secret'
            , 'access_token': 'token'
            , 'access_token_secret': 'token_secret'
        }
        self.listener = listener
        self.coin_dict = {}
        self.rebuild_dict(True)
        self.auth = None
        self.stream = None
        self.banned_users = []
        self.connect()
        self.subj_filter = 0.3

    def connect(self):
        auth = tweepy.OAuthHandler(self.twitter_secrets['consumer_key'], self.twitter_secrets['consumer_secret'])
        auth.set_access_token(self.twitter_secrets['access_token'], self.twitter_secrets['access_token_secret'])
        self.auth = auth
        self.stream = tweepy.Stream(self.auth, self)
        track = ['$' + x for x in self.coin_dict.keys()]
        self.stream.filter(languages=['en'], track=track, async=True)

    def reconnect(self):
        if self.stream.running:
            self.stream.disconnect()
        track = ['$' + x for x in self.coin_dict.keys()]
        self.stream.filter(track=track, async=True)

    def rebuild_dict(self, init=False):
        self.coin_dict = {coin[0][:-3]: coin[1] for coin in self.listener.get_coins()}
        if not init:
            self.reconnect()

    def on_data(self, data):
        js = json.loads(data)
        if 'limit' not in js.keys():
            found = set()
            for coin in self.coin_dict.keys():
                if coin in js['text']:
                    found.add(coin)
            if len(found) == 0:
                return
            tb = textblob.TextBlob(js['text']).sentiment
            if tb[1] < self.subj_filter:
                return
            time = ''.join(js['created_at'].split('+0000 '))
            dt = datetime.strptime(time, "%a %b %d %H:%M:%S %Y")
            for coin in found:
                self.listener.add_sentiment(self.coin_dict[coin], tb[0], tb[1], dt, 1)

    def on_status(self, status):
        pass

    def on_error(self, status_code):
        pass

    def ban_user(self, user):
        if user in self.banned_users:
            pass
        else:
            self.banned_users.append(user)


if __name__ == '__main__':
    stream = TwitterStreamListener()