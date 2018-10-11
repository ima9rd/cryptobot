import twitter_handler
import binance_handler
import database


def update_function(binance, twitter, db, coin):
    db.add_coin(coin)
    twitter.rebuild_dict()
    binance.rebuild_dict()


if __name__ == '__main__':
    db = database.Database(database.base)
    bot = binance_handler.BinanceStreamListener(db, update_function, twitter_handler.TwitterStreamListener(db))
