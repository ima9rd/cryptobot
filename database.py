from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.orm.query import Query
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, create_engine, Float, DateTime, Boolean
import datetime
from cred import db_user, db_password, db_address, db_port, db_name

base = declarative_base()


class SentimentRow(base):
    __tablename__ = 'SentimentValueFact'
    id = Column(Integer, primary_key=True)
    coin_id = Column(Integer)
    sentiment_value = Column(Float)
    subjectivity_value = Column(Float)
    source_dttm = Column(DateTime)
    source = Column(Integer)


class CoinDimRow(base):
    __tablename__ = 'CoinDim'
    id = Column(Integer, primary_key=True)
    trade_symbol = Column(String(10))


class AggTradeRow(base):
    __tablename__ = 'AggTradeValueFact'
    id = Column(Integer, primary_key=True)
    coin_id = Column(Integer)
    msg_timestamp = Column(DateTime)
    agg_trade_id = Column(Integer)
    price = Column(Float)
    quantity = Column(Float)
    first_trade_id = Column(Integer)
    last_trade_id = Column(Integer)
    trade_timestamp = Column(DateTime)
    buyer_was_maker = Column(Boolean)
    best_price_match = Column(Boolean)


class TickerRow(base):
    __tablename__ = 'TickerValueFact'
    id = Column(Integer, primary_key=True)
    coin_id = Column(Integer)
    msg_timestamp = Column(DateTime)
    price_change = Column(Float)
    price_change_percent = Column(Float)
    weighted_avg_price = Column(Float)
    prev_close_price = Column(Float)
    last_price = Column(Float)
    last_qty = Column(Float)
    bid_price = Column(Float)
    bid_quantity = Column(Float)
    ask_price = Column(Float)
    ask_quantity = Column(Float)
    open_price = Column(Float)
    high = Column(Float)
    low = Column(Float)
    volume = Column(Float)
    quote_volume = Column(Float)
    open_timestamp = Column(DateTime)
    close_timestamp = Column(DateTime)
    first_trade_id = Column(Integer)
    last_trade_id = Column(Integer)
    count = Column(Integer)

class Database:

    def __init__(self, base):
        self.base = base
        self.db_name ='mysql+pymysql://{0}:{1}@{2}:{3}/{4}'.format(db_user, db_password, db_address, db_port, db_name)
        self.engine = create_engine(self.db_name, pool_recycle=3600)
        self.DBSession = scoped_session(sessionmaker())
        self.DBSession.configure(bind=self.engine, autoflush=False, expire_on_commit=False)

    def add_sentiment(self, coin_id, subjectivity_value, sentiment_value, source_dttm, source):
        row = SentimentRow()
        row.coin_id = coin_id
        row.subjectivity_value = subjectivity_value
        row.sentiment_value = sentiment_value
        row.source_dttm = source_dttm
        row.source = source
        self.DBSession.add(row)
        self.DBSession.commit()

    def binance_timestamp(self, timestamp):
        return datetime.datetime.utcfromtimestamp(int(str(timestamp)[:-3]))

    def add_agg_trade(self, data):
        row = AggTradeRow()
        row.coin_id = int(data['coin_id'])
        row.msg_timestamp = self.binance_timestamp(data['msg_timestamp'])
        row.agg_trade_id = int(data['agg_trade_id'])
        row.price = float(data['price'])
        row.quantity = float(data['quantity'])
        row.first_trade_id = int(data['first_trade_id'])
        row.last_trade_id = int(data['last_trade_id'])
        row.trade_timestamp = self.binance_timestamp(data['trade_timestamp'])
        row.buyer_was_maker = bool(data['buyer_was_maker'])
        row.best_price_match = bool(data['best_price_match'])
        self.DBSession.add(row)
        self.DBSession.commit()

    def add_ticker(self, data):
        row = TickerRow()
        row.coin_id = int(data['coin_id'])
        row.msg_timestamp = self.binance_timestamp(data['msg_timestamp'])
        row.price_change = float(data['price_change'])
        row.price_change_percent = float(data['price_change_percent'])
        row.weighted_avg_price = float(data['weighted_avg_price'])
        row.prev_close_price = float(data['prev_close_price'])
        row.last_price = float(data['last_price'])
        row.last_qty = float(data['last_qty'])
        row.bid_price = float(data['bid_price'])
        row.bid_quantity = float(data['bid_quantity'])
        row.ask_price = float(data['ask_price'])
        row.ask_quantity = float(data['ask_quantity'])
        row.open_price = float(data['open_price'])
        row.high = float(data['high'])
        row.low = float(data['low'])
        row.volume = float(data['volume'])
        row.quote_volume = float(data['quote_volume'])
        row.open_timestamp = self.binance_timestamp(data['open_timestamp'])
        row.close_timestamp = self.binance_timestamp(data['close_timestamp'])
        row.first_trade_id = int(data['first_trade_id'])
        row.last_trade_id = int(data['last_trade_id'])
        row.count = int(data['count'])
        self.DBSession.add(row)
        self.DBSession.commit()

    def add_coin(self, data):
        query = Query(CoinDimRow, session=self.DBSession())
        query.add_columns(CoinDimRow.trade_symbol)
        res = query.all()
        res = list(set([coin.trade_symbol for coin in res]))
        for coin in data:
            if coin.strip() in res:
                pass
            else:
                row = CoinDimRow()
                row.trade_symbol = coin.strip()
                self.DBSession.add(row)
                self.DBSession.commit()

    def create_tables(self):
        self.base.metadata.drop_all(self.engine)
        self.base.metadata.create_all(self.engine)

    def get_coins(self):
        query = Query(CoinDimRow, session=self.DBSession())
        query.add_columns(CoinDimRow.trade_symbol, CoinDimRow.id)
        res = query.all()
        return[[coin.trade_symbol, coin.id] for coin in res]


if __name__ == '__main__':
    print('This script can not run as a stand alone script - Please run bot.py')

