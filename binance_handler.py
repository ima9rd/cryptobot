from binance.client import Client
from binance.websockets import BinanceSocketManager
import datetime
from cred import binance_key, binance_secret


class BinanceStreamListener:

    def __init__(self, listener, update_function, twitter):
        self._binance_key = binance_key
        self._binance_secret = binance_secret
        self.twitter = twitter
        self.update_function = update_function
        self.listener = listener
        self.coin_dict = {coin[0]: coin[1] for coin in self.listener.get_coins()}
        self.client = Client(self._binance_key, self._binance_secret)
        self.bm = None
        self.start_socket()
        self.agg_trade_definition = {
            'E': {'type': int, 'name': 'msg_timestamp'},
            'a': {'type': int, 'name': 'agg_trade_id'},
            'p': {'type': str, 'name': 'price'},
            'q': {'type': str, 'name': 'quantity'},
            'f': {'type': int, 'name': 'first_trade_id'},
            'l': {'type': int, 'name': 'last_trade_id'},
            'T': {'type': int, 'name': 'trade_timestamp'},
            'm': {'type': bool, 'name': 'buyer_was_maker'},
            'M': {'type': bool, 'name': 'best_price_match'}
        }
        self.ticker_definition = {
            'E': {'type': int, 'name': 'msg_timestamp'},
            'p': {'type': str, 'name': 'price_change'},
            'P': {'type': str, 'name': 'price_change_percent'},
            'w': {'type': str, 'name': 'weighted_avg_price'},
            'x': {'type': str, 'name': 'prev_close_price'},
            'c': {'type': str, 'name': 'last_price'},
            'Q': {'type': str, 'name': 'last_qty'},
            'b': {'type': str, 'name': 'bid_price'},
            'B': {'type': str, 'name': 'bid_quantity'},
            'a': {'type': str, 'name': 'ask_price'},
            'A': {'type': str, 'name': 'ask_quantity'},
            'o': {'type': str, 'name': 'open_price'},
            'h': {'type': str, 'name': 'high'},
            'l': {'type': str, 'name': 'low'},
            'v': {'type': str, 'name': 'volume'},
            'q': {'type': str, 'name': 'quote_volume'},
            'O': {'type': int, 'name': 'open_timestamp'},
            'C': {'type': int, 'name': 'close_timestamp'},
            'F': {'type': int, 'name': 'first_trade_id'},
            'L': {'type': int, 'name': 'last_trade_id'},
            'n': {'type': int, 'name': 'count'}
        }
        self.msg_definition = {
                                'stream': {'type': str},
                                'data': {'type': dict,
                                'schema': {
                                    's': {'type': str}
                                }}}

        self.ticker_24hour_definition = {'s': {'type': str}}
        self.agg_msg_count = 0
        self.ticker_msg_count = 0
        self.last_agg_bin = datetime.datetime.now()
        self.last_ticker_bin = datetime.datetime.now()

    def validate_schema(self, msg, schema):
        if type(msg) != dict:
            return False
        schema_keys = schema.keys()
        msg_keys = msg.keys()
        if not set(msg_keys).issuperset(set(schema_keys)):
            return False
        for field in schema_keys:
            if type(msg[field]) != schema[field]['type']:
                return False
        for field in schema:
            if schema[field]['type'] == dict:
                if not self.validate_schema(msg[field], schema[field]['schema']):
                    return False
        return True

    def rebuild_dict(self):
        self.coin_dict = {coin[0]: coin[1] for coin in self.listener.get_coins()}
        self.start_socket()

    def process_msg(self, msg):
        if not self.validate_schema(msg, self.msg_definition):
            print(msg)
            return
        data = msg['data']
        stream = data['s']
        stream = stream.upper()
        if stream not in self.coin_dict.keys():
            print('Unknown coin {0}.'.format(stream))
            return
        else:
            if 'aggTrade' in msg['stream']:
                self.process_agg_trade(msg, stream)
            elif 'ticker' in msg['stream']:
                self.process_ticker_msg(msg, stream)

    def process_agg_trade(self, msg, stream):
        if not self.validate_schema(msg['data'], self.agg_trade_definition):
            return
        data = {'coin_id': self.coin_dict[stream]}
        for symbol in self.agg_trade_definition.keys():
            data[self.agg_trade_definition[symbol]['name']] = msg['data'][symbol]
        self.listener.add_agg_trade(data)
        self.agg_msg_count += 1
        if self.agg_msg_count % 1000 == 0:
            self.last_agg_bin = datetime.datetime.now()
            print('{0} aggregate trade messages processed. Last bin: {1}'.format(self.agg_msg_count, self.last_agg_bin))

    def process_ticker_msg(self, msg, stream):
        if not self.validate_schema(msg['data'], self.ticker_definition):
            return
        data = {'coin_id': self.coin_dict[stream]}
        for symbol in self.ticker_definition.keys():
            data[self.ticker_definition[symbol]['name']] = msg['data'][symbol]
        self.listener.add_ticker(data)
        self.ticker_msg_count += 1
        if self.ticker_msg_count % 1000 == 0:
            self.last_ticker_bin = datetime.datetime.now()
            print('{0} ticker messages processed. Last bin: {1}'.format(self.ticker_msg_count, self.last_ticker_bin))

    def process_ticker(self, msg):
        trade_symbols = []
        for entry in msg:
            if not self.validate_schema(entry, self.ticker_24hour_definition):
                return
            trade_symbols.append(entry['s'])
        ret = []
        for symbol in trade_symbols:
            if symbol not in self.coin_dict.keys() and symbol[-3:] == 'BTC':
                ret.append(symbol)
        if len(ret) > 0:
            self.update_function(self, self.twitter, self.listener, ret)

    def start_socket(self):
        coins = []
        for coin in self.coin_dict.keys():
            coins.append(coin.lower() + '@aggTrade')
            coins.append(coin.lower() + '@ticker')
        if type(self.bm) == BinanceSocketManager:
            self.bm.close()
            del self.bm
        self.bm = BinanceSocketManager(self.client)
        self.bm.start_multiplex_socket(coins, self.process_msg)
        self.bm.start_ticker_socket(self.process_ticker)
        self.bm.start()


if __name__ == '__main__':
    print('This script can not run as a stand alone script - Please run bot.py')

