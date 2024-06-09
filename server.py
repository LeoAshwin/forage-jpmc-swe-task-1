import csv
import http.server
import json
import operator
import os.path
import re
import threading
from datetime import timedelta, datetime
from random import normalvariate, random
from socketserver import ThreadingMixIn
import dateutil.parser

# Config
REALTIME = True
SIM_LENGTH = timedelta(days=365 * 5)
MARKET_OPEN = datetime.today().replace(hour=0, minute=30, second=0)

# Market parms
SPD = (2.0, 6.0, 0.1)
PX = (60.0, 150.0, 1)
FREQ = (12, 36, 50)
OVERLAP = 4

def bwalk(min, max, std):
    rng = max - min
    while True:
        max += normalvariate(0, std)
        yield abs((max % (rng * 2)) - rng) + min

def market(t0=MARKET_OPEN):
    for hours, px, spd in zip(bwalk(*FREQ), bwalk(*PX), bwalk(*SPD)):
        yield t0, px, spd
        t0 += timedelta(hours=abs(hours))

def orders(hist):
    for t, px, spd in hist:
        stock = 'ABC' if random() > 0.5 else 'DEF'
        side, d = ('sell', 2) if random() > 0.5 else ('buy', -2)
        order = round(normalvariate(px + (spd / d), spd / OVERLAP), 2)
        size = int(abs(normalvariate(0, 100)))
        yield t, stock, side, order, size

def generate_csv():
    print("Generating CSV...")
    with open('test.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        for t, stock, side, order, size in orders(market()):
            if t > MARKET_OPEN + SIM_LENGTH:
                break
            print(f"Writing row: {t}, {stock}, {side}, {order}, {size}")
            writer.writerow([t, stock, side, order, size])
    print("CSV generated successfully.")

def read_csv():
    print("Reading CSV...")
    with open('test.csv', 'rt') as f:
        for time, stock, side, order, size in csv.reader(f):
            print(f"Read line: {time}, {stock}, {side}, {order}, {size}")
            yield dateutil.parser.parse(time), stock, side, float(order), int(size)
    print("Finished reading CSV.")

def order_book(data, book, stock):
    for t, stock_name, side, order, size in data:
        if stock_name == stock:
            if side == 'buy':
                if t not in book:
                    book[t] = {'bids': [], 'asks': []}
                book[t]['bids'].append((order, size))
            else:
                if t not in book:
                    book[t] = {'bids': [], 'asks': []}
                book[t]['asks'].append((order, size))
            yield t, sorted(book[t]['bids'], key=lambda x: -x[0]), sorted(book[t]['asks'], key=lambda x: x[0])

class ThreadedHTTPServer(ThreadingMixIn, http.server.HTTPServer):
    allow_reuse_address = True

    def shutdown(self):
        self.socket.close()
        http.server.HTTPServer.shutdown(self)

def route(path):
    def _route(f):
        setattr(f, '__route__', path)
        return f
    return _route

def read_params(path):
    query = path.split('?')
    if len(query) > 1:
        query = query[1].split('&')
        return dict(map(lambda x: x.split('='), query))

def get(req_handler, routes):
    for name, handler in routes.__class__.__dict__.items():
        if hasattr(handler, "__route__"):
            if None != re.search(handler.__route__, req_handler.path):
                req_handler.send_response(200)
                req_handler.send_header('Content-Type', 'application/json')
                req_handler.send_header('Access-Control-Allow-Origin', '*')
                req_handler.end_headers()
                params = read_params(req_handler.path)
                data = json.dumps(handler(routes, params)) + '\n'
                req_handler.wfile.write(bytes(data, encoding='utf-8'))
                return

def run(routes, host='0.0.0.0', port=8080):
    class RequestHandler(http.server.BaseHTTPRequestHandler):
        def log_message(self, *args, **kwargs):
            pass

        def do_GET(self):
            get(self, routes)

    server = ThreadedHTTPServer((host, port), RequestHandler)
    thread = threading.Thread(target=server.serve_forever)
    thread.daemon = True
    thread.start()
    print('HTTP server started on port 8080')
    while True:
        from time import sleep
        sleep(1)
    server.shutdown()
    server.start()
    server.waitForThread()

class App(object):
    def __init__(self):
        print("Initializing App...")
        self._book_1 = dict()
        self._book_2 = dict()
        self._data_1 = order_book(read_csv(), self._book_1, 'ABC')
        self._data_2 = order_book(read_csv(), self._book_2, 'DEF')
        self._rt_start = datetime.now()
        try:
            self._sim_start, _, _ = next(self._data_1)
        except StopIteration:
            print("No data found in the CSV.")
        self.read_10_first_lines()
        print("App initialized successfully.")

    @property
    def _current_book_1(self):
        for t, bids, asks in self._data_1:
            if REALTIME:
                while t > self._sim_start + (datetime.now() - self._rt_start):
                    yield t, bids, asks
            else:
                yield t, bids, asks

    @property
    def _current_book_2(self):
        for t, bids, asks in self._data_2:
            if REALTIME:
                while t > self._sim_start + (datetime.now() - self._rt_start):
                    yield t, bids, asks

    def read_10_first_lines(self):
        for _ in iter(range(10)):
            try:
                next(self._data_1)
                next(self._data_2)
            except StopIteration:
                print("Less than 10 lines in CSV.")

    @route('/query')
    def handle_query(self, x):
        try:
            t1, bids1, asks1 = next(self._current_book_1)
            t2, bids2, asks2 = next(self._current_book_2)
        except Exception as e:
            print("Error getting stocks...reinitializing app")
            self.__init__()
            t1, bids1, asks1 = next(self._current_book_1)
            t2, bids2, asks2 = next(self._current_book_2)
        t = t1 if t1 > t2 else t2
        print('Query received @ t%s' % t)
        return [{
            'id': x and x.get('id', None),
            'stock': 'ABC',
            'timestamp': str(t),
            'top_bid': bids1 and {
                'price': bids1[0][0],
                'size': bids1[0][1]
            },
            'top_ask': asks1 and {
                'price': asks1[0][0],
                'size': asks1[0][1]
            }
        },
            {
                'id': x and x.get('id', None),
                'stock': 'DEF',
                'timestamp': str(t),
                'top_bid': bids2 and {
                    'price': bids2[0][0],
                    'size': bids2[0][1]
                },
                'top_ask': asks2 and {
                    'price': asks2[0][0],
                    'size': asks2[0][1]
                }
            }]

if __name__ == '__main__':
    if not os.path.isfile('test.csv'):
        print("No data found, generating...")
        generate_csv()
    run(App())
