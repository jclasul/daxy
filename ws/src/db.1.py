from pymongo import MongoClient
import datetime
import time
import gdax
import api
import json

api = api.api()
mongo_client = MongoClient(api.mongo) #change for mongo

# specify the database and collection
db = mongo_client.test #change for mongo

class myWebsocketClient(gdax.WebsocketClient):
    def on_open(self):
        self.url = "wss://ws-feed.prime.coinbase.com"
        self.products = ["BTC-USD"]
        self.retries = 0
        self.popcolumns = ['order_id','client_oid','price']

    def _listen(self):
        while not self.stop:
            try:
                start_t = 0
                if time.time() - start_t >= 30:
                    # Set a 30 second ping to keep connection alive
                    self.ws.ping("keepalive")
                    start_t = time.time()
                data = self.ws.recv()
                msg = json.loads(data)
            except ValueError as e:
                self.on_error(e)
                self.retries += 1
                self._disconnect()
            except Exception as e:
                self.on_error(e)
                self.retries += 1
                self._disconnect()
            else:
                self.on_message(msg)

    def on_message(self, msg):
        OT = msg.get('order_type', None)

        if OT == 'market':
            current_time = time.time() 
            msg['funds'] = float(msg.get('funds', 0))
            msg['size'] = float(msg.get('size',0))
            if msg['size']  > 0 and msg['funds'] > 0:
                msg['y'] = msg['funds'] / msg['size']

                if msg['y'] > 0:
                    print(msg['y'])
                    if msg['product_id'] == 'BTC-USD':
                        mongo_collection = db.btcusd
                    elif msg['product_id'] == 'ETH-USD':
                        mongo_collection = db.ethusd
                    elif msg['product_id'] == 'LTC-USD':
                        mongo_collection = db.ltcusd

                    msg['_id'] = msg['order_id']
                    for popcolumn in self.popcolumns:
                        msg.pop(popcolumn, None)

                    msg['sequence'] = int(msg['sequence'])
                    msg['timestamp'] = time.time()  
                    msg['MONGOKEY'] = 'MARKET_UPDATE' 
                    try:
                        #mongo_collection.insert_one(msg)
                        print(msg)
                    except Exception:
                        print('exception in parsing message to mongodb')
                        self.retries += 1
                        self._disconnect()
        
        if self.retries >= 1:
            print('retries greater than 1')
            self._disconnect()

    def _disconnect(self):
        try:
            if self.ws:
                self.ws.close()
        except WebSocketConnectionClosedException as e:
            pass

        self.on_close()

    def on_close(self):
        self.ws.close()
        print('closed')
        time.sleep(30)
        self.retries += 1
        wsClient = myWebsocketClient()
        wsClient.start()
        self.retries = 0
        print('restarted after failure')   

    def on_error(self, e, data=None):
        self.error = e
        print(self.error)
        print('error function called')
        time.sleep(30)
        self._disconnect()

if __name__ == "__main__": 
    wsClient = myWebsocketClient()
    wsClient.start()
    print(wsClient.url, wsClient.products)