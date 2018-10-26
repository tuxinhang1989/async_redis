# encoding: utf-8
import tornado.ioloop
import redis

from tornado import gen
from tornado.web import RequestHandler, Application
from redis.connection import ConnectionPool
from async_redis.connection import AsyncConnection
from async_redis.client import AsyncRedis


connection_pool = ConnectionPool(AsyncConnection, host='localhost', port=6379, db=0, socket_connect_timeout=0.00000001)
async_redis = AsyncRedis(connection_pool=connection_pool)
sync_redis = redis.StrictRedis()


class MainHandler(RequestHandler):
    @gen.coroutine
    def get(self):
        key1, value1 = 'my_key1', 'value1'
        key2, value2 = 'my_key2', 'value2'
        key3, value3 = 'my_key3', 'value3'

        with async_redis.pipeline() as p:
            p.set(key1, value1)
            p.set(key2, value2)
            p.set(key3, value3)
            p.get(key1)
            p.get(key2)
            p.get(key3)
            result = yield p.execute()
        print result
        assert result[0] is True
        assert result[1] is True
        assert result[2] is True
        assert result[3] == value1
        assert result[4] == value2
        assert result[5] == value3

        # try:
        #     value1 = yield async_redis.get(key)
        # except redis.ConnectionError:
        #     value1 = 'connectionError'

        self.write(value1)


application = Application([
    (r'/', MainHandler),
])

if __name__ == '__main__':
    application.listen(8000)
    try:
        tornado.ioloop.IOLoop.instance().start()
    except KeyboardInterrupt:
        pass
