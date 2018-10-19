# encoding: utf-8
import tornado.ioloop
import redis

from tornado import gen
from tornado.web import RequestHandler, Application
from redis.connection import ConnectionPool
from async_redis.connection import AsyncConnection
from async_redis.client import AsyncRedis


connection_pool = ConnectionPool(AsyncConnection, host='localhost', port=6379, db=0)
async_redis = AsyncRedis(connection_pool=connection_pool)
sync_redis = redis.StrictRedis()


class MainHandler(RequestHandler):
    @gen.coroutine
    def get(self):
        value = yield async_redis.get('my_lock')
        print value
        # pong = yield async_redis.ping()
        # print pong
        #value = sync_redis.get('my_lock')

        self.write(value)


application = Application([
    (r'/', MainHandler),
])

if __name__ == '__main__':
    application.listen(8000)
    try:
        tornado.ioloop.IOLoop.instance().start()
    except KeyboardInterrupt:
        pass
