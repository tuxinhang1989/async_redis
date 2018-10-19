# encoding: utf-8
from redis.client import StrictRedis
from redis.exceptions import (
    ConnectionError,
    TimeoutError
)
from tornado import gen


class AsyncRedis(StrictRedis):
    @gen.coroutine
    def execute_command(self, *args, **options):
        pool = self.connection_pool
        command_name = args[0]
        connection = pool.get_connection(command_name, **options)
        try:
            yield connection.send_command(*args)
            response = yield self.parse_response(connection, command_name, **options)
            raise gen.Return(response)
        except (ConnectionError, TimeoutError) as e:
            print e
            connection.disconnect()
            if not connection.retry_on_timeout and isinstance(e, TimeoutError):
                raise
            yield connection.send_command(*args)
            response = yield self.parse_response(connection, command_name, **options)
            raise gen.Return(response)
        finally:
            pool.release(connection)

    @gen.coroutine
    def parse_response(self, connection, command_name, **options):
        response = yield connection.read_response()
        if command_name in self.response_callbacks:
            raise gen.Return(self.response_callbacks[command_name](response, **options))
        raise gen.Return(response)
