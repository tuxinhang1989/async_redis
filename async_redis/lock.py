# encoding: utf-8
import uuid
from redis.exceptions import LockError, WatchError
from redis._compat import b
from tornado import gen
from tornado.ioloop import IOLoop


class Lock(object):
    def __init__(self, redis, name, timeout=None, sleep=0.1,
                 blocking=True, blocking_timeout=None, io_loop=None):
        self.io_loop = io_loop or IOLoop.current()
        self.token = None
        self.redis = redis
        self.name = name
        self.timeout = timeout
        self.sleep = sleep
        self.blocking = blocking
        self.blocking_timeout = blocking_timeout
        if self.timeout and self.sleep > self.timeout:
            raise LockError("'sleep' must be less than 'timeout'")

    @gen.coroutine
    def acquire(self, blocking=None, blocking_timeout=None):
        self.token = b(uuid.uuid1().hex)
        sleep = self.sleep
        if blocking is None:
            blocking = self.blocking
        if blocking_timeout is None:
            blocking_timeout = self.blocking_timeout
        stop_trying_at = None
        if blocking_timeout is not None:
            stop_trying_at = self.io_loop.time() + blocking_timeout
        while 1:
            acquired = yield self.do_acquire()
            if acquired:
                raise gen.Return(True)
            if not blocking:
                raise gen.Return(False)
            if stop_trying_at is not None and self.io_loop.time() > stop_trying_at:
                raise gen.Return(False)
            yield gen.sleep(sleep)

    @gen.coroutine
    def do_acquire(self):
        success = yield self.redis.setnx(self.name, self.token)
        if success:
            if self.timeout:
                timeout = int(self.timeout * 1000)
                yield self.redis.pexpire(self.name, timeout)
            raise gen.Return(True)
        raise gen.Return(False)

    @gen.coroutine
    def release(self):
        expected_token = self.token
        if expected_token is None:
            raise LockError("Cannot release an unlocked lock")
        self.token = None
        self.do_release(expected_token)

    @gen.coroutine
    def do_release(self, expected_token):
        lock_value = yield self.redis.get(self.name)
        if lock_value != expected_token:
            raise LockError("Cannot release a lock that's no longer owned")
        yield self.redis.delete(self.name)

    @gen.coroutine
    def extend(self, additional_time):
        if self.token is None:
            raise LockError("Cannot extend an unlocked lock")
        if self.timeout is None:
            raise LockError("Cannot extend a lock with no timeout")
        result = yield self.do_extend(additional_time)
        raise gen.Return(result)

    @gen.coroutine
    def do_extend(self, additional_time):
        pipe = self.redis.pipeline()
        yield pipe.watch(self.name)
        lock_value = yield pipe.get(self.name)
        if lock_value != self.token:
            raise LockError("Cannot extend a lock that's no longer owned")
        expiration = yield pipe.pttl(self.name)
        if expiration is None or expiration < 0:
            expiration = 0
        pipe.multi()
        pipe.pexpire(self.name, expiration + int(additional_time * 1000))

        try:
            response = yield pipe.execute()
        except WatchError:
            raise LockError("Cannot extend a lock that's no longer owned")
        if not response[0]:
            raise LockError("Cannot extend a lock that's no longer owned")
        raise gen.Return(True)


class LuaLock(Lock):
    lua_acquire = None
    lua_release = None
    lua_extend = None

    # KEYS[1] - lock name
    # ARGV[1] - token
    # ARGV[2] - timeout in milliseconds
    # return 1 if lock was acquired, otherwise 0
    LUA_ACQUIRE_SCRIPT = """
        if redis.call('setnx', KEYS[1], ARGV[1]) == 1 then
            if ARGV[2] ~= '' then
                redis.call('pexpire', KEYS[1], ARGV[2])
            end
            return 1
        end
        return 0
    """

    # KEYS[1] - lock name
    # ARGV[1] - token
    # return 1 if the lock was released, otherwise 0
    LUA_RELEASE_SCRIPT = """
        local token = redis.call('get', KEYS[1])
        if not token or token ~= ARGV[1] then
            return 0
        end
        redis.call('del', KEYS[1])
        return 1
    """

    # KEYS[1] - lock name
    # ARGS[1] - token
    # ARGS[2] - additional milliseconds
    # return 1 if the locks time was extended, otherwise 0
    LUA_EXTEND_SCRIPT = """
        local token = redis.call('get', KEYS[1])
        if not token or token ~= ARGV[1] then
            return 0
        end
        local expiration = redis.call('pttl', KEYS[1])
        if not expiration then
            expiration = 0
        end
        if expiration < 0 then
            return 0
        end
        redis.call('pexpire', KEYS[1], expiration + ARGV[2])
        return 1
    """

    def __init__(self, *args, **kwargs):
        super(LuaLock, self).__init__(*args, **kwargs)
        LuaLock.register_scripts(self.redis)

    @classmethod
    def register_scripts(cls, redis):
        if cls.lua_acquire is None:
            cls.lua_acquire = redis.register_script(cls.LUA_ACQUIRE_SCRIPT).eval_sha
        if cls.lua_release is None:
            cls.lua_release = redis.register_script(cls.LUA_RELEASE_SCRIPT).eval_sha
        if cls.lua_extend is None:
            cls.lua_extend = redis.register_script(cls.LUA_EXTEND_SCRIPT).eval_sha

    @gen.coroutine
    def do_acquire(self):
        timeout = self.timeout and int(self.timeout * 1000) or ''
        acquired = yield self.lua_acquire(keys=[self.name],
                                         args=[self.token, timeout],
                                         client=self.redis)
        raise gen.Return(bool(acquired))

    @gen.coroutine
    def do_release(self, expected_token):
        released = yield self.lua_release(keys=[self.name],
                                          args=[expected_token],
                                          client=self.redis)
        if not bool(released):
            raise LockError("Cannot release a lock that's no longer owned")

    @gen.coroutine
    def do_extend(self, additional_time):
        additional_time = int(additional_time * 1000)
        extended = yield self.lua_extend(keys=[self.name],
                                         args=[self.token, additional_time],
                                         client=self.redis)
        if not bool(extended):
            raise LockError("Cannot extend a lock that's no longer owned")
        raise gen.Return(True)
