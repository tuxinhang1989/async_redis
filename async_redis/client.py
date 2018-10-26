# encoding: utf-8
import sys
import hashlib

from itertools import chain, izip, imap
from redis.client import StrictRedis
from redis._compat import safe_unicode
from redis.exceptions import (
    ConnectionError,
    TimeoutError,
    RedisError,
    ResponseError,
    ExecAbortError,
    WatchError,
    NoScriptError
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

    @gen.coroutine
    def transaction(self, func, *watches, **kwargs):
        shard_hint = kwargs.pop('shard_hint', None)
        value_from_callable = kwargs.pop('value_from_callable', False)
        watch_delay = kwargs.pop('watch_delay', None)
        with self.pipeline(True, shard_hint) as pipe:
            while 1:
                try:
                    if watches:
                        yield pipe.watch(*watches)
                    func_value = func(pipe)
                    exec_value = yield pipe.execute()
                    if value_from_callable:
                        raise gen.Return(func_value)
                    else:
                        raise gen.Return(exec_value)
                except WatchError:
                    if watch_delay is not None and watch_delay > 0:
                        yield gen.sleep(watch_delay)

    def pipeline(self, transaction=True, shard_hint=None):
        return AsyncPipeline(
            self.connection_pool,
            self.response_callbacks,
            transaction,
            shard_hint
        )


class BasePipeline(object):
    UNWATCH_COMMANDS = {'DISCARD', 'EXEC', 'UNWATCH'}

    def __init__(self, connection_pool, response_callbacks, transaction, shard_hint):
        self.connection_pool = connection_pool
        self.connection = None
        self.response_callbacks = response_callbacks
        self.transaction = transaction
        self.shard_hint = shard_hint

        self.watching = False
        self.reset()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.reset()

    def __del__(self):
        try:
            self.reset()
        except Exception:
            pass

    def __len__(self):
        return len(self.command_stack)

    def reset(self):
        self.command_stack = []
        self.scripts = set()
        connection = self.connection
        if self.watching and connection:
            def handle_read_future(future):
                exc = future.exception()
                if isinstance(exc, ConnectionError):
                    connection.disconnct()

            def handle_send_future(future):
                exc = future.exception()
                if isinstance(exc, ConnectionError):
                    connection.disconnct()
                    return
                read_future = connection.read_response()
                read_future.add_done_callback(handle_read_future)

            unwatch_future = connection.send_command('UNWATCH')
            unwatch_future.add_done_callback(handle_send_future)

        self.watching = False
        self.explicit_transaction = False
        if self.connection:
            self.connection_pool.release(self.connection)
            self.connection = None

    def multi(self):
        if self.explicit_transaction:
            raise RedisError('Cannot issue nested calls to MULTI')
        if self.command_stack:
            raise RedisError('Commands without an initial WATCH have already been issued')
        self.explicit_transaction = True

    @gen.coroutine
    def execute_command(self, *args, **kwargs):
        if (self.watching or args[0] == 'WATCH') and not self.explicit_transaction:
            response = yield self.immediate_execute_command(*args, **kwargs)
            raise gen.Return(response)
        raise gen.Return(self.pipeline_execute_command(*args, **kwargs))

    @gen.coroutine
    def immediate_execute_command(self, *args, **options):
        command_name = args[0]
        conn = self.connection
        if not conn:
            conn = self.connection_pool.get_connection(command_name, self.shard_hint)
            self.connection = conn
        try:
            yield conn.send_command(*args)
            response = yield self.parse_response(conn, command_name, **options)
            raise gen.Return(response)
        except (ConnectionError, TimeoutError) as e:
            conn.disconnect()
            if not conn.retry_on_timeout and isinstance(e, TimeoutError):
                raise
            try:
                if not self.watching:
                    yield conn.send_command(*args)
                    response = yield self.parse_response(conn, command_name, **options)
                    raise gen.Return(response)
            except ConnectionError:
                conn.disconnect()
                self.reset()
                raise

    def pipeline_execute_command(self, *args, **options):
        self.command_stack.append((args, options))
        return self

    @gen.coroutine
    def _execute_transaction(self, connection, commands, raise_on_error):
        cmds = chain([(('MULTI',), {})], commands, [(('EXEC',), {})])
        all_cmds = connection.pack_commands([args for args, _ in cmds])
        yield connection.send_packed_command(all_cmds)
        errors = []
        # parse MULTI
        try:
            yield self.parse_response(connection, '_')
        except ResponseError:
            errors.append((0, sys.exc_info()[1]))

        # parse commands
        for i, command in enumerate(commands):
            try:
                yield self.parse_response(connection, '_')
            except ResponseError:
                ex = sys.exc_info()[1]
                self.annotate_exception(ex, i+1, command[0])
                errors.append((i, ex))

        # parse EXEC
        try:
            response = yield self.parse_response(connection, '_')
        except ExecAbortError:
            if self.explicit_transaction:
                yield self.immediate_execute_command('DISCARD')
            if errors:
                raise errors[0][1]
            raise sys.exc_info()[1]

        if response is None:
            raise WatchError("Watched variable changed.")

        for i, e in errors:
            response.insert(i, e)

        if len(response) != len(commands):
            self.connection.disconnect()
            raise ResponseError("Wrong number of response items from "
                                "pipeline execution")

        if raise_on_error:
            self.raise_first_error(commands, response)

        # We have to run response callbacks manually
        data = []
        for r, cmd in izip(response, commands):
            if not isinstance(r, Exception):
                args, options = cmd
                command_name = args[0]
                if command_name in self.response_callbacks:
                    r = self.response_callbacks[command_name](r, **options)
            data.append(r)
        raise gen.Return(data)

    @gen.coroutine
    def _execute_pipeline(self, connection, commands, raise_on_error):
        all_cmds = connection.pack_commands([args for args, _ in commands])
        yield connection.send_packed_command(all_cmds)

        responses = []
        for args, options in commands:
            try:
                response = yield self.parse_response(connection, args[0], **options)
                responses.append(response)
            except ResponseError:
                responses.append(sys.exc_info()[1])

        if raise_on_error:
            self.raise_first_error(commands, responses)
        raise gen.Return(responses)

    def raise_first_error(self, commands, response):
        for i, r in enumerate(response):
            if isinstance(r, ResponseError):
                self.annotate_exception(r, i+1, commands[i][0])
                raise r

    def annotate_exception(self, exception, number, command):
        cmd = safe_unicode(' ').join(imap(safe_unicode, command))
        msg = unicode('Command # %d (%s) of pipeline caused error: %s') % (
            number, cmd, safe_unicode(exception.args[0])
        )
        exception.args = (msg,) + exception.args[1:]

    @gen.coroutine
    def parse_response(self, connection, command_name, **options):
        result = yield AsyncRedis.parse_response(self, connection, command_name, **options)
        if command_name in self.UNWATCH_COMMANDS:
            self.watching = False
        elif command_name == 'WATCH':
            self.watching = True
        raise gen.Return(result)

    @gen.coroutine
    def load_scripts(self):
        scripts = list(self.scripts)
        immediate = self.immediate_execute_command
        shas = [s.sha for s in scripts]
        exists = yield immediate('SCRIPT EXISTS', *shas)
        if not all(exists):
            for s, exist in izip(scripts, exists):
                if not exist:
                    s.sha = yield immediate('SCRIPT LOAD', s.script)

    @gen.coroutine
    def execute(self, raise_on_error=True):
        stack = self.command_stack
        if not stack:
            raise gen.Return([])
        if self.scripts:
            yield self.load_scripts()
        if self.transaction or self.explicit_transaction:
            execute = self._execute_transaction
        else:
            execute = self._execute_pipeline

        conn = self.connection
        if not conn:
            conn = self.connection_pool.get_connection('MULTI', self.shard_hint)
            self.connection = conn

        try:
            result = yield execute(conn, stack, raise_on_error)
            raise gen.Return(result)
        except (ConnectionError, TimeoutError) as e:
            conn.disconnect()
            if not conn.retry_on_timeout and isinstance(e, TimeoutError):
                raise
            if self.watching:
                raise WatchError("A ConnectionError occured on while watching "
                                 "one or more keys")
            result = yield execute(conn, stack, raise_on_error)
            raise gen.Return(result)
        finally:
            self.reset()

    @gen.coroutine
    def watch(self, *names):
        if self.explicit_transaction:
            raise RedisError('Cannot issue a WATCH after a MULTI')
        response = yield self.execute_command('WATCH', *names)
        raise gen.Return(response)

    def unwatch(self):
        if self.watching:
            response = yield self.execute_command('UNWATCH')
            raise gen.Return(response)
        else:
            raise gen.Return(True)


class AsyncPipeline(BasePipeline, AsyncRedis):
    "Pipeline for the AsyncRedis class"
    pass


class Script(object):
    "An executable Lua script object returned by ``register_script``"

    def __init__(self, registered_client, script):
        self.registered_client = registered_client
        self.script = script
        if isinstance(script, basestring):
            encoder = registered_client.connection_pool.get_encoder()
            script = encoder.encode(script)
        self.sha = hashlib.sha1(script).hexdigest()

    @gen.coroutine
    def eval_sha(self, keys=[], args=[], client=None):
        if client is None:
            client = self.registered_client
        args = tuple(keys) + tuple(args)
        if isinstance(client, BasePipeline):
            client.scripts.add(self)
        try:
            result = yield client.evalsha(self.sha, len(keys), *args)
            raise gen.Return(result)
        except NoScriptError:
            self.sha = yield client.script_load(self.script)
            result = yield client.evalsha(self.sha, len(keys), *args)
            raise gen.Return(result)
