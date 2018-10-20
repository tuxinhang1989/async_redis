# encoding: utf-8
import os
import sys
import functools
import socket

from tornado import gen, stack_context
from tornado.ioloop import IOLoop
from tornado.iostream import IOStream, StreamClosedError
from tornado.netutil import Resolver
from redis._compat import (b, xrange, byte_to_chr, bytes,
                           nativestr, imap)
from redis.connection import (
    SYM_STAR,
    SYM_DOLLAR,
    SYM_CRLF,
    SYM_EMPTY,
    SERVER_CLOSED_CONNECTION_ERROR,
    Encoder,
    Token
)
from redis.exceptions import (
    RedisError,
    ConnectionError,
    TimeoutError,
    BusyLoadingError,
    ResponseError,
    InvalidResponse,
    AuthenticationError,
    NoScriptError,
    ExecAbortError,
    ReadOnlyError
)

Resolver.configure('tornado.netutil.ThreadedResolver')


class BaseParser(object):
    EXCEPTION_CLASSES = {
        'ERR': {
            'max number of clients reached': ConnectionError
        },
        'EXECABORT': ExecAbortError,
        'LOADING': BusyLoadingError,
        'NOSCRIPT': NoScriptError,
        'READONLY': ReadOnlyError,
    }

    def parse_error(self, response):
        "Parse an error response"
        error_code = response.split(' ')[0]
        if error_code in self.EXCEPTION_CLASSES:
            response = response[len(error_code) + 1:]
            exception_class = self.EXCEPTION_CLASSES[error_code]
            if isinstance(exception_class, dict):
                exception_class = exception_class.get(response, ResponseError)
            return exception_class(response)
        return ResponseError(response)


class PythonParser(BaseParser):
    def __init__(self):
        self.encoder = None
        self._stream = None
        self.socket_timeout = None

    def __del__(self):
        try:
            self.on_disconnect()
        except Exception:
            pass

    def on_connect(self, connection):
        self._stream = connection._stream
        self.encoder = connection.encoder

    def on_disconnect(self):
        if self._stream is not None:
            self._stream.close()
            self._stream = None
        self.encoder = None

    @gen.coroutine
    def read_response(self):
        try:
            response = yield self._stream.read_until(SYM_CRLF)
        except StreamClosedError:
            raise socket.error(SERVER_CLOSED_CONNECTION_ERROR)
        response = response[:-2]
        if not response:
            raise ConnectionError(SERVER_CLOSED_CONNECTION_ERROR)
        byte, response = byte_to_chr(response[0]), response[1:]
        if byte not in ('-', '+', ':', '$', '*'):
            raise InvalidResponse("Protocol Error: %s, %s" %
                                  (str(byte), str(response)))

        if byte == '-':
            response = nativestr(response)
            error = self.parse_error(response)
            if isinstance(error, ConnectionError):
                raise error
            raise gen.Return(error)
        elif byte == '+':
            pass
        elif byte == ':':
            response = long(response)
        elif byte == '$':
            length = int(response)
            if length == -1:
                raise gen.Return(None)
            response = yield self._stream.read_bytes(length+2)  # make sure to read the '\r\n'
            response = response[:-2]
        elif byte == '*':
            length = int(response)
            if length == -1:
                raise gen.Return(None)
            response = []
            for i in xrange(length):
                part = yield self.read_response()
                response.append(part)
        if isinstance(response, bytes):
            response = self.encoder.decode(response)
        raise gen.Return(response)


class AsyncConnection(object):
    """async redis connection based on tornado"""
    description_format = "AsyncConnection<%(host)s,port=%(port)s,db=%(db)s>"

    def __init__(self, host='localhost', port=6379, db=0, password=None,
                 socket_timeout=None, socket_connect_timeout=None,
                 retry_on_timeout=False, encoding="utf-8",
                 encoding_errors='strict', decode_responses=False,
                 parser_class=PythonParser):
        self.pid = os.getpid()
        self.host = host
        self.port = int(port)
        self.db = db
        self.password = password
        self._timeout = None
        self.io_loop = IOLoop.current()
        self.socket_timeout = socket_timeout
        self.socket_connect_timeout = socket_connect_timeout or socket_timeout
        self.retry_on_timeout = retry_on_timeout
        self.encoder = Encoder(encoding, encoding_errors, decode_responses)
        self._stream = None
        self._parser = parser_class()
        self.resolver = Resolver()
        self._description_args = {
            'host': self.host,
            'port': self.port,
            'db': self.db,
        }
        self._connect_callbacks = []

    def __repr__(self):
        return self.description_format % self._description_args

    def __del__(self):
        try:
            self.disconnect()
        except Exception:
            pass

    def register_connect_callback(self, callback):
        self._connect_callbacks.append(callback)

    def clear_connect_callbacks(self):
        self._connect_callbacks = []

    @gen.coroutine
    def connect(self):
        if self._stream:
            return
        stream = yield self._connect()
        stream.set_nodelay(True)
        self._stream = stream
        try:
            yield self.on_connect()
        except RedisError:
            self.disconnect()
            raise
        for callback in self._connect_callbacks:
            callback(self)

    @gen.coroutine
    def _connect(self):
        addrinfo = yield self.resolver.resolve(self.host, self.port, 0)
        err = None
        for af, addr in addrinfo:
            try:
                s = socket.socket(af)
                self._stream = IOStream(s, io_loop=self.io_loop)
                self._timeout = self.io_loop.add_timeout(
                    self.io_loop.time() + self.socket_connect_timeout,
                    stack_context.wrap(self._on_timeout))
                stream = yield self._stream.connect(addr)
                self._remove_timeout()
                raise gen.Return(stream)
            except (StreamClosedError, socket.error) as _:
                err = _
                self.disconnect()

        if err is not None:
            raise ConnectionError(self._error_message(err))

        raise socket.error("socket.getaddrinfo returned an empty list")

    def _on_timeout(self):
        self._timeout = None
        self.disconnect()

    def _remove_timeout(self):
        if self._timeout is not None:
            self.io_loop.remove_timeout(self._timeout)
            self._timeout = None

    def _error_message(self, exception):
        if len(exception.args) == 1:
            return "Error connecting to %s:%s. %s." % (self.host, self.port, exception.args[0])
        elif len(exception.args) == 2:
            return "Error %s connecting to %s:%s. %s." % (
                exception.args[0], self.host, self.port, exception.args[1]
            )
        else:
            return "Error connecting to %s:%s" % (self.host, self.port)

    def disconnect(self):
        self._parser.on_disconnect()
        if self._stream is None:
            return
        try:
            self._stream.close()
        except socket.error:
            pass
        self._stream = None

    @gen.coroutine
    def on_connect(self):
        self._parser.on_connect(self)
        if self.password:
            yield self.send_command('AUTH', self.password)
            response = yield self.read_response()
            if nativestr(response) != 'OK':
                raise AuthenticationError('Invalid Password')

        if self.db:
            yield self.send_command('SELECT', self.db)

            response = yield self.read_response()
            if nativestr(response) != 'OK':
                raise ConnectionError('Invalid Database')

    @gen.coroutine
    def read_response(self):
        try:
            if self.socket_timeout:
                self._timeout = self.io_loop.add_timeout(self.io_loop.time() + self.socket_timeout,
                                                         stack_context.wrap(self._on_timeout))
            response = yield self._parser.read_response()
            self._remove_timeout()
        except:
            self.disconnect()
            raise
        if isinstance(response, ResponseError):
            raise response
        raise gen.Return(response)

    @gen.coroutine
    def send_packed_command(self, command):
        if not self._stream:
            yield self.connect()
        if isinstance(command, str):
            command = [command]
        for item in command:
            self._stream.write(item)

    @gen.coroutine
    def send_command(self, *args):
        yield self.send_packed_command(self.pack_command(*args))

    def pack_command(self, *args):
        output = []
        command = args[0]
        if ' ' in command:
            args = tuple([Token.get_token(s)
                          for s in command.split()]) + args[1:]
        else:
            args = (Token.get_token(command),) + args[1:]

        buff = SYM_EMPTY.join((SYM_STAR, b(str(len(args))), SYM_CRLF))

        for arg in imap(self.encoder.encode, args):
            if len(buff) > 6000 or len(arg) > 6000:
                buff = SYM_EMPTY.join(
                    (buff, SYM_DOLLAR, b(str(len(arg))), SYM_CRLF))
                output.append(buff)
                output.append(arg)
                buff = SYM_CRLF
            else:
                buff = SYM_EMPTY.join((buff, SYM_DOLLAR, b(str(len(arg))),
                                      SYM_CRLF, arg, SYM_CRLF))
        output.append(buff)
        return output

    def pack_commands(self, commands):
        output = []
        pieces = []
        buffer_length = 0

        for cmd in commands:
            for chunk in self.pack_command(*cmd):
                pieces.append(chunk)
                buffer_length += len(chunk)

            if buffer_length > 6000:
                output.append(SYM_EMPTY.join(pieces))
                buffer_length = 0
                pieces = []

        if pieces:
            output.append(SYM_EMPTY.join(pieces))
        return output
