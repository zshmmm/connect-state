#!/usr/bin/env python
#coding: utf-8
#
# version 1.0
#
# zshmmm@163.com
#
from __future__ import division
import os
import sys
import time
import datetime
import socket
import select
try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO
from itertools import imap, izip
def pairs_to_dict(response):
    """
    将一个列表或元组转换为 python dict
    ['a', 1, 'b', 2] ==> {'a': 1, 'b': 2}
    """
    it = iter(response)
    return dict(izip(it, it))
def make_color(code):
    def decorator(func):
        def color_func(s):
            if not sys.stdout.isatty():
                return func(s)
            tpl = '\x1b[{}m{}\x1b[0m'
            return tpl.format(code, func(s))
        return color_func
    return decorator
@make_color(36)
def fmta(s):
    return '{:^7}'.format(str(float(s) * 1000)[:5] + 'ms')
@make_color(36)
def fmtb(s):
    return '{:<7}'.format(str(float(s) * 1000)[:5] + 'ms')
def wrapper_timer(wrapper=True, **wkwargs):
    def decorator(func):
        def wrapper(ins, *args, **kwargs):
            response = func(ins, *args, **kwargs)
            try:
                response = str(response)[:100]
            except:
                raise
            if wrapper:
                kwargs = {
                    'response': response,
                    'client' : wkwargs.pop('client')
                }
                for k, v in wkwargs.iteritems():
                    kwargs[k] = getattr(ins, v)
                ins.format_timer = TimerTemplate.format_timer(**kwargs)
            return response
        return wrapper
    return decorator
class TimerTemplate(object):
    """
    输出字符串模板类
    """
    REDIS_TEMPLATE = """
> {redis_url}
{response}
  DNS Lookup   TCP Connection   AUTH Processing   Server Processing   Content Transfer
[   {a0000}  |     {a0001}    |     {a0002}     |      {a0003}      |      {a0004}     ]
             |                |                 |                   |                  |
    namelookup:{b0000}        |                 |                   |                  |
                        connect:{b0001}         |                   |                  |
                                      pretransfer:{b0002}           |                  |
                                                        starttransfer:{b0003}          |
                                                                                 total:{b0004}
"""[1:]
    @classmethod
    def format_timer(cls, client='redis', timer={}, **kwargs):
        if not hasattr(cls, client.upper() + '_TEMPLATE'):
            raise Exception('No such template: %s' % client)
        at = {
            'a0000' : fmta(timer.get('dns_lookup_timer', 0)),
            'a0001' : fmta(timer.get('connect_timer', 0)),
            'a0002' : fmta(timer.get('auth_timer', 0)),
            'a0003' : fmta(timer.get('server_process_timer', 0)),
            'a0004' : fmta(timer.get('content_trans_timer', 0)),
        }
        bt = {
            'b0000' : fmtb(timer.get('dns_lookup_timer', 0)),
            'b0001' : fmtb(timer.get('dns_lookup_timer', 0) + 
                           timer.get('connect_timer', 0)),
            'b0002' : fmtb(timer.get('dns_lookup_timer', 0) + 
                           timer.get('connect_timer', 0) + 
                           timer.get('auth_timer', 0)),
            'b0003' : fmtb(timer.get('dns_lookup_timer', 0) + 
                           timer.get('connect_timer', 0) + 
                           timer.get('auth_timer', 0) + 
                           timer.get('server_process_timer', 0)),
            'b0004' : fmtb(timer.get('dns_lookup_timer', 0) + 
                           timer.get('connect_timer', 0) + 
                           timer.get('auth_timer', 0) + 
                           timer.get('server_process_timer', 0) + 
                           timer.get('content_trans_timer', 0)),
        }
        kwargs.update(at)
        kwargs.update(bt)
        return cls.REDIS_TEMPLATE.format(**kwargs)
class Connect(object):
    """
    Socket 连接管理类。
    """
    def __init__(self,
                 host='localhost',
                 port=6379,
                 socket_type=socket.SOCK_STREAM, 
                 socket_connect_timeout=10,
                 socket_read_size=65536,
                 crlf='\n'):
        self.host = self.remote_ip = host
        self.port = int(port)
        self.socket_type = socket_type
        self.socket_connect_timeout = socket_connect_timeout
        self.socket_read_size = socket_read_size
        self.socket = None
        self._buffer = StringIO()
        self.bytes_written = 0
        self.bytes_read = 0
        self.crlf = crlf
        self.timer = None        
        self.reset_timer()
        self._connect()
    @property
    def buffer_length(self):
        return self.bytes_written - self.bytes_read
    def buffer_purge(self):
        self._buffer.seek(0)
        self._buffer.truncate()
        self.bytes_written = 0
        self.bytes_read = 0
    def buffer_close(self):
        try:
            self.purge()
            self._buffer.close()
        except:
            pass
        self._buffer = None
        self.socket = None
        self.reset_timer()
    def reset_timer(self):
        self.curr_timer = {
            'dns_time_s' : 0,
            'dns_time_e' : 0,
            'connect_time_s' : 0,
            'connect_time_e': 0,
            'send_time_s' : 0,
            'send_time_e' : 0,
            'read_time_s' : 0,
            'read_time_e' : 0,
        }
        self.timer = {
            'dns_lookup_timer' : 0,
            'connect_timer' : 0,
            'server_process_timer' : 0,
            'content_trans_timer' : 0,
        }
    def is_ipv4_address(self):
        """
        判断一个地址是否为 IPV4 地址
        """
        try:
            socket.inet_aton(self.host)
        except socket.error:
            return False
        return True
    def dns_lookup(self):
        self.curr_timer['dns_time_s'] = time.time()
        self.remote_ip = socket.gethostbyname(self.host)
        self.curr_timer['dns_time_e'] = time.time()
        self.timer['dns_lookup_timer'] = self.curr_timer['dns_time_e'] - \
                                        self.curr_timer['dns_time_s']
    def _connect(self):
        if not self.is_ipv4_address():
            self.dns_lookup()
        self.socket = socket.socket(socket.AF_INET, self.socket_type)
        self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.socket.settimeout(self.socket_connect_timeout)
        try:
            self.curr_timer['connect_time_s'] = time.time()
            self.socket.connect((self.remote_ip , self.port))
            self.curr_timer['connect_time_e'] = time.time()
            self.timer['connect_timer'] = self.curr_timer['connect_time_e'] - \
                                        self.curr_timer['connect_time_s']
        except:
            self.disconnect()
            raise
    def disconnect(self):
        if self.socket is None:
            return
        try:
            self.socket.shutdown(socket.SHUT_RDWR)
            self.socket.close()
        except socket.error:
            pass
        self.socket = None
        self.buffer_close()
        self.reset_timer()
    def send_data(self, data):
        try:
            if isinstance(data, str):
                data = [data]
            self.curr_timer['send_time_s'] = time.time()
            for item in data:
                self.socket.sendall(item)
            self.curr_timer['send_time_e'] = time.time()
            if bool(select.select([self.socket], [], [], self.socket_connect_timeout)[0]):
                self.timer['server_process_timer'] = time.time() - self.curr_timer['send_time_s']
        except:
            self.disconnect()
            raise
    def read_from_socket(self, length=None):
        self._buffer.seek(self.bytes_written)
        marker = 0
        try:
            while True:
                self.curr_timer['read_time_s'] = time.time()
                data = self.socket.recv(self.socket_read_size)
                self.curr_timer['read_time_e'] = time.time()
                self.timer['content_trans_timer'] += self.curr_timer['read_time_e'] - \
                                                    self.curr_timer['read_time_s']
                if isinstance(data, bytes) and len(data) == 0:
                    raise socket.error('Connection closed by server.')
                self._buffer.write(data)
                data_length = len(data)
                self.bytes_written += data_length
                marker += data_length
                if length is not None and length > marker:
                    continue
                break
        except:
            self.disconnect()
            raise
    def read(self, length):
        if length > self.buffer_length:
            self.read_from_socket(length - self.buffer_length)
        self._buffer.seek(self.bytes_read)
        data = self._buffer.read(length)
        self.bytes_read += len(data)
        if self.bytes_read == self.bytes_written:
            self.buffer_purge()
        return data
    def readline(self):
        self._buffer.seek(self.bytes_read)
        data = self._buffer.readline()
        while not data.endswith(self.crlf):
            self.read_from_socket()
            self._buffer.seek(self.bytes_read)
            data = self._buffer.readline()
        self.bytes_read += len(data)
        if self.bytes_read == self.bytes_written:
            self.buffer_purge()
        return data
class Redis(object):
    """
    Redis 类，用于执行 redis 操作。
    """
    SUPPORT_CMD = ['INFO', 'SET', 'HSET', 'GET', 'HGET', 'HGETALL','LPUSH', 'LPOP']
    # 回调函数字典，用于格式化 redis 返回结果
    RESPONSE_CALLBACKS = {
        'SET': lambda r: r and r == 'OK',
        'HGETALL': pairs_to_dict,
    }
    SYM_STAR = '*'
    SYM_DOLLAR = '$'
    SYM_CRLF = '\r\n'
    SYM_EMPTY = ''
    def __init__(self,
                 host='localhost',
                 port=6379,
                 db=0,
                 password=None,
                 encode='utf-8'):
        self.host = host
        self.port = int(port)
        self.db = int(db)
        self.password = password
        self.encode = encode
        self.redis_url = "redis://:{password}@{host}:{port}/{db}".format(
                          password = self.password or '',
                          host = self.host,
                          port = self.port,
                          db = self.db)
        self.timer = {
            'dns_lookup_timer': 0,
            'connect_timer': 0,
            'auth_timer': 0,
            'server_process_timer' : 0,
            'content_trans_timer' : 0,
        }
        self.connection = Connect(self.host, self.port, crlf=self.SYM_CRLF)
        if self.password:
            self._auth()
        self.timer['dns_lookup_timer'] = self.connection.timer['dns_lookup_timer']
        self.timer['connect_timer'] = self.connection.timer['connect_timer']
        self.timer['auth_timer'] = self.connection.timer['server_process_timer'] + \
                                    self.connection.timer['content_trans_timer']
    def _auth(self):
        self.send_command('AUTH', self.password)
        response = self.read_response()
        if response != 'OK':
            raise Exception('Invalid Password')
    def execute_command(self, *args):
        self.send_command(*args)
        response = self.read_response()
        if args[0] in self.RESPONSE_CALLBACKS:
            response = self.RESPONSE_CALLBACKS[args[0]](response)
        self.timer['server_process_timer'] = self.connection.timer['server_process_timer']
        self.timer['content_trans_timer'] = self.connection.timer['content_trans_timer'] - \
                                            self.timer['content_trans_timer']
        return response
    def send_command(self, *args):
        self.connection.send_data(self.pack_command(*args))
    def pack_command(self, *args):
        "将 redis 命令参数格式化为 redis 协议"
        output = []
        command = args[0]
        if ' ' in command:
            args = tuple(command.split()) + args[1:]
        buff = self.SYM_EMPTY.join((self.SYM_STAR, str(len(args)), self.SYM_CRLF))
        for arg in args:
            # 如果 redis 命令过大则将命令分段
            if len(buff) > 6000 or len(arg) > 6000:
                buff = self.SYM_EMPTY.join((buff, self.SYM_DOLLAR, str(len(arg)), self.SYM_CRLF))
                output.append(buff)
                output.append(arg)
                buff = self.SYM_CRLF
            else:
                buff = self.SYM_EMPTY.join((buff, self.SYM_DOLLAR, str(len(arg)),
                                       self.SYM_CRLF, arg, self.SYM_CRLF))
        output.append(buff)
        return output
    def read_response(self):
        # redis 返回结果解析
        response = self.connection.readline()[:-2]
        if not response:
            raise Exception('Redis read error')
        byte, response = response[0], response[1:]
        if byte not in ('-', '+', ':', '$', '*'):
            raise Exception("Protocol Error: %s, %s" %
                                  (str(byte), str(response)))
        # 根据 redis 协议返回结果分为以下几种情况
        # redis server 返回一个错误
        if byte == '-':
            pass
        # 返回单一值
        elif byte == '+':
            pass
        # 返回整数
        elif byte == ':':
            response = long(response)
        # 批量返回
        elif byte == '$':
            length = int(response)
            if length == -1:
                return None
            response = self.connection.read(length + 2)[:-2]
        # 多批量返回
        elif byte == '*':
            length = int(response)
            if length == -1:
                return None
            response = [self.read_response() for i in xrange(length)]
        return response
    @wrapper_timer(True, client='redis', timer='timer', redis_url='redis_url')
    def set(self, name, value, ex=None, px=None, nx=False, xx=False):
        """
        redis set 操作
        ex: 以秒为单位的过期时间。
        px: 以毫秒为单位的过期时间。
        nx: 设置为 True 时，仅当 redis 中没有相应的 key 时才设置。
        xx: 设置为 True 时，仅当 redis 中有相应的 key 时才设置。
        """
        pieces = [name, value]
        if ex is not None:
            pieces.append('EX')
            if isinstance(ex, datetime.timedelta):
                ex = ex.seconds + ex.days * 24 * 3600
            pieces.append(ex)
        if px is not None:
            pieces.append('PX')
            if isinstance(px, datetime.timedelta):
                ms = int(px.microseconds / 1000)
                px = (px.seconds + px.days * 24 * 3600) * 1000 + ms
            pieces.append(px)
        if nx:
            pieces.append('NX')
        if xx:
            pieces.append('XX')
        return self.execute_command('SET', *pieces)
    @wrapper_timer(True, client='redis', timer='timer', redis_url='redis_url')
    def hset(self, name, key, value):
        """
        redis hset 操作。
        成功返回 1 ，否则返回 0 。
        """
        return self.execute_command('HSET', name, key, value)
    @wrapper_timer(True, client='redis', timer='timer', redis_url='redis_url')
    def get(self, name):
        """
        redis get 操作
        返回相应的 key 对应的 value，如果不存在返回 None 。
        """
        return self.execute_command('GET', name)
    @wrapper_timer(True, client='redis', timer='timer', redis_url='redis_url')
    def hget(self, name, key):
        """
        redis hget 操作
        """
        return self.execute_command('HGET', name, key)
    @wrapper_timer(True, client='redis', timer='timer', redis_url='redis_url')
    def hgetall(self, name):
        """
        redis hgetall
        返回一个 Python 字典
        """
        return self.execute_command('HGETALL', name)
    @wrapper_timer(True, client='redis', timer='timer', redis_url='redis_url')
    def lpop(self, name):
        """
        redis lpop 操作
        """
        return self.execute_command('LPOP', name)
    @wrapper_timer(True, client='redis', timer='timer', redis_url='redis_url')
    def lpush(self, name, *values):
        """
        redis lpush 操作
        """
        return self.execute_command('LPUSH', name, *values)
    @wrapper_timer(True, client='redis', timer='timer', redis_url='redis_url')
    def info(self, section=None):
        """
        redis info 操作
        """
        if section is None:
            return self.execute_command('INFO')
        else:
            return self.execute_command('INFO', section)
if __name__ == '__main__':
    cli = Redis('192.168.100.103', 6379, password='123456')
    response = cli.info()
    print cli.format_timer
