"""Pure python implementation of the binary Tokyo Tyrant 1.1.17 protocol

Tokyo Cabinet <http://tokyocabinet.sourceforge.net/> is a "super hyper ultra
database manager" written and maintained by Mikio Hirabayashi and released
under the LGPL.

Tokyo Tyrant is the de facto database server for Tokyo Cabinet written and
maintained by the same author. It supports a REST HTTP protocol, memcached,
and its own simple binary protocol. This library implements the full binary
protocol for the Tokyo Tyrant 1.1.17 in pure Python as defined here::

    http://tokyocabinet.sourceforge.net/tyrantdoc/

Typical usage is with the PyTyrant class which provides a dict-like wrapper
for the raw Tyrant protocol::

    >>> import pytyrant
    >>> t = pytyrant.PyTyrant.open('127.0.0.1', 1978)
    >>> t['__test_key__'] = 'foo'
    >>> t.concat('__test_key__', 'bar')
    >>> print t['__test_key__']
    foobar
    >>> del t['__test_key__']

"""
import itertools
import math
import socket
import struct
import UserDict

__version__ = '1.1.17'

__all__ = [
    'Tyrant', 'TyrantError', 'PyTyrant',
    'RDBMONOULOG', 'RDBXOLCKREC', 'RDBXOLCKGLB',
]

class TyrantError(Exception):
    pass


DEFAULT_PORT = 1978
MAGIC = 0xc8


RDBMONOULOG = 1 << 0
RDBXOLCKREC = 1 << 0
RDBXOLCKGLB = 1 << 1

RDBQOSTRASC, RDBQOSTRDESC, RDBQONUMASC, RDBQONUMDESC = range(4)


class C(object):
    """
    Tyrant Protocol constants
    """
    put = 0x10
    putkeep = 0x11
    putcat = 0x12
    putshl = 0x13
    putnr = 0x18
    out = 0x20
    get = 0x30
    mget = 0x31
    vsiz = 0x38
    iterinit = 0x50
    iternext = 0x51
    fwmkeys = 0x58
    addint = 0x60
    adddouble = 0x61
    ext = 0x68
    sync = 0x70
    vanish = 0x71
    copy = 0x72
    restore = 0x73
    setmst = 0x78
    rnum = 0x80
    size = 0x81
    stat = 0x88
    misc = 0x90


QUERY_OPERATIONS = {
    'streq': '0',     # string is equal to
    'strinc': '1',    # string is included in
    'strbw': '2',     # string begins with
    'strew': '3',     # string ends with
    'strand': '4',    # string includes all tokens in
    'stror': '5',     # string includes at least one token in
    'stroreq': '6',   # string is equal to at least one token in
    'strrx': '7',     # string matches regular expression of
    'numeq': '8',     # number is equal to
    'numgt': '9',     # number is greater than
    'numge': '10',    # number is greater than or equal to
    'numlt': '11',    # number is less than
    'numle': '12',    # number is less than or equal to
    'numbt': '13',    # number is between two tokens of
    'numoreq': '14',  # number is equal to at least one token in
}


def _t0(code):
    return [chr(MAGIC) + chr(code)]


def _t1(code, key):
    return [
        struct.pack('>BBI', MAGIC, code, len(key)),
        key,
    ]


def _t1FN(code, func, opts, args):
    outlst = [
        struct.pack('>BBIII', MAGIC, code, len(func), opts, len(args)),
        func,
    ]
    for k in args:
        outlst.extend([struct.pack('>I', len(k)), k])
    return outlst


def _t1R(code, key, msec):
    return [
        struct.pack('>BBIQ', MAGIC, code, len(key), msec),
        key,
    ]


def _t1M(code, key, count):
    return [
        struct.pack('>BBII', MAGIC, code, len(key), count),
        key,
    ]


def _tN(code, klst):
    outlst = [struct.pack('>BBI', MAGIC, code, len(klst))]
    for k in klst:
        outlst.extend([struct.pack('>I', len(k)), k])
    return outlst


def _t2(code, key, value):
    return [
        struct.pack('>BBII', MAGIC, code, len(key), len(value)),
        key,
        value,
    ]


def _t2W(code, key, value, width):
    return [
        struct.pack('>BBIII', MAGIC, code, len(key), len(value), width),
        key,
        value,
    ]


def _t3F(code, func, opts, key, value):
    return [
        struct.pack('>BBIIII', MAGIC, code, len(func), opts, len(key), len(value)),
        func,
        key,
        value,
    ]


def _tDouble(code, key, integ, fract):
    return [
        struct.pack('>BBIQQ', MAGIC, code, len(key), integ, fract),
        key,
    ]


def socksend(sock, lst):
    sock.sendall(''.join(lst))


def sockrecv(sock, bytes):
    d = ''
    while len(d) < bytes:
        d += sock.recv(min(8192, bytes - len(d)))
    return d


def socksuccess(sock):
    fail_code = ord(sockrecv(sock, 1))
    if fail_code:
        raise TyrantError(fail_code)


def socklen(sock):
    return struct.unpack('>I', sockrecv(sock, 4))[0]


def socklong(sock):
    return struct.unpack('>Q', sockrecv(sock, 8))[0]


def sockstr(sock):
    return sockrecv(sock, socklen(sock))


def sockdouble(sock):
    intpart, fracpart = struct.unpack('>QQ', sockrecv(sock, 16))
    return intpart + (fracpart * 1e-12)


def sockstrpair(sock):
    klen = socklen(sock)
    vlen = socklen(sock)
    k = sockrecv(sock, klen)
    v = sockrecv(sock, vlen)
    return k, v


def dict_to_list(dct):
    return list(itertools.chain(*dct.iteritems()))


def list_to_dict(lst):
    if not isinstance(lst, (list, tuple)):
        lst = list(lst)
    return dict((lst[i], lst[i + 1]) for i in xrange(0, len(lst), 2))


class PyTyrant(object, UserDict.DictMixin):
    """
    Dict-like proxy for a Tyrant instance
    """
    @classmethod
    def open(cls, *args, **kw):
        return cls(Tyrant.open(*args, **kw))

    def __init__(self, t):
        self.t = t

    def __repr__(self):
        # The __repr__ for UserDict.DictMixin isn't desirable
        # for a large KV store :)
        return object.__repr__(self)

    def has_key(self, key):
        return key in self

    def __contains__(self, key):
        try:
            self.t.vsiz(key)
        except TyrantError:
            return False
        else:
            return True

    def setdefault(self, key, value):
        try:
            self.t.putkeep(key, value)
        except TyrantError:
            return self[key]
        return value

    def __setitem__(self, key, value):
        self.t.put(key, value)

    def __getitem__(self, key):
        try:
            return self.t.get(key)
        except TyrantError:
            raise KeyError(key)

    def __delitem__(self, key):
        try:
            self.t.out(key)
        except TyrantError:
            raise KeyError(key)

    def __iter__(self):
        return self.iterkeys()

    def iterkeys(self):
        self.t.iterinit()
        try:
            while True:
                yield self.t.iternext()
        except TyrantError:
            pass

    def keys(self):
        return list(self.iterkeys())

    def __len__(self):
        return self.t.rnum()

    def clear(self):
        self.t.vanish()

    def update(self, other=None, **kwargs):
        # Make progressively weaker assumptions about "other"
        if other is None:
            pass
        elif hasattr(other, 'iteritems'):
            self.multi_set(other.iteritems())
        elif hasattr(other, 'keys'):
            self.multi_set([(k, other[k]) for k in other.keys()])
        else:
            self.multi_set(other)
        if kwargs:
            self.update(kwargs)

    def multi_del(self, keys, no_update_log=False):
        opts = (no_update_log and RDBMONOULOG or 0)
        if not isinstance(keys, (list, tuple)):
            keys = list(keys)
        self.t.misc("outlist", opts, keys)

    def multi_get(self, keys, no_update_log=False):
        opts = (no_update_log and RDBMONOULOG or 0)
        if not isinstance(keys, (list, tuple)):
            keys = list(keys)
        rval = self.t.misc("getlist", opts, keys)
        if len(rval) <= len(keys):
            # 1.1.10 protocol, may return invalid results
            if len(rval) < len(keys):
                raise KeyError("Missing a result, unusable response in 1.1.10")
            return rval
        # 1.1.11 protocol returns interleaved key, value list
        d = list_to_dict(rval)
        return map(d.get, keys)

    def multi_set(self, items, no_update_log=False):
        opts = (no_update_log and RDBMONOULOG or 0)
        lst = []
        for k, v in items:
            lst.extend((k, v))
        self.t.misc("putlist", opts, lst)

    def call_func(self, func, key, value, record_locking=False, global_locking=False):
        opts = (
            (record_locking and RDBXOLCKREC or 0) |
            (global_locking and RDBXOLCKGLB or 0))
        return self.t.ext(func, opts, key, value)

    def get_size(self, key):
        try:
            return self.t.vsiz(key)
        except TyrantError:
            raise KeyError(key)

    def get_stats(self):
        return dict(l.split('\t', 1) for l in self.t.stat().splitlines() if l)

    def prefix_keys(self, prefix, maxkeys=None):
        if maxkeys is None:
            maxkeys = len(self)
        return self.t.fwmkeys(prefix, maxkeys)

    def concat(self, key, value, width=None):
        if width is None:
            self.t.putcat(key, value)
        else:
            self.t.putshl(key, value, width)

    def sync(self):
        self.t.sync()

    def close(self):
        self.t.close()


class Query(object):
    def __init__(self, ptt):
        self.ptt = ptt
        self.conditions = []
        self._result_cache = None
    
    def __iter__(self):
        return iter(self._get_results())
    
    def __len__(self):
        return len(self._get_results())

    def __repr__(self):
        return repr(list(self))
    
    def __getitem__(self, k):
        if not isinstance(k, (slice, int, long)):
            raise TypeError
        assert ((not isinstance(k, slice) and (k >= 0))
            or (isinstance(k, slice) and (k.start is None or k.start >= 0)
                and (k.stop is None or k.stop >= 0))), \
            "Negative indexing is not supported."

        if self._result_cache:
            try:
                return self._result_cache[k]
            except IndexError:
                # Same behavior as querying the DB if the key doesn't exist
                if not isinstance(k, slice):
                    return None
                raise
        
        if isinstance(k, slice):
            if k.stop is not None:
                limit = k.stop - (k.start or 0)
            else:
                limit = -1
            condition = '\x00'.join(('setlimit', str(limit), str(k.start or 0)))
            resp = self.ptt.t.misc('search', 0, self.conditions + [condition])
            return k.step and list(resp)[::k.step] or resp

        condition = '\x00'.join(('setlimit', str(1), str(k)))
        resp = self.ptt.t.misc('search', 0, self.conditions + [condition])
        if not resp:
            return None
        else:
            return resp[0] 
    
    def filter(self, **query):
        q = self._clone()
        for key, value in query.iteritems():
            parts = key.split('__')
            if len(parts) != 2:
                raise ValueError("Filter arguments should be of the form "
                    "`field__operation`")
            field, operation = parts
            try:
                opcode = QUERY_OPERATIONS[operation]
            except KeyError:
                raise ValueError('%s is not a valid query operation' % (operation,))
            if not isinstance(value, basestring) and hasattr(value, '__iter__'):
                # Value is a list. Make it a comma separated string.
                value = ','.join(value)
            condition = '\x00'.join(["addcond", field, opcode, value])
            q.conditions.append(condition)
        return q

    def items(self):
        return self.ptt.multi_get(list(self))
    
    def order_by_num(self, field):
        q = self._clone()
        if field.startswith('-'):
            direction = RDBQONUMDESC
            field = field[1:]
        else:
            direction = RDBQONUMASC
        condition = '\x00'.join(["setorder", field, str(direction)])
        q.conditions.append(condition)
        return q
    
    def order_by_str(self, field):
        q = self._clone()
        if field.startswith('-'):
            direction = RDBQOSTRDESC
            field = field[1:]
        else:
            direction = RDBQOSTRASC
        condition = '\x00'.join(["setorder", field, str(direction)])
        q.conditions.append(condition)
        return q

    def _clone(self, klass=None, **kwargs):
        if klass is None:
            klass = self.__class__
        q = klass(self.ptt)
        q.conditions = self.conditions[:]
        q.__dict__.update(kwargs)
        return q
    
    def _get_results(self):
        if self._result_cache is None:
            self._result_cache = self.ptt.t.misc('search', 0, self.conditions)
        return self._result_cache


class PyTableTyrant(PyTyrant):
    """
    Dict-like proxy for a Table-based Tyrant instance
    """
    def setdefault(self, key, value, no_update_log=False):
        opts = (no_update_log and RDBMONOULOG or 0)
        try:
            self.t.misc('putkeep', opts, [key] + dict_to_list(value))
        except TyrantError:
            return self[key]
        return value

    def __setitem__(self, key, value):
        self.t.misc('put', 0, [key] + dict_to_list(value))

    def __getitem__(self, key):
        try:
            return list_to_dict(self.t.misc('get', 0, (key,)))
        except TyrantError:
            raise KeyError(key)

    def multi_get(self, keys, no_update_log=False):
        opts = (no_update_log and RDBMONOULOG or 0)
        if not isinstance(keys, (list, tuple)):
            keys = list(keys)
        rval = self.t.misc("getlist", opts, keys)
        if len(rval) <= len(keys):
            # 1.1.10 protocol, may return invalid results
            if len(rval) < len(keys):
                raise KeyError("Missing a result, unusable response in 1.1.10")
            return list_to_dict(rval.split('\x00'))
        # 1.1.11 protocol returns interleaved key, value list
        d = dict((rval[i], rval[i + 1]) for i in xrange(0, len(rval), 2))
        return [list_to_dict(d.get(i).split('\x00')) for i in keys]

    def multi_set(self, items, no_update_log=False):
        opts = (no_update_log and RDBMONOULOG or 0)
        lst = []
        for k, v in items:
            lst.extend((k, '\x00'.join(dict_to_list(v))))
        self.t.misc("putlist", opts, lst)

    def concat(self, key, value, width=None, no_update_log=False):
        opts = (no_update_log and RDBMONOULOG or 0)
        if width is None:
            self.t.misc('putcat', opts, ([key] + dict_to_list(value)))
        else:
            raise ValueError('Cannot concat with a width on a table database')
    
    def _search(self):
        return Query(self)
    search = property(_search)


class Tyrant(object):
    @classmethod
    def open(cls, host='127.0.0.1', port=DEFAULT_PORT):
        sock = socket.socket()
        sock.connect((host, port))
        sock.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
        return cls(sock)

    def __init__(self, sock):
        self.sock = sock

    def close(self):
        self.sock.close()

    def put(self, key, value):
        """Unconditionally set key to value
        """
        socksend(self.sock, _t2(C.put, key, value))
        socksuccess(self.sock)

    def putkeep(self, key, value):
        """Set key to value if key does not already exist
        """
        socksend(self.sock, _t2(C.putkeep, key, value))
        socksuccess(self.sock)

    def putcat(self, key, value):
        """Append value to the existing value for key, or set key to
        value if it does not already exist
        """
        socksend(self.sock, _t2(C.putcat, key, value))
        socksuccess(self.sock)

    def putshl(self, key, value, width):
        """Equivalent to::

            self.putcat(key, value)
            self.put(key, self.get(key)[-width:])
        """
        socksend(self.sock, _t2W(C.putshl, key, value, width))
        socksuccess(self.sock)

    def putnr(self, key, value):
        """Set key to value without waiting for a server response
        """
        socksend(self.sock, _t2(C.putnr, key, value))

    def out(self, key):
        """Remove key from server
        """
        socksend(self.sock, _t1(C.out, key))
        socksuccess(self.sock)

    def get(self, key):
        """Get the value of a key from the server
        """
        socksend(self.sock, _t1(C.get, key))
        socksuccess(self.sock)
        return sockstr(self.sock)

    def _mget(self, klst):
        socksend(self.sock, _tN(C.mget, klst))
        socksuccess(self.sock)
        numrecs = socklen(self.sock)
        for i in xrange(numrecs):
            k, v = sockstrpair(self.sock)
            yield k, v

    def mget(self, klst):
        """Get key,value pairs from the server for the given list of keys
        """
        return list(self._mget(klst))

    def vsiz(self, key):
        """Get the size of a value for key
        """
        socksend(self.sock, _t1(C.vsiz, key))
        socksuccess(self.sock)
        return socklen(self.sock)

    def iterinit(self):
        """Begin iteration over all keys of the database
        """
        socksend(self.sock, _t0(C.iterinit))
        socksuccess(self.sock)

    def iternext(self):
        """Get the next key after iterinit
        """
        socksend(self.sock, _t0(C.iternext))
        socksuccess(self.sock)
        return sockstr(self.sock)

    def _fwmkeys(self, prefix, maxkeys):
        socksend(self.sock, _t1M(C.fwmkeys, prefix, maxkeys))
        socksuccess(self.sock)
        numkeys = socklen(self.sock)
        for i in xrange(numkeys):
            yield sockstr(self.sock)

    def fwmkeys(self, prefix, maxkeys):
        """Get up to the first maxkeys starting with prefix
        """
        return list(self._fwmkeys(prefix, maxkeys))

    def addint(self, key, num):
        socksend(self.sock, _t1M(C.addint, key, num))
        socksuccess(self.sock)
        return socklen(self.sock)

    def adddouble(self, key, num):
        fracpart, intpart = math.modf(num)
        fracpart, intpart = int(fracpart * 1e12), int(intpart)
        socksend(self.sock, _tDouble(C.adddouble, key, fracpart, intpart))
        socksuccess(self.sock)
        return sockdouble(self.sock)

    def ext(self, func, opts, key, value):
        # tcrdbext opts are RDBXOLCKREC, RDBXOLCKGLB
        """Call func(key, value) with opts

        opts is a bitflag that can be RDBXOLCKREC for record locking
        and/or RDBXOLCKGLB for global locking"""
        socksend(self.sock, _t3F(C.ext, func, opts, key, value))
        socksuccess(self.sock)
        return sockstr(self.sock)

    def sync(self):
        """Synchronize the database
        """
        socksend(self.sock, _t0(C.sync))
        socksuccess(self.sock)

    def vanish(self):
        """Remove all records
        """
        socksend(self.sock, _t0(C.vanish))
        socksuccess(self.sock)

    def copy(self, path):
        """Hot-copy the database to path
        """
        socksend(self.sock, _t1(C.copy, path))
        socksuccess(self.sock)

    def restore(self, path, msec):
        """Restore the database from path at timestamp (in msec)
        """
        socksend(self.sock, _t1R(C.copy, path, msec))
        socksuccess(self.sock)

    def setmst(self, host, port):
        """Set master to host:port
        """
        socksend(self.sock, _t1M(C.setmst, host, port))
        socksuccess(self.sock)

    def rnum(self):
        """Get the number of records in the database
        """
        socksend(self.sock, _t0(C.rnum))
        socksuccess(self.sock)
        return socklong(self.sock)

    def size(self):
        """Get the size of the database
        """
        socksend(self.sock, _t0(C.size))
        socksuccess(self.sock)
        return socklong(self.sock)

    def stat(self):
        """Get some statistics about the database
        """
        socksend(self.sock, _t0(C.stat))
        socksuccess(self.sock)
        return sockstr(self.sock)

    def _misc(self, func, opts, args):
        # tcrdbmisc opts are RDBMONOULOG
        socksend(self.sock, _t1FN(C.misc, func, opts, args))
        try:
            socksuccess(self.sock)
        finally:
            numrecs = socklen(self.sock)
        for i in xrange(numrecs):
            yield sockstr(self.sock)

    def misc(self, func, opts, args):
        """All databases support "putlist", "outlist", and "getlist".
        "putlist" is to store records. It receives keys and values one after the other, and returns an empty list.
        "outlist" is to remove records. It receives keys, and returns an empty list.
        "getlist" is to retrieve records. It receives keys, and returns values.

        Table database supports "setindex", "search", "genuid".

        opts is a bitflag that can be RDBMONOULOG to prevent writing to the update log
        """
        return list(self._misc(func, opts, args))


def main():
    import doctest
    doctest.testmod()


if __name__ == '__main__':
    main()
