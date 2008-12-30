#!/usr/bin/env python

from distutils.core import setup

VERSION = '1.1.10'
DESCRIPTION = "Pure python client implementation of the Tokyo Tyrant protocol"
LONG_DESCRIPTION = """
pytyrant is a pure python client implementation of the binary Tokyo Tyrant
protocol. Tokyo Cabinet <http://tokyocabinet.sourceforge.net/> is a "super
hyper ultra database manager" written and maintained by Mikio Hirabayashi and
released under the LGPL.

Tokyo Tyrant is the de facto database server for Tokyo Cabinet written and
maintained by the same author. It supports a REST HTTP protocol, memcached,
and its own simple binary protocol. This library implements the full binary
protocol for the Tokyo Tyrant 1.1.10 in pure Python as defined here::
    
        http://tokyocabinet.sourceforge.net/tyrantdoc/
"""

CLASSIFIERS = filter(None, map(str.strip,
"""                 
Intended Audience :: Developers
License :: OSI Approved :: MIT License
Programming Language :: Python
Topic :: Database :: Front-Ends
Topic :: Software Development :: Libraries :: Python Modules
""".splitlines()))


setup(
    name="pytyrant",
    version=VERSION,
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    classifiers=CLASSIFIERS,
    author="Bob Ippolito",
    author_email="bob@redivi.com",
    url="http://code.google.com/p/pytyrant/",
    license="MIT License",
    py_modules=['pytyrant'],
    platforms=['any'],
)
