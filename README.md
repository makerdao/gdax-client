# gdax-client

[![Build Status](https://travis-ci.org/makerdao/gdax-client.svg?branch=master)](https://travis-ci.org/makerdao/gdax-client)
[![codecov](https://codecov.io/gh/makerdao/gdax-client/branch/master/graph/badge.svg)](https://codecov.io/gh/makerdao/gdax-client)

`gdax-client` is a simple library for accessing price feeds from _coinbase_ (former GDAX) WebSocket.

It handles WebSocket reconnection very reliably. It also automatically expires the price value
if the client isn't able to connect to the WebSocket for at least `expiry` seconds.


## Installation

This project uses *Python 3.6.2*.

In order to clone the project and install required third-party packages please execute:
```
git clone https://github.com/makerdao/gdax-client.git
cd gdax-client
git submodule update --init --recursive
pip3 install -r requirements.txt
```

For some known Ubuntu and macOS issues see the [pymaker](https://github.com/makerdao/pymaker) README.


## License

See [COPYING](https://github.com/makerdao/gdax-client/blob/master/COPYING) file.
