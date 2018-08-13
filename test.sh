#!/bin/sh

PYTHONPATH=$PYTHONPATH:./lib/pymaker py.test --cov=gdax_client --cov-report=term --cov-append tests/
