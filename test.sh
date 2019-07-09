#!/bin/sh

source _virtualenv/bin/activate

py.test --cov=gdax_client --cov-report=term --cov-append tests/
