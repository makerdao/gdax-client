#!/bin/sh

py.test --cov=gdax_client --cov-report=term --cov-append tests/
