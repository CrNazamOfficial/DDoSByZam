
# DDoSByZam v2.0

Bounded asynchronous HTTP performance testing.

## Features

- Real HTTP GET requests
- Async worker manager
- Global request rate limiter
- Concurrency control
- Duration limit
- Request count limit
- HTTP status analyzer
- Connection and timeout tracking
- Average latency
- Median latency
- P95 and P99 latency
- Actual requests per second
- Local process CPU and RAM monitoring
- JSON reports
- Automatic safety stops

## Requirements

Python 3.10 or newer.

## Installation

    pip install -r requirements.txt

## Run

    python main.py

## Target Restrictions

Only literal private and loopback IP addresses are accepted.

Examples:

    http://127.0.0.1:8000
    http://192.168.1.10:8000

Only use systems you own or have explicit permission to test.

## Reports

Reports are saved inside:

    reports/

Each report contains actual measurements from the test.

## Important

Local process CPU and RAM are not remote server metrics.
Remote server metrics must be collected directly on the server.
