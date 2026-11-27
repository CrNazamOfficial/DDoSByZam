
import asyncio
import time
import statistics
import ipaddress
import os

from collections import Counter
from urllib.parse import urlsplit

import aiohttp


# ==========================================
# DDoSByZam - Bounded HTTP Testing Engine
# ==========================================

HARD_MAX_REQUESTS = 1000
HARD_MAX_CONCURRENCY = 30
HARD_MAX_RPS = 30
HARD_MAX_DURATION = 160
HARD_MAX_TIMEOUT = 5
HARD_MAX_BODY_BYTES = 99999


# ==========================================
# TARGET VALIDATION
# ==========================================

def validate_target(url):
    try:
        parsed = urlsplit(url)

        if parsed.scheme not in ("http", "https"):
            return False, "Only HTTP and HTTPS are allowed."

        if not parsed.hostname:
            return False, "Missing hostname."

        if parsed.username or parsed.password:
            return False, "URL credentials are forbidden."

        # Accept literal IP addresses only.
        ip = ipaddress.ip_address(parsed.hostname)

        if not (ip.is_private or ip.is_loopback):
            return False, "Public IP addresses are forbidden."

        if ip.is_link_local:
            return False, "Link-local addresses are forbidden."

        if ip.is_multicast or ip.is_unspecified:
            return False, "This IP address is forbidden."

        if parsed.port is not None:
            if not 1 <= parsed.port <= 65535:
                return False, "Invalid port."

        return True, "Target accepted."

    except ValueError:
        return False, "Use a literal private or loopback IP address."

    except Exception:
        return False, "Invalid target URL."


# ==========================================
# LATENCY PERCENTILE
# ==========================================

def percentile(values, percent):
    if not values:
        return 0.0

    ordered = sorted(values)

    index = int(
        (len(ordered) - 1) * percent
    )

    return round(ordered[index], 2)


# ==========================================
# LOCAL RESOURCE MONITOR
# ==========================================

def read_memory_mb():
    try:
        with open("/proc/self/status", "r") as file:
            for line in file:
                if line.startswith("VmRSS:"):
                    memory_kb = int(line.split()[1])
                    return round(memory_kb / 1024, 2)

    except (OSError, ValueError, IndexError):
        pass

    return 0.0


# ==========================================
# MAIN ASYNC ENGINE
# ==========================================

async def run_test(url, config):

    # --------------------------------------
    # Configuration with hard limits
    # --------------------------------------

    max_requests = min(
        max(1, int(config.get("max_requests", 800))),
        HARD_MAX_REQUESTS
    )

    concurrency = min(
        max(1, int(config.get("max_concurrency", 20))),
        HARD_MAX_CONCURRENCY
    )

    max_rps = min(
        max(1, int(config.get("max_rps", 20))),
        HARD_MAX_RPS
    )

    duration = min(
        max(1, int(config.get("duration_seconds", 60))),
        HARD_MAX_DURATION
    )

    timeout_seconds = min(
        max(1, int(config.get("request_timeout", 5))),
        HARD_MAX_TIMEOUT
    )

    max_consecutive_errors = min(
        max(1, int(config.get("max_consecutive_errors", 10))),
        10
    )

    max_failure_ratio = min(
        max(0.1, float(config.get("max_failure_ratio", 0.5))),
        0.5
    )

    monitor_interval = min(
        max(1, int(config.get("monitor_interval", 1))),
        5
    )

    # --------------------------------------
    # Runtime counters
    # --------------------------------------

    attempted = 0
    completed = 0
    successful = 0
    failed = 0
    bytes_received = 0

    consecutive_errors = 0

    latencies = []
    statuses = Counter()
    errors = Counter()
    resource_samples = []

    state_lock = asyncio.Lock()
    pace_lock = asyncio.Lock()

    stop_event = asyncio.Event()

    start = time.perf_counter()
    deadline = start + duration

    next_slot = start

    stop_reason = "Request limit completed"

    # --------------------------------------
    # Global request scheduler
    # --------------------------------------

    async def reserve_request():

        nonlocal attempted
        nonlocal next_slot

        async with pace_lock:

            if stop_event.is_set():
                return False

            if attempted >= max_requests:
                return False

            now = time.perf_counter()

            if now >= deadline:
                return False

            wait_time = next_slot - now

            if wait_time > 0:
                await asyncio.sleep(wait_time)

            now = time.perf_counter()

            if stop_event.is_set():
                return False

            if now >= deadline:
                return False

            if attempted >= max_requests:
                return False

            attempted += 1

            next_slot = (
                max(next_slot, now) + (1 / max_rps)
            )

            return True

    # --------------------------------------
    # Result collector
    # --------------------------------------

    async def record_result(
        status=None,
        error=None,
        latency=None,
        received=0
    ):

        nonlocal completed
        nonlocal successful
        nonlocal failed
        nonlocal consecutive_errors
        nonlocal bytes_received
        nonlocal stop_reason

        async with state_lock:

            completed += 1
            bytes_received += received

            if latency is not None:
                latencies.append(latency)

            if status is not None:
                statuses[str(status)] += 1

            is_success = (
                status is not None
                and 200 <= status < 400
                and error is None
            )

            if is_success:
                successful += 1
                consecutive_errors = 0

            else:
                failed += 1
                consecutive_errors += 1

                if error:
                    errors[error] += 1

            # Stop after repeated errors.
            if consecutive_errors >= max_consecutive_errors:

                stop_reason = (
                    "Consecutive error safety stop"
                )

                stop_event.set()

            # Stop when failure ratio is too high.
            if completed >= 20:

                failure_ratio = failed / completed

                if failure_ratio >= max_failure_ratio:

                    stop_reason = (
                        "Failure ratio safety stop"
                    )

                    stop_event.set()

    # --------------------------------------
    # Resource monitoring
    # --------------------------------------

    async def monitor():

        previous_cpu = os.times()
        previous_time = time.perf_counter()

        while not stop_event.is_set():

            try:
                current_cpu = os.times()
                current_time = time.perf_counter()

                elapsed = current_time - previous_time

                cpu_used = (
                    (current_cpu.user - previous_cpu.user)
                    +
                    (current_cpu.system - previous_cpu.system)
                )

                cpu_percent = (
                    cpu_used / elapsed * 100
                    if elapsed > 0 else 0
                )

                resource_samples.append({
                    "elapsed": round(
                        current_time - start, 2
                    ),
                    "cpu_percent": round(
                        cpu_percent, 2
                    ),
                    "memory_mb": read_memory_mb()
                })

                previous_cpu = current_cpu
                previous_time = current_time

                await asyncio.sleep(monitor_interval)

            except asyncio.CancelledError:
                raise

            except Exception:
                await asyncio.sleep(monitor_interval)

    # --------------------------------------
    # HTTP worker
    # --------------------------------------

    async def worker(session):

        nonlocal completed

        while await reserve_request():

            request_start = time.perf_counter()

            try:

                async with session.get(
                    url,
                    allow_redirects=False
                ) as response:

                    body = await response.content.read(
                        HARD_MAX_BODY_BYTES
                    )

                    latency = (
                        time.perf_counter() - request_start
                    ) * 1000

                    await record_result(
                        status=response.status,
                        latency=latency,
                        received=len(body)
                    )

            except asyncio.TimeoutError:

                await record_result(
                    error="timeout"
                )

            except aiohttp.ClientConnectorError:

                await record_result(
                    error="connection_error"
                )

            except aiohttp.ClientSSLError:

                await record_result(
                    error="tls_error"
                )

            except aiohttp.ClientError as exc:

                await record_result(
                    error=type(exc).__name__
                )

            except Exception as exc:

                await record_result(
                    error=type(exc).__name__
                )

    # --------------------------------------
    # HTTP session
    # --------------------------------------

    timeout = aiohttp.ClientTimeout(
        total=timeout_seconds
    )

    connector = aiohttp.TCPConnector(
        limit=concurrency,
        limit_per_host=concurrency
    )

    # --------------------------------------
    # Start workers and monitor
    # --------------------------------------

    async with aiohttp.ClientSession(
        timeout=timeout,
        connector=connector,
        trust_env=False
    ) as session:

        monitor_task = asyncio.create_task(
            monitor()
        )

        workers = [
            asyncio.create_task(worker(session))
            for _ in range(concurrency)
        ]

        try:

            await asyncio.wait_for(
                asyncio.gather(*workers),
                timeout=duration + 1
            )

        except asyncio.TimeoutError:

            stop_reason = "Duration limit reached"
            stop_event.set()

        finally:

            stop_event.set()

            for task in workers:

                if not task.done():
                    task.cancel()

            await asyncio.gather(
                *workers,
                return_exceptions=True
            )

            monitor_task.cancel()

            await asyncio.gather(
                monitor_task,
                return_exceptions=True
            )

    # --------------------------------------
    # Final statistics
    # --------------------------------------

    elapsed = time.perf_counter() - start

    if attempted >= max_requests:

        stop_reason = "Request limit reached"

    elif time.perf_counter() >= deadline:

        stop_reason = "Duration limit reached"

    elif stop_event.is_set() and (
        stop_reason == "Request limit completed"
    ):

        stop_reason = "Safety stop"

    average_latency = (
        statistics.mean(latencies)
        if latencies else 0
    )

    # --------------------------------------
    # Return actual measurements
    # --------------------------------------

    return {

        "attempted": attempted,

        "completed": completed,

        "incomplete": attempted - completed,

        "successful": successful,

        "failed": failed,

        "duration_seconds": round(
            elapsed, 2
        ),

        "requests_per_second": round(
            completed / elapsed, 2
        ) if elapsed else 0,

        "average_latency_ms": round(
            average_latency, 2
        ),

        "median_latency_ms": percentile(
            latencies, 0.50
        ),

        "p95_latency_ms": percentile(
            latencies, 0.95
        ),

        "p99_latency_ms": percentile(
            latencies, 0.99
        ),

        "bytes_received": bytes_received,

        "http_statuses": dict(statuses),

        "errors": dict(errors),

        "resource_monitor": resource_samples,

        "stop_reason": stop_reason,

        "settings": {

            "max_requests": max_requests,

            "max_concurrency": concurrency,

            "max_rps": max_rps,

            "duration_seconds": duration,

            "request_timeout": timeout_seconds

        }
    }
