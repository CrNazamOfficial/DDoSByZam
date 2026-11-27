
import asyncio
import time
import statistics
import ipaddress
from collections import Counter
from urllib.parse import urlsplit

import aiohttp
import psutil


# Hard safety limits
HARD_MAX_REQUESTS = 500
HARD_MAX_CONCURRENCY = 5
HARD_MAX_RPS = 5
HARD_MAX_DURATION = 60
HARD_MAX_TIMEOUT = 5
HARD_MAX_BODY_BYTES = 65536


def percentile(values, percent):
    if not values:
        return 0.0

    ordered = sorted(values)
    index = round((len(ordered) - 1) * percent)

    return round(ordered[index], 2)


def validate_target(url):
    try:
        parsed = urlsplit(url)

        if parsed.scheme not in ("http", "https"):
            return False, "Only HTTP and HTTPS are allowed."

        if not parsed.hostname:
            return False, "Missing hostname."

        if parsed.username or parsed.password:
            return False, "URL credentials are forbidden."

        # Only literal IP addresses are accepted.
        ip = ipaddress.ip_address(parsed.hostname)

        if not (ip.is_private or ip.is_loopback):
            return False, "Public IP addresses are forbidden."

        if ip.is_link_local or ip.is_multicast or ip.is_unspecified:
            return False, "This IP address is forbidden."

        if parsed.port is not None:
            if not 1 <= parsed.port <= 65535:
                return False, "Invalid port."

        return True, "Target accepted."

    except ValueError:
        return False, "Use a literal private or loopback IP address."

    except Exception:
        return False, "Invalid URL."


async def run_test(url, config):
    max_requests = min(
        max(1, int(config.get("max_requests", 100))),
        HARD_MAX_REQUESTS
    )

    concurrency = min(
        max(1, int(config.get("max_concurrency", 3))),
        HARD_MAX_CONCURRENCY
    )

    max_rps = min(
        max(1, int(config.get("max_rps", 2))),
        HARD_MAX_RPS
    )

    duration = min(
        max(1, int(config.get("duration_seconds", 20))),
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

    attempted = 0
    completed = 0
    successful = 0
    failed = 0
    bytes_received = 0

    consecutive_errors = 0
    stop_reason = "Request limit completed"

    latencies = []
    statuses = Counter()
    errors = Counter()
    resource_samples = []

    state_lock = asyncio.Lock()
    pace_lock = asyncio.Lock()

    start = time.perf_counter()
    deadline = start + duration
    next_slot = start

    stop_event = asyncio.Event()

    process = psutil.Process()

    async def reserve_request():
        nonlocal attempted, next_slot

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

            if now >= deadline or attempted >= max_requests:
                return False

            attempted += 1
            next_slot = max(next_slot, now) + (1 / max_rps)

            return True

    async def record_result(status=None, error=None, latency=None,
                            received=0):
        nonlocal completed, successful, failed
        nonlocal consecutive_errors, bytes_received
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

            if consecutive_errors >= max_consecutive_errors:
                stop_reason = "Consecutive error safety stop"
                stop_event.set()

            if completed >= 20:
                ratio = failed / completed

                if ratio >= max_failure_ratio:
                    stop_reason = "Failure ratio safety stop"
                    stop_event.set()

    timeout = aiohttp.ClientTimeout(
        total=timeout_seconds
    )

    connector = aiohttp.TCPConnector(
        limit=concurrency,
        limit_per_host=concurrency
    )

    async def worker(session):
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

    async def monitor():
        while not stop_event.is_set():
            try:
                cpu = process.cpu_percent(interval=None)
                memory = process.memory_info().rss

                resource_samples.append({
                    "elapsed": round(
                        time.perf_counter() - start, 2
                    ),
                    "cpu_percent": cpu,
                    "memory_mb": round(
                        memory / (1024 * 1024), 2
                    )
                })

                await asyncio.sleep(monitor_interval)

            except asyncio.CancelledError:
                raise

            except Exception:
                await asyncio.sleep(monitor_interval)

    async with aiohttp.ClientSession(
        timeout=timeout,
        connector=connector,
        trust_env=False
    ) as session:

        monitor_task = asyncio.create_task(monitor())

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

    elapsed = time.perf_counter() - start

    if attempted >= max_requests:
        stop_reason = "Request limit reached"

    elif time.perf_counter() >= deadline:
        stop_reason = "Duration limit reached"

    elif stop_event.is_set() and stop_reason == "Request limit completed":
        stop_reason = "Safety stop"

    average_latency = (
        statistics.mean(latencies)
        if latencies else 0
    )

    return {
        "attempted": attempted,
        "completed": completed,
        "incomplete": attempted - completed,
        "successful": successful,
        "failed": failed,

        "duration_seconds": round(elapsed, 2),

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
