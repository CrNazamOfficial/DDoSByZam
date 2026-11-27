
import asyncio
import json
import os
from datetime import datetime, timezone

from engine import run_test, validate_target


CONFIG_FILE = "config.json"
REPORT_DIRECTORY = "reports"


def load_config():
    with open(CONFIG_FILE, "r", encoding="utf-8") as file:
        return json.load(file)


def save_report(url, result):
    os.makedirs(REPORT_DIRECTORY, exist_ok=True)

    report = {
        "tool": "DDoSByZam",
        "version": "2.0",
        "timestamp": datetime.now(
            timezone.utc
        ).isoformat(),
        "target": url,
        "results": result
    }

    filename = datetime.now().strftime(
        "report_%Y%m%d_%H%M%S.json"
    )

    path = os.path.join(
        REPORT_DIRECTORY,
        filename
    )

    with open(path, "w", encoding="utf-8") as file:
        json.dump(report, file, indent=4)

    print(f"\nReport saved: {path}")


def show_result(result):
    print("\n" + "=" * 42)
    print("             TEST RESULTS")
    print("=" * 42)

    print(f"Attempted       : {result['attempted']}")
    print(f"Completed       : {result['completed']}")
    print(f"Incomplete      : {result['incomplete']}")
    print(f"Successful      : {result['successful']}")
    print(f"Failed          : {result['failed']}")

    print("-" * 42)

    print(f"Duration        : {result['duration_seconds']} s")
    print(f"Actual RPS      : {result['requests_per_second']}")

    print(f"Average latency : {result['average_latency_ms']} ms")
    print(f"Median latency  : {result['median_latency_ms']} ms")
    print(f"P95 latency     : {result['p95_latency_ms']} ms")
    print(f"P99 latency     : {result['p99_latency_ms']} ms")

    print(f"Bytes received  : {result['bytes_received']}")

    print("-" * 42)
    print("HTTP STATUS")

    if result["http_statuses"]:
        for status, count in result["http_statuses"].items():
            print(f"  {status}: {count}")
    else:
        print("  No HTTP responses")

    print("-" * 42)
    print("ERRORS")

    if result["errors"]:
        for error, count in result["errors"].items():
            print(f"  {error}: {count}")
    else:
        print("  None")

    print("-" * 42)
    print("STOP REASON")
    print(result["stop_reason"])

    print("=" * 42)


def show_resource_summary(result):
    samples = result["resource_monitor"]

    print("\nLOCAL PROCESS MONITOR")

    if not samples:
        print("No resource samples.")
        return

    cpu_values = [
        item["cpu_percent"] for item in samples
    ]

    memory_values = [
        item["memory_mb"] for item in samples
    ]

    print(
        "Average process CPU:",
        round(sum(cpu_values) / len(cpu_values), 2),
        "%"
    )

    print(
        "Peak process RAM:",
        round(max(memory_values), 2),
        "MB"
    )

    print(
        "Samples:",
        len(samples)
    )


async def start_test():
    try:
        config = load_config()
    except (OSError, json.JSONDecodeError) as exc:
        print(f"Configuration error: {exc}")
        return

    url = input("\nTarget URL: ").strip()

    valid, message = validate_target(url)

    if not valid:
        print(f"\nRejected: {message}")
        return

    print(f"\n{message}")
    print("Only test systems you own or have explicit permission to test.")

    confirmation = input(
        "Type YES to confirm authorization: "
    ).strip()

    if confirmation != "YES":
        print("Cancelled.")
        return

    print("\nStarting bounded HTTP performance test...")

    try:
        result = await run_test(url, config)

    except KeyboardInterrupt:
        print("\nInterrupted.")
        return

    except Exception as exc:
        print(
            f"\nTest error: {type(exc).__name__}: {exc}"
        )
        return

    show_result(result)
    show_resource_summary(result)

    save_report(url, result)


def main():
    while True:
        print("""
========================================
               DDoSByZam
             VERSION 2.0
========================================
[1] Start HTTP Load Test
[2] Exit
========================================
""")

        choice = input("Select: ").strip()

        if choice == "1":
            try:
                asyncio.run(start_test())
            except KeyboardInterrupt:
                print("\nProgram interrupted.")

        elif choice == "2":
            print("Exiting DDoSByZam.")
            break

        else:
            print("Invalid selection.")


if __name__ == "__main__":
    main()
