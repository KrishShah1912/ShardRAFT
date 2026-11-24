#!/usr/bin/env python3

import argparse
import datetime
import json
import pathlib
import re
import subprocess
import time

ROOT = pathlib.Path(__file__).resolve().parent.parent
OUTPUT = (
    ROOT
    / "web"
    / "public"
    / "generated"
    / "validation-report.json"
)


def run(
    command,
    cwd=ROOT,
    timeout=180,
):
    started = time.perf_counter()

    completed = subprocess.run(
        command,
        cwd=cwd,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )

    duration_ms = round(
        (time.perf_counter() - started)
        * 1000,
        3,
    )

    return (
        completed.returncode,
        completed.stdout,
        completed.stderr,
        duration_ms,
    )


def git_value(*args):
    code, stdout, _, _ = run(
        ["git", *args],
        timeout=10,
    )

    if code != 0:
        return "unknown"

    return stdout.strip()


def go_suite():
    code, stdout, stderr, duration = run(
        [
            "go",
            "test",
            "-json",
            "-count=1",
            "./...",
        ],
        timeout=240,
    )

    tests = {}

    for raw in stdout.splitlines():
        try:
            event = json.loads(raw)
        except json.JSONDecodeError:
            continue

        test_name = event.get("Test")

        if not test_name:
            continue

        package = event.get(
            "Package",
            "",
        )

        key = (
            package,
            test_name,
        )

        action = event.get("Action")

        if action not in {
            "pass",
            "fail",
            "skip",
        }:
            continue

        tests[key] = {
            "name": test_name,
            "package": package,
            "status": {
                "pass": "passed",
                "fail": "failed",
                "skip": "skipped",
            }[action],
            "duration_ms": round(
                float(
                    event.get(
                        "Elapsed",
                        0,
                    )
                )
                * 1000,
                3,
            ),
        }

    values = list(
        tests.values()
    )

    passed = sum(
        test["status"] == "passed"
        for test in values
    )

    failed = sum(
        test["status"] == "failed"
        for test in values
    )

    skipped = sum(
        test["status"] == "skipped"
        for test in values
    )

    return {
        "id": "go",
        "name": "Go Backend",
        "kind": "go",
        "command":
            "go test -json -count=1 ./...",
        "status":
            "passed"
            if code == 0
            else "failed",
        "duration_ms": duration,
        "passed": passed,
        "failed": failed,
        "skipped": skipped,
        "tests": sorted(
            values,
            key=lambda value: (
                value.get(
                    "package",
                    "",
                ),
                value["name"],
            ),
        ),
        "_error":
            stderr.strip(),
    }


def frontend_suite():
    code, stdout, stderr, duration = run(
        [
            "npm",
            "test",
        ],
        cwd=ROOT / "web",
        timeout=120,
    )

    tests = []

    pattern = re.compile(
        r"^[✔✓]\s+(.+?)\s+\(([0-9.]+)ms\)$"
    )

    for line in stdout.splitlines():
        line = line.strip()

        match = pattern.match(line)

        if not match:
            continue

        tests.append(
            {
                "name":
                    match.group(1),
                "status":
                    "passed",
                "duration_ms":
                    float(
                        match.group(2)
                    ),
            }
        )

    passed = len(tests)

    return {
        "id": "frontend-routing",
        "name":
            "Frontend Request Routing",
        "kind": "frontend",
        "command": "npm test",
        "status":
            "passed"
            if code == 0
            else "failed",
        "duration_ms": duration,
        "passed": passed,
        "failed":
            0 if code == 0 else 1,
        "skipped": 0,
        "tests": tests,
        "_error":
            stderr.strip(),
    }


def e2e_suite():
    result_path = (
        ROOT
        / "web"
        / "test-results"
        / "validation-playwright.json"
    )

    result_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    if result_path.exists():
        result_path.unlink()

    code, stdout, stderr, duration = run(
        [
            "npx",
            "playwright",
            "test",
            "--reporter=json",
        ],
        cwd=ROOT / "web",
        timeout=180,
    )

    data = {}

    # Playwright JSON is normally emitted to stdout.
    # npm wrappers can add extra text, so first attempt
    # direct parsing and then recover the JSON object.
    try:
        data = json.loads(stdout)
    except json.JSONDecodeError:
        first = stdout.find("{")
        last = stdout.rfind("}")

        if first >= 0 and last > first:
            try:
                data = json.loads(
                    stdout[first:last + 1]
                )
            except json.JSONDecodeError:
                data = {}

    tests = []

    def walk_suite(suite):
        for spec in suite.get("specs", []):
            spec_title = spec.get(
                "title",
                "Unnamed Playwright test",
            )

            for test in spec.get("tests", []):
                results = test.get(
                    "results",
                    [],
                )

                if not results:
                    continue

                result = results[-1]

                raw_status = result.get(
                    "status",
                    "unknown",
                )

                status = {
                    "passed": "passed",
                    "failed": "failed",
                    "skipped": "skipped",
                    "timedOut": "failed",
                    "interrupted": "failed",
                }.get(
                    raw_status,
                    "unknown",
                )

                tests.append(
                    {
                        "name": spec_title,
                        "status": status,
                        "duration_ms": float(
                            result.get(
                                "duration",
                                0,
                            )
                        ),
                    }
                )

        for child in suite.get(
            "suites",
            [],
        ):
            walk_suite(child)

    for suite in data.get("suites", []):
        walk_suite(suite)

    passed = sum(
        test["status"] == "passed"
        for test in tests
    )

    failed = sum(
        test["status"] == "failed"
        for test in tests
    )

    skipped = sum(
        test["status"] == "skipped"
        for test in tests
    )

    return {
        "id": "playwright",
        "name": "Playwright End-to-End",
        "kind": "e2e",
        "command": "npx playwright test",
        "status":
            "passed"
            if code == 0
            else "failed",
        "duration_ms": duration,
        "passed": passed,
        "failed": failed,
        "skipped": skipped,
        "tests": tests,
        "_error": stderr.strip(),
    }

def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--mode",
        choices=("standard", "full"),
        default="full",
    )

    args = parser.parse_args()

    started = time.perf_counter()

    print(
        "Running Go validation..."
    )
    go = go_suite()

    print(
        "Running frontend validation..."
    )
    frontend = frontend_suite()

    suites = [
        go,
        frontend,
    ]

    if args.mode == "full":
        print(
            "Running Playwright validation..."
        )
        suites.append(
            e2e_suite()
        )

    total = sum(
        suite["passed"]
        + suite["failed"]
        + suite["skipped"]
        for suite in suites
    )

    passed = sum(
        suite["passed"]
        for suite in suites
    )

    failed = sum(
        suite["failed"]
        for suite in suites
    )

    skipped = sum(
        suite["skipped"]
        for suite in suites
    )

    report = {
        "schema_version": 1,
        "validation_mode": args.mode,
        "generated_at":
            datetime.datetime.now(
                datetime.timezone.utc
            ).isoformat(),
        "git_commit":
            git_value(
                "rev-parse",
                "--short",
                "HEAD",
            ),
        "git_branch":
            git_value(
                "branch",
                "--show-current",
            ),
        "duration_ms": round(
            (
                time.perf_counter()
                - started
            )
            * 1000,
            3,
        ),
        "status":
            "passed"
            if all(
                suite["status"]
                == "passed"
                for suite in suites
            )
            else "failed",
        "total": total,
        "passed": passed,
        "failed": failed,
        "skipped": skipped,
        "suites": [],
    }

    for suite in suites:
        suite = dict(suite)
        suite.pop(
            "_error",
            None,
        )

        report["suites"].append(
            suite
        )

    OUTPUT.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    OUTPUT.write_text(
        json.dumps(
            report,
            indent=2,
        )
        + "\n"
    )

    print()
    print("Validation complete")
    print("===================")
    print(
        f"Status  : {report['status']}"
    )
    print(
        f"Tests   : {total}"
    )
    print(
        f"Passed  : {passed}"
    )
    print(
        f"Failed  : {failed}"
    )
    print(
        f"Skipped : {skipped}"
    )
    print(
        f"Report  : {OUTPUT}"
    )

    if report["status"] != "passed":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
