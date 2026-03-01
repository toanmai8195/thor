#!/usr/bin/env python3
# =============================================================================
# DBT RUNNER SCRIPT
# =============================================================================
# Script để chạy dbt commands từ Bazel.
#
# CÁCH DÙNG:
#
# 1. Với Bazel:
#    bazel run //com/tm/src/services/analytics-aggregator:run_dbt -- run
#    bazel run //com/tm/src/services/analytics-aggregator:run_dbt -- test
#    bazel run //com/tm/src/services/analytics-aggregator:run_dbt -- docs generate
#
# 2. Với Python:
#    python scripts/run_dbt.py run
#    python scripts/run_dbt.py test --select staging
#    python scripts/run_dbt.py docs generate
#
# ENVIRONMENT VARIABLES:
#    DBT_PROFILES_DIR: Directory chứa profiles.yml
#    CLICKHOUSE_HOST: ClickHouse host
#    CLICKHOUSE_PORT: ClickHouse port
#    CLICKHOUSE_USER: ClickHouse user
#    CLICKHOUSE_PASSWORD: ClickHouse password
# =============================================================================

import os
import subprocess
import sys
from pathlib import Path
from typing import List


def get_dbt_project_dir() -> Path:
    """
    Tìm dbt project directory.

    Returns:
        Path đến dbt project
    """
    # Thử các paths có thể
    possible_paths = [
        Path(__file__).parent.parent / "dbt",
        Path("dbt"),
        Path("com/tm/src/services/analytics-aggregator/dbt"),
    ]

    for path in possible_paths:
        if (path / "dbt_project.yml").exists():
            return path.resolve()

    raise FileNotFoundError("Cannot find dbt project (dbt_project.yml)")


def check_dbt_installed() -> bool:
    """
    Kiểm tra dbt đã được cài chưa.

    Returns:
        True nếu dbt có thể chạy
    """
    try:
        result = subprocess.run(
            ["dbt", "--version"],
            capture_output=True,
            text=True
        )
        return result.returncode == 0
    except FileNotFoundError:
        return False


def run_dbt_command(args: List[str], project_dir: Path) -> int:
    """
    Chạy dbt command.

    Args:
        args: dbt command arguments
        project_dir: Path đến dbt project

    Returns:
        Exit code
    """
    # Build full command
    cmd = [
        "dbt",
        *args,
        "--project-dir", str(project_dir),
        "--profiles-dir", str(project_dir),
    ]

    print(f"Running: {' '.join(cmd)}")
    print(f"Project dir: {project_dir}")
    print("-" * 60)

    # Run command
    result = subprocess.run(
        cmd,
        cwd=project_dir,
        env={
            **os.environ,
            # Default environment variables
            "CLICKHOUSE_HOST": os.getenv("CLICKHOUSE_HOST", "localhost"),
            "CLICKHOUSE_PORT": os.getenv("CLICKHOUSE_PORT", "8123"),
            "CLICKHOUSE_USER": os.getenv("CLICKHOUSE_USER", "default"),
            "CLICKHOUSE_PASSWORD": os.getenv("CLICKHOUSE_PASSWORD", ""),
        }
    )

    return result.returncode


def print_help():
    """Print help message."""
    print("""
dbt Runner - Run dbt commands via Bazel

Usage:
    bazel run //com/tm/src/services/analytics-aggregator:run_dbt -- <command> [options]

Commands:
    run             Run dbt models
    test            Run dbt tests
    build           Run models and tests
    compile         Compile SQL (don't execute)
    docs generate   Generate documentation
    docs serve      Serve documentation locally
    deps            Install dbt packages
    debug           Debug dbt configuration
    ls              List resources
    clean           Clean target directory

Examples:
    # Run all models
    bazel run //...:run_dbt -- run

    # Run specific models
    bazel run //...:run_dbt -- run --select staging

    # Run tests
    bazel run //...:run_dbt -- test

    # Generate docs
    bazel run //...:run_dbt -- docs generate

    # Debug
    bazel run //...:run_dbt -- debug

Environment Variables:
    CLICKHOUSE_HOST     ClickHouse host (default: localhost)
    CLICKHOUSE_PORT     ClickHouse port (default: 8123)
    CLICKHOUSE_USER     ClickHouse user (default: default)
    CLICKHOUSE_PASSWORD ClickHouse password (default: empty)
""")


def main():
    """Main entry point."""
    # Check arguments
    if len(sys.argv) < 2:
        print_help()
        sys.exit(0)

    if sys.argv[1] in ["-h", "--help", "help"]:
        print_help()
        sys.exit(0)

    # Check dbt is installed
    if not check_dbt_installed():
        print("ERROR: dbt is not installed")
        print("")
        print("Install dbt with:")
        print("  pip install dbt-core dbt-clickhouse")
        print("")
        print("Or install specific adapter:")
        print("  pip install dbt-clickhouse  # For ClickHouse")
        print("  pip install dbt-postgres    # For PostgreSQL")
        sys.exit(1)

    # Find project directory
    try:
        project_dir = get_dbt_project_dir()
    except FileNotFoundError as e:
        print(f"ERROR: {e}")
        sys.exit(1)

    # Special handling for deps command - install packages first
    if sys.argv[1] == "deps":
        packages_file = project_dir / "packages.yml"
        if packages_file.exists():
            print("Installing dbt packages...")
        else:
            print("No packages.yml found, skipping deps")
            sys.exit(0)

    # Run dbt command
    exit_code = run_dbt_command(sys.argv[1:], project_dir)

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
