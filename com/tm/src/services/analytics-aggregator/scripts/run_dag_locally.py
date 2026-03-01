#!/usr/bin/env python3
# =============================================================================
# RUN DAG LOCALLY
# =============================================================================
# Script để test run Airflow DAG locally mà không cần full Airflow setup.
#
# CÁCH DÙNG:
#
# 1. Với Bazel:
#    bazel run //com/tm/src/services/analytics-aggregator:run_dag
#
# 2. Với Python:
#    python scripts/run_dag_locally.py
#
# 3. Test specific task:
#    python scripts/run_dag_locally.py --task=dbt_run_staging
#
# LƯU Ý:
# - Script này simulate DAG execution
# - Không có full Airflow features (XCom, connections, etc.)
# - Dùng để test logic, không phải production
# =============================================================================

import argparse
import importlib.util
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional


def load_dag_module(dag_path: Path) -> Any:
    """
    Load DAG module từ file path.

    Args:
        dag_path: Path đến DAG file

    Returns:
        Module object
    """
    spec = importlib.util.spec_from_file_location("dag_module", dag_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load module from {dag_path}")

    module = importlib.util.module_from_spec(spec)
    sys.modules["dag_module"] = module
    spec.loader.exec_module(module)

    return module


def find_dags_in_module(module: Any) -> List[Any]:
    """
    Tìm tất cả DAG objects trong module.

    Args:
        module: Python module

    Returns:
        List of DAG objects
    """
    dags = []

    for name in dir(module):
        obj = getattr(module, name)
        # Check if object is a DAG
        if hasattr(obj, "dag_id") and hasattr(obj, "tasks"):
            dags.append(obj)

    return dags


def print_dag_info(dag: Any) -> None:
    """
    In thông tin về DAG.

    Args:
        dag: DAG object
    """
    print("\n" + "=" * 60)
    print(f"DAG: {dag.dag_id}")
    print("=" * 60)

    if hasattr(dag, "description") and dag.description:
        print(f"Description: {dag.description}")

    if hasattr(dag, "schedule_interval"):
        print(f"Schedule: {dag.schedule_interval}")

    if hasattr(dag, "tags") and dag.tags:
        print(f"Tags: {', '.join(dag.tags)}")

    print(f"\nTasks ({len(dag.tasks)}):")
    for task in dag.tasks:
        task_id = task.task_id if hasattr(task, "task_id") else str(task)
        print(f"  - {task_id}")

    # Print task dependencies
    print("\nTask Dependencies:")
    for task in dag.tasks:
        if hasattr(task, "upstream_list") and task.upstream_list:
            upstream = [t.task_id for t in task.upstream_list]
            print(f"  {task.task_id} <- {upstream}")
        elif hasattr(task, "downstream_list") and task.downstream_list:
            downstream = [t.task_id for t in task.downstream_list]
            print(f"  {task.task_id} -> {downstream}")


def simulate_task_execution(task: Any, context: Dict[str, Any]) -> None:
    """
    Simulate task execution.

    Args:
        task: Task object
        context: Airflow context dict
    """
    task_id = task.task_id if hasattr(task, "task_id") else str(task)
    print(f"\n>>> Executing task: {task_id}")

    try:
        # Check task type and execute accordingly
        if hasattr(task, "python_callable"):
            # PythonOperator
            callable_fn = task.python_callable
            print(f"    Type: PythonOperator")
            print(f"    Callable: {callable_fn.__name__}")

            # Call the function with context
            result = callable_fn(**context)
            print(f"    Result: {result}")

        elif hasattr(task, "bash_command"):
            # BashOperator
            print(f"    Type: BashOperator")
            print(f"    Command: {task.bash_command[:100]}...")
            print("    [Simulated - not executing actual command]")

        elif hasattr(task, "python_callable") is False and hasattr(task, "poke"):
            # Sensor
            print(f"    Type: Sensor")
            print("    [Simulated - returning True]")

        else:
            print(f"    Type: {type(task).__name__}")
            print("    [Simulated]")

        print(f"<<< Task {task_id} completed")

    except Exception as e:
        print(f"!!! Task {task_id} failed: {e}")
        raise


def run_dag(dag: Any, task_id: Optional[str] = None) -> None:
    """
    Run DAG hoặc specific task.

    Args:
        dag: DAG object
        task_id: Optional task ID để run
    """
    # Create mock context
    context = {
        "ds": datetime.now().strftime("%Y-%m-%d"),
        "ds_nodash": datetime.now().strftime("%Y%m%d"),
        "execution_date": datetime.now(),
        "dag": dag,
        "task_instance": None,
        "params": {},
        "var": {},
        "conf": {},
    }

    print("\n" + "=" * 60)
    print(f"Running DAG: {dag.dag_id}")
    print(f"Execution Date: {context['ds']}")
    print("=" * 60)

    if task_id:
        # Run specific task
        task = None
        for t in dag.tasks:
            if hasattr(t, "task_id") and t.task_id == task_id:
                task = t
                break

        if task is None:
            print(f"Task '{task_id}' not found in DAG")
            return

        simulate_task_execution(task, context)

    else:
        # Run all tasks in topological order
        # Simple implementation - doesn't handle complex dependencies
        executed = set()

        def can_execute(task: Any) -> bool:
            """Check if task can be executed."""
            if not hasattr(task, "upstream_list"):
                return True
            for upstream in task.upstream_list:
                if upstream.task_id not in executed:
                    return False
            return True

        remaining = list(dag.tasks)
        max_iterations = len(remaining) * 2  # Prevent infinite loop

        iteration = 0
        while remaining and iteration < max_iterations:
            iteration += 1

            for task in remaining[:]:  # Copy list for iteration
                if can_execute(task):
                    simulate_task_execution(task, context)
                    executed.add(task.task_id)
                    remaining.remove(task)

        if remaining:
            print(f"\nWARNING: Could not execute tasks: {[t.task_id for t in remaining]}")

    print("\n" + "=" * 60)
    print("DAG execution completed")
    print("=" * 60)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Run Airflow DAG locally for testing"
    )
    parser.add_argument(
        "--dag",
        type=str,
        default="dropshipping_analytics_dag.py",
        help="DAG file name (default: dropshipping_analytics_dag.py)"
    )
    parser.add_argument(
        "--task",
        type=str,
        default=None,
        help="Specific task ID to run (optional)"
    )
    parser.add_argument(
        "--info",
        action="store_true",
        help="Only print DAG info, don't execute"
    )

    args = parser.parse_args()

    # Find DAG file
    dags_dir = Path(__file__).parent.parent / "dags"
    dag_path = dags_dir / args.dag

    if not dag_path.exists():
        print(f"DAG file not found: {dag_path}")
        sys.exit(1)

    print(f"Loading DAG from: {dag_path}")

    try:
        # Mock Airflow imports if not available
        try:
            import airflow
        except ImportError:
            print("Airflow not installed - using mock imports")
            # Create mock airflow module
            import types
            airflow = types.ModuleType("airflow")
            sys.modules["airflow"] = airflow

        # Load DAG module
        module = load_dag_module(dag_path)

        # Find DAGs
        dags = find_dags_in_module(module)

        if not dags:
            print("No DAGs found in module")
            print("Note: This script works best with actual Airflow installed")
            sys.exit(1)

        for dag in dags:
            print_dag_info(dag)

            if not args.info:
                run_dag(dag, args.task)

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
