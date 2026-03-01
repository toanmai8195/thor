#!/usr/bin/env python3
# =============================================================================
# DAG VALIDATION TESTS
# =============================================================================
# Tests này validate Airflow DAGs:
# - Syntax errors
# - Import errors
# - DAG structure
# - Task dependencies
#
# CÁCH CHẠY:
# 1. Với Bazel:
#    bazel test //com/tm/src/services/analytics-aggregator:dag_validation_test
#
# 2. Với pytest (trong Airflow environment):
#    cd com/tm/src/services/analytics-aggregator
#    pytest tests/test_dag_validation.py -v
#
# 3. Với Python trực tiếp:
#    python -m pytest tests/test_dag_validation.py
#
# LƯU Ý:
# - Tests này chạy mà KHÔNG cần Airflow running
# - Chỉ validate syntax và structure
# - Không test actual execution
# =============================================================================

import ast
import os
import sys
import unittest
from pathlib import Path


class TestDAGValidation(unittest.TestCase):
    """Test suite để validate Airflow DAGs."""

    @classmethod
    def setUpClass(cls):
        """Setup test fixtures."""
        # Tìm thư mục dags
        cls.dags_dir = cls._find_dags_dir()
        cls.dag_files = list(cls.dags_dir.glob("*.py")) if cls.dags_dir else []

    @classmethod
    def _find_dags_dir(cls) -> Path:
        """Tìm thư mục chứa DAG files."""
        # Thử các paths có thể
        possible_paths = [
            Path(__file__).parent.parent / "dags",
            Path("dags"),
            Path("com/tm/src/services/analytics-aggregator/dags"),
        ]

        for path in possible_paths:
            if path.exists() and path.is_dir():
                return path

        return None

    def test_dags_directory_exists(self):
        """Test: Thư mục dags tồn tại."""
        self.assertIsNotNone(
            self.dags_dir,
            "Không tìm thấy thư mục dags"
        )
        self.assertTrue(
            self.dags_dir.exists(),
            f"Thư mục {self.dags_dir} không tồn tại"
        )

    def test_dag_files_exist(self):
        """Test: Có ít nhất 1 DAG file."""
        self.assertGreater(
            len(self.dag_files),
            0,
            "Không có DAG files trong thư mục dags"
        )

    def test_dag_syntax_valid(self):
        """Test: Tất cả DAG files có syntax Python hợp lệ."""
        for dag_file in self.dag_files:
            with self.subTest(dag_file=dag_file.name):
                try:
                    with open(dag_file, "r", encoding="utf-8") as f:
                        source = f.read()
                    # Parse AST để check syntax
                    ast.parse(source)
                except SyntaxError as e:
                    self.fail(
                        f"Syntax error trong {dag_file.name}: "
                        f"Line {e.lineno}: {e.msg}"
                    )

    def test_dag_no_relative_imports(self):
        """Test: DAG files không dùng relative imports (Airflow không hỗ trợ)."""
        for dag_file in self.dag_files:
            with self.subTest(dag_file=dag_file.name):
                with open(dag_file, "r", encoding="utf-8") as f:
                    source = f.read()

                tree = ast.parse(source)

                for node in ast.walk(tree):
                    if isinstance(node, ast.ImportFrom):
                        # level > 0 = relative import
                        if node.level > 0:
                            self.fail(
                                f"Relative import trong {dag_file.name}: "
                                f"'from {'.' * node.level}{node.module or ''} import ...'"
                            )

    def test_dag_has_dag_definition(self):
        """Test: Mỗi DAG file có định nghĩa DAG."""
        for dag_file in self.dag_files:
            # Skip __init__.py và test files
            if dag_file.name.startswith("__") or dag_file.name.startswith("test_"):
                continue

            with self.subTest(dag_file=dag_file.name):
                with open(dag_file, "r", encoding="utf-8") as f:
                    source = f.read()

                # Check for DAG definition patterns
                has_dag = (
                    "DAG(" in source or
                    "@dag" in source or
                    "with DAG" in source
                )

                self.assertTrue(
                    has_dag,
                    f"Không tìm thấy DAG definition trong {dag_file.name}"
                )

    def test_dag_has_unique_dag_id(self):
        """Test: Mỗi DAG có dag_id unique."""
        dag_ids = []

        for dag_file in self.dag_files:
            if dag_file.name.startswith("__"):
                continue

            with open(dag_file, "r", encoding="utf-8") as f:
                source = f.read()

            # Simple regex-like search for dag_id
            # Format: dag_id='xxx' or dag_id="xxx"
            import re
            matches = re.findall(r"dag_id\s*=\s*['\"]([^'\"]+)['\"]", source)

            for dag_id in matches:
                with self.subTest(dag_id=dag_id):
                    self.assertNotIn(
                        dag_id,
                        dag_ids,
                        f"Duplicate dag_id: {dag_id}"
                    )
                    dag_ids.append(dag_id)

    def test_dag_no_top_level_code(self):
        """Test: DAG files không có code chạy ở top level (ngoài imports và definitions)."""
        allowed_top_level = (
            ast.Import,
            ast.ImportFrom,
            ast.FunctionDef,
            ast.AsyncFunctionDef,
            ast.ClassDef,
            ast.Assign,  # Variable assignments (like default_args)
            ast.AnnAssign,  # Annotated assignments
            ast.With,  # with DAG(...) as dag:
            ast.If,  # if __name__ == "__main__":
            ast.Expr,  # Docstrings
        )

        for dag_file in self.dag_files:
            if dag_file.name.startswith("__"):
                continue

            with self.subTest(dag_file=dag_file.name):
                with open(dag_file, "r", encoding="utf-8") as f:
                    source = f.read()

                tree = ast.parse(source)

                for node in tree.body:
                    # Check if node is allowed at top level
                    if not isinstance(node, allowed_top_level):
                        self.fail(
                            f"Unexpected top-level code trong {dag_file.name}: "
                            f"Line {node.lineno}: {type(node).__name__}"
                        )


class TestDAGStructure(unittest.TestCase):
    """Test DAG structure và best practices."""

    @classmethod
    def setUpClass(cls):
        """Setup."""
        cls.dags_dir = Path(__file__).parent.parent / "dags"

    def test_dag_has_description(self):
        """Test: DAG có description hoặc doc_md."""
        if not self.dags_dir.exists():
            self.skipTest("dags directory not found")

        for dag_file in self.dags_dir.glob("*.py"):
            if dag_file.name.startswith("__"):
                continue

            with self.subTest(dag_file=dag_file.name):
                with open(dag_file, "r", encoding="utf-8") as f:
                    source = f.read()

                has_description = (
                    "description=" in source or
                    "doc_md=" in source
                )

                self.assertTrue(
                    has_description,
                    f"DAG trong {dag_file.name} thiếu description"
                )

    def test_dag_has_owner(self):
        """Test: DAG có owner trong default_args."""
        if not self.dags_dir.exists():
            self.skipTest("dags directory not found")

        for dag_file in self.dags_dir.glob("*.py"):
            if dag_file.name.startswith("__"):
                continue

            with self.subTest(dag_file=dag_file.name):
                with open(dag_file, "r", encoding="utf-8") as f:
                    source = f.read()

                # Check for owner in default_args
                has_owner = "'owner'" in source or '"owner"' in source

                self.assertTrue(
                    has_owner,
                    f"DAG trong {dag_file.name} thiếu owner"
                )

    def test_dag_has_tags(self):
        """Test: DAG có tags để categorize."""
        if not self.dags_dir.exists():
            self.skipTest("dags directory not found")

        for dag_file in self.dags_dir.glob("*.py"):
            if dag_file.name.startswith("__"):
                continue

            with self.subTest(dag_file=dag_file.name):
                with open(dag_file, "r", encoding="utf-8") as f:
                    source = f.read()

                has_tags = "tags=" in source or "tags =" in source

                self.assertTrue(
                    has_tags,
                    f"DAG trong {dag_file.name} thiếu tags"
                )


class TestDAGImports(unittest.TestCase):
    """Test DAG có thể import được (không có missing dependencies)."""

    def test_dag_importable(self):
        """Test: DAG files có thể import (nếu có Airflow)."""
        try:
            # Try to import airflow
            import airflow
            has_airflow = True
        except ImportError:
            has_airflow = False
            self.skipTest("Airflow not installed - skipping import test")

        if has_airflow:
            dags_dir = Path(__file__).parent.parent / "dags"
            if not dags_dir.exists():
                self.skipTest("dags directory not found")

            # Add dags dir to path
            sys.path.insert(0, str(dags_dir))

            try:
                for dag_file in dags_dir.glob("*.py"):
                    if dag_file.name.startswith("__"):
                        continue

                    module_name = dag_file.stem

                    with self.subTest(module=module_name):
                        try:
                            __import__(module_name)
                        except Exception as e:
                            self.fail(f"Cannot import {module_name}: {e}")
            finally:
                # Cleanup path
                sys.path.pop(0)


if __name__ == "__main__":
    # Run tests
    unittest.main(verbosity=2)
