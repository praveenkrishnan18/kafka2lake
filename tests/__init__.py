"""
tests/
──────
Pytest test suite for the CDC pipeline.
All external services (PostgreSQL, Kafka, ADLS) are mocked.
"""

import os
import sys

# Ensure the project root is on sys.path so all modules are importable.
_REPO_ROOT = os.path.dirname(os.path.dirname(__file__))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)
