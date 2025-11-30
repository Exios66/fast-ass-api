# tests/conftest.py
import os
import shutil
import tempfile
import pytest

@pytest.fixture(scope="session")
def tmp_data_dir():
    d = tempfile.mkdtemp(prefix="csv_data_")
    yield d
    # teardown
    try:
        shutil.rmtree(d)
    except Exception:
        pass

@pytest.fixture(autouse=True)
def set_env_tmp_dir(tmp_data_dir, monkeypatch):
    """
    Set the environment variable used by main.py to point to the temporary directory.
    This fixture runs automatically for all tests so that main.py uses the isolated dir.
    """
    monkeypatch.setenv("CSV_DATA_DIR", tmp_data_dir)
    # also ensure major libs see the env change immediately
    yield
