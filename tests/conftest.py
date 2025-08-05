"""
Simple pytest configuration for YouNiverse Dataset Enrichment
"""

import pytest
import tempfile
import shutil
from pathlib import Path


@pytest.fixture
def temp_dir():
    """Create a temporary directory for testing"""
    temp_path = Path(tempfile.mkdtemp())
    yield temp_path
    shutil.rmtree(temp_path, ignore_errors=True)


@pytest.fixture
def sample_channels():
    """Sample channel data for testing"""
    return [
        "UCuAXFkgsw1L7xaCfnd5JJOw",  # Real channel ID
        "UCX6OQ3DkcsbYNE6H8uQQuVA",  # Real channel ID
        "UCYO_jab_esuFRV4b17AJtAw",  # Real channel ID
    ]