from unittest.mock import patch

import pytest


# patch common metrics for all the tests
@pytest.fixture(autouse=True)
def patch_common_metrics() -> None:
    _ = patch("cif.api.metrics").start()
    _ = patch("cif.middleware.metrics").start()
    _ = patch("cif.main.metrics").start()
