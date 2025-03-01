from contextlib import contextmanager

import pytest

try:
    from jumpstarter.common.utils import serve
    from jumpstarter.config import ExporterConfigV1Alpha1DriverInstance
except ImportError:
    # some packages in the workspace does not depend on jumpstarter
    pass
else:

    @contextmanager
    def run(config):
        with serve(ExporterConfigV1Alpha1DriverInstance.from_str(config).instantiate()) as client:
            yield client

    @pytest.fixture(autouse=True)
    def jumpstarter_namespace(doctest_namespace):
        doctest_namespace["serve"] = serve
        doctest_namespace["run"] = run
