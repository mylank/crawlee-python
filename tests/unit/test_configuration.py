from __future__ import annotations

from crawlee.configuration import Configuration
from crawlee.service_container import get_configuration


def test_global_configuration_works() -> None:
    assert (
        Configuration.get_global_configuration()
        is Configuration.get_global_configuration()
        is get_configuration()
        is get_configuration()
    )
