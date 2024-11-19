from __future__ import annotations

from crawlee.configuration import Configuration, get_global_configuration


def test_global_configuration_works() -> None:
    assert (
        Configuration.get_global_configuration()
        is Configuration.get_global_configuration()
        is get_global_configuration()
        is get_global_configuration()
    )
