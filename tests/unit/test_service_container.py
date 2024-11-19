from __future__ import annotations

from unittest.mock import Mock

import pytest

from crawlee import service_container
from crawlee.configuration import Configuration, get_global_configuration, set_global_configuration
from crawlee.errors import ServiceConflictError
from crawlee.events._local_event_manager import LocalEventManager
from crawlee.memory_storage_client._memory_storage_client import MemoryStorageClient


async def test_get_event_manager() -> None:
    event_manager = service_container.get_event_manager()
    assert isinstance(event_manager, LocalEventManager)


async def test_set_event_manager() -> None:
    event_manager = Mock()
    service_container.set_event_manager(event_manager)
    assert service_container.get_event_manager() is event_manager


async def test_overwrite_event_manager() -> None:
    event_manager = Mock()
    service_container.set_event_manager(event_manager)
    service_container.set_event_manager(event_manager)

    with pytest.raises(ServiceConflictError):
        service_container.set_event_manager(Mock())


async def test_get_configuration() -> None:
    configuration = get_global_configuration()
    assert isinstance(configuration, Configuration)


async def test_set_configuration() -> None:
    configuration = Mock()
    set_global_configuration(configuration)

    assert Configuration.get_global_configuration() is get_global_configuration() is configuration


async def test_overwrite_configuration() -> None:
    configuration = Mock()
    set_global_configuration(configuration)
    set_global_configuration(configuration)

    with pytest.raises(ServiceConflictError):
        set_global_configuration(Mock())


async def test_get_storage_client() -> None:
    storage_client = service_container.get_storage_client()
    assert isinstance(storage_client, MemoryStorageClient)

    with pytest.raises(RuntimeError):
        service_container.get_storage_client(client_type='cloud')

    with pytest.raises(RuntimeError):
        service_container.get_storage_client()

    storage_client = service_container.get_storage_client(client_type='local')
    assert isinstance(storage_client, MemoryStorageClient)

    cloud_client = Mock()
    service_container.set_cloud_storage_client(cloud_client)
    assert service_container.get_storage_client(client_type='cloud') is cloud_client
    assert service_container.get_storage_client() is cloud_client


async def test_reset_local_storage_client() -> None:
    storage_client = Mock()

    service_container.set_local_storage_client(storage_client)
    service_container.set_local_storage_client(storage_client)

    with pytest.raises(ServiceConflictError):
        service_container.set_local_storage_client(Mock())


async def test_reset_cloud_storage_client() -> None:
    storage_client = Mock()

    service_container.set_cloud_storage_client(storage_client)
    service_container.set_cloud_storage_client(storage_client)

    with pytest.raises(ServiceConflictError):
        service_container.set_cloud_storage_client(Mock())
