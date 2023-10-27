import asyncio
import sys
import threading
from concurrent.futures import Future
from functools import cached_property
from typing import Optional, Any, Coroutine

from bleak import BleakScanner, BleakClient


class _Manager:

    def __init__(self):
        self._managerLoopEvent = threading.Event()
        self._managerLoop: Optional[asyncio.AbstractEventLoop] = None

    @property
    def loop(self):
        self._managerLoopEvent.wait()
        return self._managerLoop

    @loop.setter
    def loop(self, value):
        self._managerLoop = value
        if value:
            self._managerLoopEvent.set()
        else:
            self._managerLoopEvent.clear()


manager = _Manager()


def run_blocking(coro: Coroutine[Any, Any, Any]):
    future = run_in_background(coro)
    result = future.result()
    return result


def run_in_background(coro: Coroutine[Any, Any, None]) -> Future:
    return asyncio.run_coroutine_threadsafe(coro, manager.loop)


class Descriptor:
    """
    Represents a GATT Descriptor which can contain metadata or configuration of its characteristic.
    """

    def __init__(self, bleak_client, bleak_descriptor):
        self._bleak_client = bleak_client
        self._bleak_descriptor = bleak_descriptor

    def read_value(self):
        """
        Reads the value of this descriptor.

        When successful, the value will be returned, otherwise `descriptor_read_value_failed()` of the related
        device is invoked.
        """
        return run_blocking(self._bleak_client.read_gatt_descriptor(self._bleak_descriptor.handle))


class Characteristic:
    """
    Represents a GATT characteristic.
    """

    def __init__(self, bleak_client, bleak_characteristics, device):
        self._bleak_client = bleak_client
        self._bleak_characteristics = bleak_characteristics
        self._device = device

    def _get_uuid(self):
        return self._bleak_characteristics.uuid

    uuid = property(_get_uuid)

    def _descriptor_factory(self):
        return Descriptor(self._bleak_client, self._bleak_characteristics.descriptor)

    descriptor = property(_descriptor_factory)

    def properties_changed(self, properties, changed_properties, invalidated_properties):
        """
        Called when a Characteristic property has changed.
        """
        pass

    def read_value(self):
        """
        Reads the value of this characteristic.

        When successful, `characteristic_value_updated()` of the related device will be called,
        otherwise `characteristic_read_value_failed()` is invoked.
        """
        value = None
        try:
            value = run_blocking(self._bleak_client.read_gatt_char(self._bleak_characteristics))
            self._device.characteristic_value_updated(self, value)
        except Exception as ex:
            pass
        return value

    def write_value(self, value):
        """
        Attempts to write a value to the characteristic.

        Success or failure will be notified by calls to `write_value_succeeded` or `write_value_failed` respectively.

        :param value: array of bytes to be written
        :param offset: offset from where to start writing the bytes (defaults to 0)
        """
        result = None
        try:
            result = run_blocking(self._bleak_client.write_gatt_char(self._bleak_characteristics, value))
            self._device.characteristic_write_value_succeeded(self)
        except Exception as ex:
            self._device.characteristic_write_value_failed(self, ex)
        return result

    def enable_notifications(self, enabled=True):
        """
        Enables or disables value change notifications.

        Success or failure will be notified by calls to `characteristic_enable_notifications_succeeded`
        or `enable_notifications_failed` respectively.

        Each time when the device notifies a new value, `characteristic_value_updated()` of the related
        device will be called.
        """
        pass


class Service:
    """
    Represents a GATT service.
    """

    def __init__(self, bleak_clieant, bleak_service, device):
        self._bleak_client = bleak_clieant
        self._bleak_service = bleak_service
        self._device = device
        self.characteristics_resolved()

    def _characteristics_factory(self, bleak_characteristics):
        return Characteristic(self._bleak_client, bleak_characteristics, self._device)

    def _get_characteristics(self):
        return map(self._characteristics_factory, self._bleak_service.characteristics)

    characteristics = property(_get_characteristics)

    def characteristics_resolved(self):
        """
        Called when all service's characteristics got resolved.
        """
        pass


pending_devices = 0


class Device:
    mac_address: str

    def __init__(self, mac_address, manager, managed=True):
        """
        Represents a BLE GATT device.

        This class is intended to be sublcassed with a device-specific implementations
        that reflect the device's GATT profile.

        :param mac_address: MAC address of this device
        :manager: `DeviceManager` that shall manage this device
        :managed: If False, the created device will not be managed by the device manager
                  Particularly of interest for subclasses of `DeviceManager` who want
                  to decide on certain device properties if they then create a subclass
                  instance of that `Device` or not.
        """

        self.mac_address = mac_address
        self.manager = manager
        global pending_devices
        pending_devices = pending_devices + 1
        print("New pening device {}: {}".format(mac_address, pending_devices), file=sys.stderr)

    @cached_property
    def _bleak_device(self):
        global pending_devices
        device = run_blocking(BleakScanner.find_device_by_address(device_identifier=self.mac_address))
        pending_devices = pending_devices - 1
        if device:
            print("Finished pending device \"{}\" {}: {}".format(device.name, device.address, pending_devices), file=sys.stderr)
        else:
            print("Not found pending device {}: {}".format(self.mac_address, pending_devices), file=sys.stderr)
        return device

    @cached_property
    def _bleak_client(self):
        if self._bleak_device:
            return BleakClient(self._bleak_device)
        else:
            return None

    def _service_factory(self, bleak_service):
        return Service(self._bleak_client, bleak_service, self)

    def _get_services(self):
        if self._bleak_client:
            return map(self._service_factory, self._bleak_client.services)
        else:
            return []

    services = property(_get_services)

    def advertised(self):
        """
        Called when an advertisement package has been received from the device. Requires device discovery to run.
        """
        pass

    def connect(self):
        """
        Connects to the device. Blocks until the connection was successful.
        """
        if self._bleak_client:
            try:
                run_blocking(self._bleak_client.connect())
            except Exception as e:
                self.connect_failed(e)
            else:
                self.connect_succeeded()
                self.services_resolved()
        else:
            self.connect_failed("No bleak client")

    def connect_succeeded(self):
        """
        Will be called when `connect()` has finished connecting to the device.
        Will not be called if the device was already connected.
        """
        pass

    def connect_failed(self, error):
        """
        Called when the connection could not be established.
        """
        pass

    def disconnect(self):
        """
        Disconnects from the device, if connected.
        """
        if self._bleak_client:
            run_blocking(self._bleak_client.disconnect())
        if not self.is_connected():
            self.disconnect_succeeded()

    def disconnect_succeeded(self):
        """
        Will be called when the device has disconnected.
        """
        pass

    def is_connected(self):
        """
        Returns `True` if the device is connected, otherwise `False`.
        """
        return self._bleak_client and self._bleak_client.is_connected

    def is_services_resolved(self):
        """
        Returns `True` is services are discovered, otherwise `False`.
        """
        return self.is_connected()

    def alias(self):
        """
        Returns the device's alias (name).
        """
        if self._bleak_device:
            return self._bleak_device.name
        else:
            return "None"

    def properties_changed(self, sender, changed_properties, invalidated_properties):
        """
        Called when a device property has changed or got invalidated.
        """
        pass

    def services_resolved(self):
        """
        Called when all device's services and characteristics got resolved.
        """
        pass

    def characteristic_value_updated(self, characteristic, value):
        """
        Called when a characteristic value has changed.
        """
        # To be implemented by subclass
        pass

    def characteristic_read_value_failed(self, characteristic, error):
        """
        Called when a characteristic value read command failed.
        """
        # To be implemented by subclass
        pass

    def characteristic_write_value_succeeded(self, characteristic):
        """
        Called when a characteristic value write command succeeded.
        """
        # To be implemented by subclass
        pass

    def characteristic_write_value_failed(self, characteristic, error):
        """
        Called when a characteristic value write command failed.
        """
        # To be implemented by subclass
        pass

    def characteristic_enable_notifications_succeeded(self, characteristic):
        """
        Called when a characteristic notifications enable command succeeded.
        """
        # To be implemented by subclass
        pass

    def characteristic_enable_notifications_failed(self, characteristic, error):
        """
        Called when a characteristic notifications enable command failed.
        """
        # To be implemented by subclass
        pass

    def descriptor_read_value_failed(self, descriptor, error):
        """
        Called when a descriptor read command failed.
        """
        # To be implemented by subclass
        pass


class DeviceManager:
    """
    Entry point for managing BLE GATT devices.

    This class is intended to be subclassed to manage a specific set of GATT devices.
    """
    adapter_name: str
    listener = None
    powered = True
    _stop_scanning_event: Optional[asyncio.Event] = None
    _devices: dict[str, Device] = {}
    _scanner_task = None

    def __init__(self, adapter_name):
        self.adapter_name = adapter_name
        self.update_devices()

    @staticmethod
    def run():
        """
        Starts the main loop that is necessary to receive Bluetooth events from the Bluetooth adapter.

        This call blocks until you call `stop()` to stop the main loop.
        """
        global manager
        manager.loop = asyncio.new_event_loop()
        print("Manager loop created", file=sys.stderr)
        manager.loop.run_forever()

    @staticmethod
    def stop():
        """
        Stops the main loop started with `start()`
        """
        global manager
        manager.loop.call_soon_threadsafe(manager.loop.stop)
        print("Manager loop stopped", file=sys.stderr)

    def update_devices(self):
        pass

    def devices(self):
        """
        Returns all known Bluetooth devices.
        """
        return self._devices.values()

    def _discovery_callback(self, device, advertising_data):
        # TODO: do something with incoming data
        if device.address not in self._devices:
            self._devices[device.address] = self.make_device(device.address)
        pass

    async def _start_discovery(self, service_uuids=[]):
        self._stop_scanning_event = asyncio.Event()
        async with BleakScanner(self._discovery_callback, service_uuids=service_uuids, adapter = self.adapter_name) as _:
            await self._stop_scanning_event.wait()

    def start_discovery(self, service_uuids=[]):
        """Starts a discovery for BLE devices with given service UUIDs.

        :param service_uuids: Filters the search to only return devices with given UUIDs.
        """
        print("Scanning started", file=sys.stderr)
        self._scanner_task = run_in_background(self._start_discovery(service_uuids))

    async def _stop_discovery(self):
        self._stop_scanning_event.set()
        _stop_scanning_event = None

    def stop_discovery(self):
        """
        Stops the discovery started with `start_discovery`
        """
        run_blocking(self._stop_discovery())
        try:
            self._scanner_task.result()
        except Exception as e:
            print("Stopping the scanner raised: {}".format(e))
        self._scanner_task = None
        print("Scanning stopped", file=sys.stderr)

    def device_discovered(self, device):
        pass

    def make_device(self, mac_address):
        """
        Makes and returns a `Device` instance with specified MAC address.

        Override this method to return a specific subclass instance of `Device`.
        Return `None` if the specified device shall not be supported by this class.
        """
        return Device(mac_address=mac_address, manager=self)

    def remove_all_devices(self, skip_alias=None):
        pass
