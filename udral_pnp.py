#!/usr/bin/env python
# Pavel Kirienko <pavel@uavcan.org>

from __future__ import annotations
from typing import Optional, Callable, Sequence, Tuple, AbstractSet, Type, TypeVar
try:
    from pprint import pformat
except ImportError:
    pformat = str
import random
import logging
import asyncio
import pyuavcan  # type: ignore
import pyuavcan.application  # type: ignore
from pyuavcan.presentation import Publisher, Subscriber  # type: ignore
from pyuavcan.application import make_node, NodeInfo, register, node_tracker  # type: ignore
# DSDL compiled types:
import uavcan
import uavcan.node
import uavcan.register
import uavcan.primitive
import uavcan.si.sample.temperature
import uavcan.si.sample.pressure
import zubax.physics.dynamics.translation

from node_proxy import PortAssignment, perform_automatic_port_id_allocation
from service_detector import PortSuffixMapping, detect_service_instances


PORT_ID_UNSET = 0xFFFF


MessageClass = TypeVar("MessageClass", bound=pyuavcan.dsdl.CompositeObject)
ServiceClass = TypeVar("ServiceClass", bound=pyuavcan.dsdl.ServiceObject)


def match_port_assignment(source: PortAssignment) -> PortAssignment:
    pass


class AirspeedClient:
    def __init__(self, local_node: pyuavcan.application.Node, prefix: str) -> None:
        """
        :raises: :class:`pyuavcan.application.PortNotConfiguredError` if a mandatory port is not configured.
        """
        self._node = local_node

        self._sub_dp = self._node.make_subscriber(uavcan.si.sample.pressure.Scalar_1_0, f"{prefix}.diff_pressure")
        self._sub_dp.receive_in_background(lambda msg, _trans: print("Airspeed", prefix, msg))

        # Suppose that the temperature subject is optional: if not configured, simply ignore it.
        try:
            self._sub_temp: Subscriber[uavcan.si.sample.temperature.Scalar_1_0] = self._node.make_subscriber(
                uavcan.si.sample.temperature.Scalar_1_0,
                f"{prefix}.temperature",
            )
            self._sub_temp.receive_in_background(lambda msg, _trans: print("Airspeed", prefix, msg))
        except pyuavcan.application.PortNotConfiguredError:
            self._sub_temp = None

    @staticmethod
    def instantiate_if_enabled(local_node: pyuavcan.application.Node, prefix: str) -> Optional[AirspeedClient]:
        try:
            return AirspeedClient(local_node, prefix)
        except pyuavcan.application.PortNotConfiguredError:
            return None


class ServoClient:
    def __init__(self, local_node: pyuavcan.application.Node, prefix: str) -> None:
        """
        :raises: :class:`pyuavcan.application.PortNotConfiguredError` if a mandatory port is not configured.
        """
        self._node = local_node

        self._sub_dn = self._node.make_subscriber(zubax.physics.dynamics.translation.LinearTs_0_1, f"{prefix}.dynamics")
        self._sub_dn.receive_in_background(lambda msg, _trans: print("Servo", prefix, msg))

        self._pub_sp = self._node.make_publisher(zubax.physics.dynamics.translation.Linear_0_1, f"{prefix}.setpoint")
        self._pub_sp.priority = pyuavcan.transport.Priority.HIGH
        self._pub_sp.send_timeout = 0.1

    def send_setpoint(self, position: float, velocity: float, acceleration: float, force: float) -> None:
        msg = zubax.physics.dynamics.translation.Linear_0_1()
        msg.kinematics.position.meter                           = position
        msg.kinematics.velocity.meter_per_second                = velocity
        msg.kinematics.acceleration.meter_per_second_per_second = acceleration
        msg.force.newton                                        = force
        self._pub_sp.publish_soon(msg)

    @staticmethod
    def instantiate_if_enabled(local_node: pyuavcan.application.Node, prefix: str) -> Optional[ServoClient]:
        try:
            return ServoClient(local_node, prefix)
        except pyuavcan.application.PortNotConfiguredError:
            return None


async def main() -> None:
    alloc_tasks: dict[int, asyncio.Task] = {}

    with make_node(NodeInfo(name="org.uavcan.udral_pnp_demo"), "udral_pnp_demo.db") as node:
        # If a remote node stores this cookie, it does not require auto-configuration.
        # Different networks are expected to have different cookies. This ensures that an auto-configurable
        # device can be safely moved between different networks without risking configuration conflicts.
        expected_pnp_cookie = str(node.registry.setdefault("expected_pnp_cookie",
                                                           f"autoconfigured {random.getrandbits(32):08x}"))

        # Application logic -- here we instantiate service client endpoints.
        airspeed = [
            AirspeedClient.instantiate_if_enabled(node, f"airspeed.{i}") for i in range(2)
        ]
        servo = [
            ServoClient.instantiate_if_enabled(node, f"servo.{i}") for i in range(3)
        ]

        def allocate_services(remote_node_id: int, ports: PortAssignment) -> PortAssignment:
            logging.info("Allocating services of remote node %d; available ports: %s", remote_node_id, ports)
            services = detect_service_instances(
                pub=ports.pub,
                sub=ports.sub,
                cln=ports.cln,
                srv=ports.srv,
            )
            logging.info("Detected services on node %d:\n%s", remote_node_id, pformat(services))
            # TODO: perform allocation
            return PortAssignment()

        def on_node_status_change(remote_node_id: int,
                                  _: Optional[node_tracker.Entry],
                                  entry: Optional[node_tracker.Entry]) -> None:
            if entry is None:  # The node went offline, cancel the allocation task if it's still running.
                logging.info('Node %d went offline', remote_node_id)
                try:
                    alloc_tasks[remote_node_id].cancel()
                    del alloc_tasks[remote_node_id]
                except LookupError:
                    pass
            elif remote_node_id not in alloc_tasks:  # The node is new, launch the allocation procedure.
                logging.info('Detected new online node %d', remote_node_id)
                alloc_tasks[remote_node_id] = asyncio.create_task(
                    perform_automatic_port_id_allocation(node,
                                                         remote_node_id,
                                                         expected_pnp_cookie,
                                                         allocate_services),
                )

        trk = node_tracker.NodeTracker(node)
        trk.add_update_handler(on_node_status_change)

        while True:
            await asyncio.sleep(1.0)
            for k in list(alloc_tasks):
                t = alloc_tasks[k]
                if t.done():
                    try:
                        t.result()
                    except Exception as ex:
                        logging.exception("Allocation task for node %d failed (will retry next time): %s", k, ex)
                    del alloc_tasks[k]


if __name__ == "__main__":
    try:
        logging.basicConfig(level=logging.INFO, format="%(levelname)-3.3s %(name)s: %(message)s\n")
        asyncio.get_event_loop().run_until_complete(main())
    except KeyboardInterrupt:
        pass
