#!/usr/bin/env python
# Pavel Kirienko <pavel@uavcan.org>

from __future__ import annotations
from typing import Optional, TypeVar
try:
    from pprint import pformat
except ImportError:
    pformat = str  # type: ignore
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
from service_discoverer import discover_service_instances


PORT_ID_UNSET = 0xFFFF

MessageClass = TypeVar("MessageClass", bound=pyuavcan.dsdl.CompositeObject)
ServiceClass = TypeVar("ServiceClass", bound=pyuavcan.dsdl.ServiceObject)


class AirspeedClient:
    def __init__(self, local_node: pyuavcan.application.Node, prefix: str) -> None:
        """
        :raises: :class:`pyuavcan.application.PortNotConfiguredError` if a mandatory port is not configured.
        """
        self._node = local_node
        self._prefix = prefix

        self._sub_dp = self._node.make_subscriber(uavcan.si.sample.pressure.Scalar_1_0, f"{prefix}.diff_pressure")
        self._sub_dp.receive_in_background(self._on_diff_pressure)

        # Suppose that the temperature subject is optional: if not configured, simply ignore it.
        try:
            self._sub_temp: Subscriber[uavcan.si.sample.temperature.Scalar_1_0] = self._node.make_subscriber(
                uavcan.si.sample.temperature.Scalar_1_0,
                f"{prefix}.temperature",
            )
            self._sub_temp.receive_in_background(self._on_temperature)
        except pyuavcan.application.PortNotConfiguredError:
            self._sub_temp = None

    async def _on_diff_pressure(self,
                                msg: uavcan.si.sample.pressure.Scalar_1_0,
                                meta: pyuavcan.transport.TransferFrom) -> None:
        print("Airspeed", self._prefix, msg, meta)

    async def _on_temperature(self,
                              msg: uavcan.si.sample.temperature.Scalar_1_0,
                              meta: pyuavcan.transport.TransferFrom) -> None:
        print("Airspeed", self._prefix, msg, meta)

    @staticmethod
    def instantiate_if_enabled(local_node: pyuavcan.application.Node, prefix: str) -> Optional[AirspeedClient]:
        try:
            return AirspeedClient(local_node, prefix)
        except pyuavcan.application.PortNotConfiguredError:
            return None

    def __repr__(self) -> str:
        return pyuavcan.util.repr_attributes(self, diff_pressure=self._sub_dp, temperature=self._sub_temp)


class ServoClient:
    def __init__(self, local_node: pyuavcan.application.Node, prefix: str) -> None:
        """
        :raises: :class:`pyuavcan.application.PortNotConfiguredError` if a mandatory port is not configured.
        """
        self._node = local_node
        self._prefix = prefix

        self._sub_dn = self._node.make_subscriber(zubax.physics.dynamics.translation.LinearTs_0_1, f"{prefix}.dynamics")
        self._sub_dn.receive_in_background(self._on_dynamics)

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

    async def _on_dynamics(self,
                           msg: zubax.physics.dynamics.translation.Linear_0_1,
                           meta: pyuavcan.transport.TransferFrom) -> None:
        print("Servo", self._prefix, msg, meta)

    @staticmethod
    def instantiate_if_enabled(local_node: pyuavcan.application.Node, prefix: str) -> Optional[ServoClient]:
        try:
            return ServoClient(local_node, prefix)
        except pyuavcan.application.PortNotConfiguredError:
            return None

    def __repr__(self) -> str:
        return pyuavcan.util.repr_attributes(self, dynamics=self._sub_dn, setpoint=self._pub_sp)


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
            AirspeedClient.instantiate_if_enabled(node, f"airspd.{i}") for i in range(2)
        ]
        servo = [
            ServoClient.instantiate_if_enabled(node, f"servo.{i}") for i in range(3)
        ]

        def allocate_services(remote_node_id: int,
                              service_instance_prefixes: dict[str, list[str]],
                              ports: PortAssignment) -> PortAssignment:
            logging.info("Allocating services of remote node %d; available ports: %s", remote_node_id, ports)
            services = discover_service_instances(service_instance_prefixes,
                                                  pub=ports.pub,
                                                  sub=ports.sub,
                                                  cln=ports.cln,
                                                  srv=ports.srv)
            logging.info("Discovered services on node %d:\n%s", remote_node_id, pformat(services))
            final = PortAssignment()

            # Allocate airspeed services if provided by the node.
            for instance_name, psm in services.get("reg.udral.service.pitot", {}).items():
                if "differential_pressure" not in psm.pub:
                    logging.warning("Differential pressure subject not found in %r %r", instance_name, psm)
                    continue
                try:
                    free_index = airspeed.index(None)
                except ValueError:
                    logging.warning("Cannot allocate airspeed service %r %r because no free slots are available",
                                    instance_name, psm)
                    break
                # FIXME There is a bunch of leaky logic here: we rely on the same subject names here and in the client.
                id_diff_pres = 6010 + free_index
                id_temp      = 6020 + free_index
                prefix = f"airspd.{free_index}"
                node.registry[f"uavcan.sub.{prefix}.diff_pressure.id"] = id_diff_pres
                final.pub[psm.pub["differential_pressure"]]            = id_diff_pres
                if "static_air_temperature" in psm.pub:
                    node.registry[f"uavcan.sub.{prefix}.temperature.id"] = id_temp
                    final.pub[psm.pub["static_air_temperature"]]         = id_temp
                airspeed[free_index] = AirspeedClient(node, prefix)
                logging.warning("New airspeed client of node %d: %r", remote_node_id, airspeed[free_index])

            # Allocate servo services if provided by the node.
            for instance_name, psm in services.get("reg.udral.service.actuator.servo", {}).items():
                if "dynamics" not in psm.pub:
                    logging.warning("Dynamics subject not found in %r %r", instance_name, psm)
                    continue
                if "setpoint" not in psm.sub:  # "sub" because servos subscribe to setpoint
                    logging.warning("Setpoint subject not found in %r %r", instance_name, psm)
                    continue
                try:
                    free_index = servo.index(None)
                except ValueError:
                    logging.warning("Cannot allocate servo service %r %r because no free slots are available",
                                    instance_name, psm)
                    break
                # FIXME There is a bunch of leaky logic here: we rely on the same subject names here and in the client.
                id_dynamics = 5000 + free_index
                id_setpoint = 5050 + free_index
                prefix = f"servo.{free_index}"
                # Mind the difference between pub/sub: we subscribe to dynamics and publish the setpoint!
                node.registry[f"uavcan.sub.{prefix}.dynamics.id"] = id_dynamics
                node.registry[f"uavcan.pub.{prefix}.setpoint.id"] = id_setpoint
                final.pub[psm.pub["dynamics"]]                    = id_dynamics
                final.sub[psm.sub["setpoint"]]                    = id_setpoint
                servo[free_index] = ServoClient(node, prefix)
                logging.warning("New servo client of node %d: %r", remote_node_id, airspeed[free_index])

            return final

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
            await asyncio.sleep(0.5)
            for k in list(alloc_tasks):
                t = alloc_tasks[k]
                if t.done():
                    try:
                        t.result()
                    except Exception as ex:
                        logging.exception("Allocation task for node %d failed (will retry next time): %s", k, ex)
                    del alloc_tasks[k]
            for s in servo:
                if s:
                    s.send_setpoint(position=random.random(), velocity=10.0, acceleration=1.0, force=float("nan"))


if __name__ == "__main__":
    try:
        logging.basicConfig(level=logging.INFO, format="%(levelname)-3.3s %(name)s: %(message)s\n")
        asyncio.get_event_loop().run_until_complete(main())
    except KeyboardInterrupt:
        pass
