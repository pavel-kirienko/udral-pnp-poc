#!/usr/bin/env python
# Pavel Kirienko <pavel@uavcan.org>

from __future__ import annotations
from typing import Optional, Callable, Sequence, Tuple, AbstractSet, Type, TypeVar
from itertools import count
import dataclasses
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

from .node_proxy import PortAssignment, perform_automatic_port_id_allocation


MessageClass = TypeVar("MessageClass", bound=pyuavcan.dsdl.CompositeObject)
ServiceClass = TypeVar("ServiceClass", bound=pyuavcan.dsdl.ServiceObject)


@dataclasses.dataclass(frozen=True)
class PortMapping:
    """
    If the full port name follows the pattern ``service_name.[instance_name.]suffix``,
    where the bracketed part is optional, this type provides the mapping from ``suffix`` to the full port name.
    """
    pub: dict[str, str] = dataclasses.field(default_factory=dict)
    sub: dict[str, str] = dataclasses.field(default_factory=dict)
    cln: dict[str, str] = dataclasses.field(default_factory=dict)
    srv: dict[str, str] = dataclasses.field(default_factory=dict)

    def match(self, cp: NonFixedPorts) -> NonFixedPorts:
        """
        Matches each port in the mapping with the corresponding item in the argument and returns
        the subset of the latter. Raises :class:`KeyError` if at least one port cannot be matched.
        """
        return NonFixedPorts(
            pub={k: cp.pub[k] for k in self.pub.values()},
            sub={k: cp.sub[k] for k in self.sub.values()},
            cln={k: cp.cln[k] for k in self.cln.values()},
            srv={k: cp.srv[k] for k in self.srv.values()},
        )


def detect_service_instances(remote: NonFixedPorts,
                             prefix: str,
                             pub: AbstractSet[str] = frozenset(),
                             sub: AbstractSet[str] = frozenset(),
                             cln: AbstractSet[str] = frozenset(),
                             srv: AbstractSet[str] = frozenset()) -> dict[str, PortMapping]:
    """
    This is basically the key function: it detects service instances provided by a remote node based on
    the names of its ports.

    Given a prefix and a set of port name suffixes, returns grouped ports that belong to the same service instance.
    The caller is responsible for checking if all of the required ports are available.
    The output instances are always lexicographically sorted by service name.
    Ports within each instance follow the ordering defined by the remote node.

    Here's a demo; suppose that the remote node has the following non-fixed ports:

    >>> node_ports = NonFixedPorts(
    ...     pub={
    ...         "airspeed.foo.differential_pressure":   1010,
    ...         "airspeed.foo.static_air_temperature":  1011,
    ...         "airspeed.bar.differential_pressure":   1012,
    ...         "servo.feedback":                       1202,
    ...         "servo.status":                         1203,
    ...         "servo.power":                          1204,
    ...         "servo.dynamics":                       1205,
    ...     },
    ...     sub={
    ...         "airspeed.bar.heater_state":    1000,
    ...         "servo.setpoint":               1200,
    ...         "servo.readiness":              1201,
    ...         "unrelated.subscription":       2222,
    ...     },
    ...     srv={
    ...         "unrelated.server": 123,
    ...     },
    ... )

    We check if the node provides the ``airspeed`` service as follows:

    >>> result = detect_service_instances(
    ...     node_ports,
    ...     "airspeed",     # This is the name of the service we are interested in; non-matching ports will be ignored.
    ...     pub={           # The airspeed service specification defines these ports. Other ports will be ignored.
    ...         "differential_pressure",
    ...         "static_air_temperature",
    ...     },
    ... )
    >>> list(result)        # Names of the matching services (sorted lexicographically).
    ['bar', 'foo']
    >>> result["foo"]       # The first service instance has both subjects. # doctest: +NORMALIZE_WHITESPACE
    PortMapping(pub={'differential_pressure':  'airspeed.foo.differential_pressure',
                     'static_air_temperature': 'airspeed.foo.static_air_temperature'},
                sub={},
                cln={},
                srv={})
    >>> result["bar"]       # This instance lacks the temperature subject. # doctest: +NORMALIZE_WHITESPACE
    PortMapping(pub={'differential_pressure': 'airspeed.bar.differential_pressure'},
                sub={},
                cln={},
                srv={})

    Ports that are not part of the airspeed service are ignored
    (they could be vendor-specific extensions or parts of other services).
    Now, let's check if the ``servo`` service is supported by the node:

    >>> result = detect_service_instances(
    ...     node_ports,
    ...     "servo",
    ...     pub={           # Suppose we don't need "status" in this application.
    ...         "feedback",
    ...         "power",
    ...         "dynamics",
    ...     },
    ...     sub={
    ...         "setpoint",
    ...         "readiness",
    ...     },
    ... )
    >>> list(result)    # The instance is unnamed; otherwise, it would be like "servo.name.power", not "servo.power".
    ['']
    >>> result[""]      # doctest: +NORMALIZE_WHITESPACE
    PortMapping(pub={'feedback':  'servo.feedback',
                     'power':     'servo.power',
                     'dynamics':  'servo.dynamics'},
                sub={'setpoint':  'servo.setpoint',
                     'readiness': 'servo.readiness'},
                cln={},
                srv={})

    The output says that there is one instance of the servo service provided, named "" (empty).
    As you can see, the name of a port is formed of three parts: "service_name.instance_name.suffix",
    where the instance name can be omitted (along with its separator ".") if there is only one instance of the service.
    """
    def group(port_names: Sequence[str], suffixes: AbstractSet[str]) -> dict[str, list[str]]:
        out: dict[str, list[str]] = {}
        for pn in port_names:
            if pn.startswith(prefix + "."):
                for suf in suffixes:
                    if pn.endswith("." + suf):
                        key = pn[(len(prefix) + 1) : -(len(suf) + 1)]
                        out.setdefault(key, []).append(suf)
        return out

    # Find all ports that match the given prefix and the given suffixes, group them by key.
    g_pub = group(list(remote.pub), pub)
    g_sub = group(list(remote.sub), sub)
    g_cln = group(list(remote.cln), cln)
    g_srv = group(list(remote.srv), srv)

    # Detect which service instances that match the prefix & suffixes are available on the node.
    # Sort them lexicographically to ensure deterministic ordering during allocation.
    all_keys = list(sorted(set(g_pub) | set(g_sub) | set(g_cln) | set(g_srv)))

    # Reconstruct port names from the groups by simply joining the three components together.
    def make_item(k: str, suffix: str) -> Tuple[str, str]:
        return suffix, "".join((prefix, f".{k}." if k else ".", suffix))

    return {
        key: PortMapping(
            pub=dict(make_item(key, suffix) for suffix in g_pub.get(key, [])),
            sub=dict(make_item(key, suffix) for suffix in g_sub.get(key, [])),
            cln=dict(make_item(key, suffix) for suffix in g_cln.get(key, [])),
            srv=dict(make_item(key, suffix) for suffix in g_srv.get(key, [])),
        )
        for key in all_keys
    }


async def main() -> None:
    alloc_tasks: dict[int, asyncio.Task] = {}

    with make_node(NodeInfo(name="org.uavcan.udral_pnp_demo"), "udral_pnp_demo.db") as node:
        # If a remote node stores this cookie, it does not require auto-configuration.
        # Different networks are expected to have different cookies. This ensures that an auto-configurable
        # device can be safely moved between different networks without risking configuration conflicts.
        expected_pnp_cookie = str(node.registry.setdefault("expected_pnp_cookie",
                                                           f"autoconfigured {random.getrandbits(64)}"))

        def on_node_status_change(node_id: int,
                                  _: Optional[node_tracker.Entry],
                                  entry: Optional[node_tracker.Entry]) -> None:
            if entry is None:  # The node went offline, cancel the allocation task if it's still running.
                logging.info('Node %d went offline', node_id)
                try:
                    alloc_tasks[node_id].cancel()
                    del alloc_tasks[node_id]
                except LookupError:
                    pass
            elif node_id not in alloc_tasks:  # The node is new, launch the allocation procedure.
                logging.info('Detected new online node %d', node_id)
                alloc_tasks[node_id] = asyncio.create_task(
                    perform_automatic_port_id_allocation(node,
                                                         node_id,
                                                         expected_pnp_cookie),
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
        logging.basicConfig(level=logging.INFO, format="%(levelname)-3.3s %(name)s: %(message)s")
        asyncio.get_event_loop().run_until_complete(main())
    except KeyboardInterrupt:
        pass
