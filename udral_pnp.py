#!/usr/bin/env python
# Pavel Kirienko <pavel@uavcan.org>

from __future__ import annotations
from typing import Optional, Callable, Sequence, Tuple, AbstractSet, Type, TypeVar
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


MessageClass = TypeVar("MessageClass", bound=pyuavcan.dsdl.CompositeObject)
ServiceClass = TypeVar("ServiceClass", bound=pyuavcan.dsdl.ServiceObject)


def match_port_assignment(source: PortAssignment) -> PortAssignment:
    pass


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
