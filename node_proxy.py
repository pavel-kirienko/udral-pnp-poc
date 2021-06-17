# Pavel Kirienko <pavel@uavcan.org>

from __future__ import annotations
from typing import Optional, Callable
from itertools import count
import dataclasses
import logging
import pyuavcan  # type: ignore
import pyuavcan.application  # type: ignore
from pyuavcan.application import register, NetworkTimeoutError  # type: ignore
# DSDL compiled types:
import uavcan.node
import uavcan.register
import uavcan.primitive


_logger = logging.getLogger(__name__)


@dataclasses.dataclass(frozen=True)
class PortAssignment:
    """
    Name/ID pairs for each non-fixed port on the node.
    """
    pub: dict[str, int] = dataclasses.field(default_factory=dict)
    sub: dict[str, int] = dataclasses.field(default_factory=dict)
    cln: dict[str, int] = dataclasses.field(default_factory=dict)
    srv: dict[str, int] = dataclasses.field(default_factory=dict)


async def perform_automatic_port_id_allocation(local_node: pyuavcan.application.Node,
                                               remote_node_id: int,
                                               expected_pnp_cookie: str,
                                               service_allocator: Callable[[PortAssignment], PortAssignment]) -> None:
    """
    This function implements the interaction strategy with the remote node that may require auto-configuration.
    It is lengthy but actually very simple:

    - Check if the remote node supports auto-configuration, bail if not.
    - Check if the remote node is already configured by checking the cookie register, bail if so.
    - Fetch all registers available at the remote node.
    - Hand the registers to the service_allocator and get the new values back. This is the key part!
    - Write the new register values to the remote node and update its cookie register, too.
    - Restart the remote node to ensure the new settings are applied.

    Raises a :class:`pyuavcan.application.NetworkTimeoutError` if the remote node fails to respond.
    """
    _logger.info("Started auto-configuration of node %d", remote_node_id)

    reg_access = local_node.make_client(uavcan.register.Access_1_0, remote_node_id)
    reg_access.priority = pyuavcan.transport.Priority.SLOW
    reg_access.response_timeout = 3.0

    async def access(name: str, value: Optional[uavcan.register.Value_1_0] = None) -> uavcan.register.Value_1_0:
        resp_transfer = await reg_access.call(uavcan.register.Access_1_0.Request(uavcan.register.Name_1_0(name),
                                                                                 value=value))
        if resp_transfer is None:
            raise NetworkTimeoutError(f'Node {remote_node_id} did not respond to register request {name!r}')
        resp, _ = resp_transfer
        assert isinstance(resp, uavcan.register.Access_1_0.Response)
        return resp.value

    # Check the cookie. It should be a string register.
    value = await access("udral.pnp.cookie")
    if not value.string:
        _logger.info("Node %d is not UDRAL-PnP-capable, please configure it manually", remote_node_id)
        return
    cookie = value.string.value.tobytes().decode().strip().lower()
    if cookie == expected_pnp_cookie:
        _logger.info("Node %d is already configured for use in this network", remote_node_id)
        return
    if cookie == "reject":
        _logger.info("Node %d is manually configured, skipping auto-configuration", remote_node_id)
        return
    _logger.info("Node %d requires autoconfiguration because cookie %r != expected %r",
                 remote_node_id, cookie, expected_pnp_cookie)

    # The node has to be auto-configured. First, we need to read all of its registers to discover the ports.
    reg_list = local_node.make_client(uavcan.register.List_1_0, remote_node_id)
    reg_list.priority = pyuavcan.transport.Priority.SLOW
    reg_list.response_timeout = 3.0
    available_registers: list[str] = []
    for i in count():
        list_response_transfer = await reg_list.call(uavcan.register.List_1_0.Request(i))
        if list_response_transfer is None:
            raise NetworkTimeoutError(f"Node {remote_node_id} did not respond to register list request with index {i}")
        list_response, _ = list_response_transfer
        assert isinstance(list_response, uavcan.register.List_1_0.Response)
        nm = list_response.name.name.tobytes().decode()
        if not nm:
            break
        available_registers.append(nm)
    _logger.info("Registers available on node %d: %r", remote_node_id, available_registers)

    # Detect which ports are available based on the standard registers. See UAVCAN docs:
    # https://github.com/UAVCAN/public_regulated_data_types/blob/master/uavcan/register/384.Access.1.0.uavcan
    def extract_ports(kind: str) -> list[str]:
        s, e = f"uavcan.{kind}.", ".id"
        return [r[len(s) - 1 : -len(e)] for r in available_registers if r.startswith(s) and r.endswith(e)]

    pub, sub, cln, srv = map(extract_ports, ("pub", "sub", "cln", "srv"))

    # Query the ID of each port from the node.
    async def read_port_id(kind: str, port_name: str) -> int:
        name = f"uavcan.{kind}.{port_name}.id"
        value = await access(name)
        return int(register.ValueProxy(value))  # This form will accept any numeric value, not just natural16.

    original_ports = PortAssignment(
        pub={n: await read_port_id("pub", n) for n in pub},
        sub={n: await read_port_id("sub", n) for n in sub},
        cln={n: await read_port_id("cln", n) for n in cln},
        srv={n: await read_port_id("srv", n) for n in srv},
    )
    _logger.info("Node %d: currently configured ports: %s", remote_node_id, original_ports)

    # Update the local configuration and obtain the new remote configuration to match the local one.
    new_ports = service_allocator(original_ports)
    _logger.info("Node %d: new ports: %s", remote_node_id, new_ports)
    assert isinstance(new_ports, PortAssignment)

    # Generate the list of registers that shall be reconfigured on the remote side.
    # Note that we also rewrite the cookie (in the last order) to indicate that the node is configured correctly.
    def ports_to_registers(kind: str, ports: dict[str, int]) -> dict[str, register.Value]:
        return {
            f"uavcan.{kind}.{name}.id": register.Value(natural16=register.Natural16([pid]))
            for name, pid in ports.items()
        }

    final_registers: dict[str, register.Value] = {}
    final_registers.update(ports_to_registers("pub", new_ports.pub))
    final_registers.update(ports_to_registers("sub", new_ports.sub))
    final_registers.update(ports_to_registers("cln", new_ports.cln))
    final_registers.update(ports_to_registers("srv", new_ports.srv))
    final_registers["udral.pnp.cookie"] = register.Value(string=register.String(expected_pnp_cookie))
    _logger.info("Writing registers of node %d: %r", remote_node_id, final_registers)
    for rn, rv in final_registers.items():
        value = await access(rn, rv)
        if repr(value) != repr(rv):  # Comparison is not implemented for generated classes yet.
            _logger.error("Node %d: could not write register %r: unexpected transform: %r -> %r",
                          remote_node_id, rn, rv, value)

    # To maximize compatibility, we need to explicitly save the new values and restart the node.
    # After the node is restarted, this strategy will be executed again & exit after the cookie check in the beginning.
    command = local_node.make_client(uavcan.node.ExecuteCommand_1_1, remote_node_id)
    command.priority = pyuavcan.transport.Priority.LOW

    async def try_command(code: int) -> None:
        request = uavcan.node.ExecuteCommand_1_1.Request(command=code)
        response_transfer = await command.call(request)
        if not response_transfer:
            _logger.warning("Node %d did not respond to %r", remote_node_id, request)
        else:
            response, _ = response_transfer
            _logger.info("Node %d: command %r response: %r", request, response)

    await try_command(uavcan.node.ExecuteCommand_1_1.Request.COMMAND_STORE_PERSISTENT_STATES)
    await try_command(uavcan.node.ExecuteCommand_1_1.Request.COMMAND_RESTART)

    _logger.info("Node %d configured successfully", remote_node_id)
