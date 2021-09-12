# Pavel Kirienko <pavel@uavcan.org>

from __future__ import annotations
import dataclasses
from typing import Iterable, Tuple
from functools import partial


@dataclasses.dataclass(frozen=True)
class PortSuffixMapping:
    """
    Each dict maps suffix to the full port name.
    """
    pub: dict[str, str] = dataclasses.field(default_factory=dict)
    sub: dict[str, str] = dataclasses.field(default_factory=dict)
    cln: dict[str, str] = dataclasses.field(default_factory=dict)
    srv: dict[str, str] = dataclasses.field(default_factory=dict)


# pytest --doctest-modules -sv service_discoverer.py
def discover_service_instances(service_instance_prefixes: dict[str, list[str]],
                             /,
                             pub: Iterable[str] = (),
                             sub: Iterable[str] = (),
                             cln: Iterable[str] = (),
                             srv: Iterable[str] = ()) -> dict[str, dict[str, PortSuffixMapping]]:
    """
    Discovers service instances provided by a remote node from the instance prefix mapping and the names of its ports
    (publishers, subscribers, clients, and servers).

    Here's a demo; suppose that the remote node has the following non-fixed ports:

    >>> pub = [
    ...     "foo.differential_pressure",
    ...     "foo.static_air_temperature",
    ...     "bar.differential_pressure",
    ...     "left_servo.feedback",
    ...     "left_servo.status",
    ...     "left_servo.power",
    ...     "left_servo.dynamics",
    ... ]
    >>> sub = [
    ...     "bar.heater_state",
    ...     "bar.bad.pattern.ignored",
    ...     "left_servo.setpoint",
    ...     "left_servo.readiness",
    ...     "unrelated.subscription",
    ... ]
    >>> srv = [
    ...     "unrelated.server",
    ...     "standalone_server",
    ... ]

    Also, the following service discovery registers:

    >>> service_instance_prefixes = {
    ...     "reg.udral.service.actuator.servo": ["left_servo"],
    ...     "reg.udral.service.pitot":          ["foo", "bar"],
    ... }

    Then we discover which services the node provides as follows:

    >>> result = discover_service_instances(service_instance_prefixes, pub=pub, sub=sub, srv=srv)
    >>> list(result)                                # Notice the ordering.
    ['reg.udral.service.actuator.servo', 'reg.udral.service.pitot']
    >>> result["reg.udral.service.pitot"]           # doctest: +NORMALIZE_WHITESPACE
    {'foo': PortSuffixMapping(pub={'differential_pressure':  'foo.differential_pressure',
                                   'static_air_temperature': 'foo.static_air_temperature'},
                              sub={},
                              cln={},
                              srv={}),
     'bar': PortSuffixMapping(pub={'differential_pressure': 'bar.differential_pressure'},
                              sub={'heater_state':          'bar.heater_state'},
                              cln={},
                              srv={})}
    >>> result["reg.udral.service.actuator.servo"]  # doctest: +NORMALIZE_WHITESPACE
    {'left_servo': PortSuffixMapping(pub={'feedback':   'left_servo.feedback',
                                          'status':     'left_servo.status',
                                          'power':      'left_servo.power',
                                          'dynamics':   'left_servo.dynamics'},
                                     sub={'setpoint':   'left_servo.setpoint',
                                          'readiness':  'left_servo.readiness'},
                                     cln={},
                                     srv={})}

    Ports that don't match any known service (vendor-specific and so on) are not reported.
    The ordering is guaranteed to follow that of the input arguments.
    """
    out: dict[str, dict[str, PortSuffixMapping]] = {}

    def psm(s: str, i: str) -> PortSuffixMapping:
        return out.setdefault(s, {}).setdefault(i, PortSuffixMapping())

    do_split = partial(_split, service_instance_prefixes)

    for svc, ins, suf, port in do_split(pub): psm(svc, ins).pub[suf] = port
    for svc, ins, suf, port in do_split(sub): psm(svc, ins).sub[suf] = port
    for svc, ins, suf, port in do_split(cln): psm(svc, ins).cln[suf] = port
    for svc, ins, suf, port in do_split(srv): psm(svc, ins).srv[suf] = port

    return out


def _split(service_instance_prefixes: dict[str, list[str]],
           port_names: Iterable[str]) -> Iterable[Tuple[str, str, str, str]]:
    """
    Result: service name, instance name, suffix, full port name.
    """
    for service_name, instance_names in service_instance_prefixes.items():
        for ins in instance_names:
            ins = ins.strip(".")
            for pn in port_names:
                if len(components := pn.split(".")) == 2:  # The pattern is "instance_name.suffix"
                    if ins == components[0]:
                        yield service_name, ins, components[1], pn
