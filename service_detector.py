# Pavel Kirienko <pavel@uavcan.org>

from __future__ import annotations
import dataclasses
from typing import Iterable, Sequence, Tuple


@dataclasses.dataclass(frozen=True)
class PortSuffixMapping:
    """
    If the full port name follows the pattern ``[service_name.[instance_name.]]suffix``,
    where the bracketed part is optional, this type provides the mapping from ``suffix`` to the full port name.
    """
    pub: dict[str, str] = dataclasses.field(default_factory=dict)
    sub: dict[str, str] = dataclasses.field(default_factory=dict)
    cln: dict[str, str] = dataclasses.field(default_factory=dict)
    srv: dict[str, str] = dataclasses.field(default_factory=dict)


# pytest --doctest-modules -sv service_detector.py
def detect_service_instances(pub: Iterable[str] = (),
                             sub: Iterable[str] = (),
                             cln: Iterable[str] = (),
                             srv: Iterable[str] = ()) -> dict[str, dict[str, PortSuffixMapping]]:
    """
    Detects service instances provided by a remote node from the names of its ports
    (publishers, subscribers, clients, and servers).

    Here's a demo; suppose that the remote node has the following non-fixed ports:

    >>> pub = [
    ...     "airspeed.foo.differential_pressure",   # Service "airspeed", instance "foo"
    ...     "airspeed.foo.static_air_temperature",  # Service "airspeed", instance "foo"
    ...     "airspeed.bar.differential_pressure",   # Service "airspeed", instance "bar"
    ...     "servo.feedback",                       # Service "servo", anonymous instance (singleton)
    ...     "servo.status",                         # etc.
    ...     "servo.power",
    ...     "servo.dynamics",
    ... ]
    >>> sub = [
    ...     "airspeed.bar.heater.state",            # Service "airspeed", instance "bar" (see above)
    ...     "servo.setpoint",                       # Service "servo", anonymous instance (see above)
    ...     "servo.readiness",
    ...     "unrelated.subscription",               # Application-specific or vendor-specific subject, non-standard
    ... ]
    >>> srv = [
    ...     "unrelated.server",                     # Application-specific or vendor-specific server, non-standard
    ...     "standalone_server",                    # Not part of a service, non-standard
    ... ]

    Then we detect which services the node provides as follows:

    >>> result = detect_service_instances(pub=pub, sub=sub, srv=srv)
    >>> list(result)                                # Notice the ordering.
    ['airspeed', 'servo', 'unrelated', '']
    >>> result["airspeed"]                          # doctest: +NORMALIZE_WHITESPACE
    {'foo': PortSuffixMapping(pub={'differential_pressure':  'airspeed.foo.differential_pressure',
                                   'static_air_temperature': 'airspeed.foo.static_air_temperature'},
                              sub={},
                              cln={},
                              srv={}),
     'bar': PortSuffixMapping(pub={'differential_pressure': 'airspeed.bar.differential_pressure'},
                              sub={'heater.state':          'airspeed.bar.heater.state'},
                              cln={},
                              srv={})}
    >>> result["servo"]                             # doctest: +NORMALIZE_WHITESPACE
    {'': PortSuffixMapping(pub={'feedback':   'servo.feedback',
                                'status':     'servo.status',
                                'power':      'servo.power',
                                'dynamics':   'servo.dynamics'},
                           sub={'setpoint':   'servo.setpoint',
                                'readiness':  'servo.readiness'},
                           cln={},
                           srv={})}
    >>> result["unrelated"]                         # doctest: +NORMALIZE_WHITESPACE
    {'': PortSuffixMapping(pub={},
                           sub={'subscription': 'unrelated.subscription'},
                           cln={},
                           srv={'server': 'unrelated.server'})}
    >>> result[""]                                  # doctest: +NORMALIZE_WHITESPACE
    {'': PortSuffixMapping(pub={},
                           sub={},
                           cln={},
                           srv={'standalone_server': 'standalone_server'})}

    As you can see, the name of a port is formed of three parts: "service_name.instance_name.suffix",
    where the instance name can be omitted (along with its separator ".") if there is only one instance of the service.
    If both are omitted, only the suffix remains, which can only be an application-specific port (non-standard).

    The ordering is guaranteed to follow that of the input arguments.
    """
    out: dict[str, dict[str, PortSuffixMapping]] = {}

    def psm(s: str, i: str) -> PortSuffixMapping:
        return out.setdefault(s, {}).setdefault(i, PortSuffixMapping())

    for svc, ins, suf, port in _split(pub): psm(svc, ins).pub[suf] = port
    for svc, ins, suf, port in _split(sub): psm(svc, ins).sub[suf] = port
    for svc, ins, suf, port in _split(cln): psm(svc, ins).cln[suf] = port
    for svc, ins, suf, port in _split(srv): psm(svc, ins).srv[suf] = port

    return out


def _split(port_names: Iterable[str]) -> Iterable[Tuple[str, str, str, str]]:
    for pn in port_names:
        p = pn.split(".", 2)
        if   len(p) > 2: yield p[0], p[1], p[2], pn
        elif len(p) > 1: yield p[0],   "", p[1], pn
        else:            yield   "",   "", p[0], pn
