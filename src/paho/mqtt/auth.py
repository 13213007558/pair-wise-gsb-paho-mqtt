# Copyright (c) 2024 Roger Light and others
#
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Eclipse Public License v2.0
# and Eclipse Distribution License v1.0 which accompany this distribution.
#
# The Eclipse Public License is available at
#    http://www.eclipse.org/legal/epl-v20.html
# and the Eclipse Distribution License is available at
#   http://www.eclipse.org/org/documents/edl-v10.php.
#
# Contributors:
#    MQTT v5.0 enhanced authentication support
"""
MQTT version 5.0 enhanced authentication (section 4.12 of the specification).

The client library never implements an authentication algorithm itself.  It is
only responsible for:

* carrying the ``Authentication Method`` and ``Authentication Data``
  properties between the server and the application,
* sending the correct AUTH packets with the correct reason codes, and
* maintaining the authentication state machine, rejecting packets that do not
  fit the current phase of the exchange.

The application supplies an `AuthController` subclass that implements the
challenge/response algorithm.  A controller instance is attached to a
`paho.mqtt.client.Client` through ``client.auth_controller = ...`` (or
`paho.mqtt.client.Client.authenticate`).

Two distinct lifecycles are exposed through `AuthPhase`:

* `AuthPhase.INITIAL` covers the exchange that happens while a connection is
  being established, between CONNECT and the successful CONNACK.  The
  connection is not reported as connected/authenticated until this completes.
* `AuthPhase.REAUTHENTICATE` covers an exchange started after the connection is
  already established, through `paho.mqtt.client.Client.re_authenticate`.
  The existing connection keeps working until the exchange finishes; it is only
  considered re-authenticated once the terminal success event has run.
"""
from __future__ import annotations

import enum
from typing import Any, NamedTuple

from .packettypes import PacketTypes
from .reasoncodes import ReasonCode


class AuthPhase(enum.Enum):
    """Lifecycle phase of an enhanced authentication exchange."""

    INITIAL = "initial"
    """The exchange is part of connection establishment, before CONNACK."""

    REAUTHENTICATE = "re-authenticate"
    """The exchange was started on an already established connection."""


class AuthResult(NamedTuple):
    """Return value of the `AuthController` challenge callbacks.

    Every field is optional.  Construct it with keyword arguments, for example
    ``AuthResult(data=b"response")`` or
    ``AuthResult(reason_code=ReasonCode(PacketTypes.AUTH, "Not authorized"))``.

    :param bytes|bytearray|None data: bytes to put in the outgoing
        ``Authentication Data`` property.  ``None`` (or omitted) means the
        outgoing packet carries no Authentication Data.
    :param ReasonCode|None reason_code: the AUTH reason code to send.  ``None``
        (or omitted) selects the correct default for the current phase:
        ``Continue authentication`` (24) while an exchange is in progress and
        ``Re-authenticate`` (25) when starting re-authentication.  Only reason
        codes defined for AUTH packets are accepted; a failure code (value
        >= 0x80, e.g. ``Not authorized`` 135) aborts the exchange.
    """

    data: bytes | bytearray | None = None
    reason_code: ReasonCode | None = None


# MQTT v5.0 AUTH reason codes (section 3.15.2).
AUTH_REASON_CONTINUE = 0x18
AUTH_REASON_REAUTHENTICATE = 0x19
AUTH_REASON_SUCCESS = 0x00


class AuthError(Exception):
    """Raised when enhanced authentication cannot be started.

    This is a programming/usage error (for example requesting
    re-authentication while no enhanced authentication is configured), not a
    protocol error received from the server.
    """


class AuthController:
    """Application provided enhanced authentication algorithm.

    Subclass this and override the callbacks relevant to the authentication
    method being implemented.  The library owns the AUTH packet exchange and
    the state machine; the subclass only decides what data to send and whether
    the exchange succeeded.

    All callbacks receive the ``userdata`` configured on the client and the
    `AuthPhase` of the current exchange.  Callbacks may raise any exception;
    such an exception aborts the exchange and, for initial authentication,
    prevents the connection from being reported as authenticated.

    The return value of each callback is documented on the method itself.
    """

    #: The Authentication Method name sent in every packet of the exchange
    #: (section 4.12.1).  Must be overridden with a non-empty string.  The
    #: library sends it unchanged and verifies that the server never replaces
    #: it mid-exchange.
    method: str = ""

    def initial_authentication(
        self,
        client: Any,
        userdata: Any,
    ) -> AuthResult:
        """Build the authentication material for the CONNECT packet.

        Called once when a connection is established with enhanced
        authentication enabled.  The returned `AuthResult` populates the
        ``Authentication Method`` (always, taken from `method`) and optional
        ``Authentication Data`` of the CONNECT packet.

        Allowed return values:

        * ``AuthResult()`` or ``None``: send the method with no Authentication
          Data, letting the server issue the first challenge.
        * ``AuthResult(data=b"...")``: include Authentication Data in CONNECT.
        * ``AuthResult(reason_code=<AUTH failure code>)``: refuse to start; the
          connection attempt is aborted locally and no CONNECT is sent.
        * Raising an exception: same as returning a failure; the connection is
          not established.

        The default implementation sends no Authentication Data.
        """
        return AuthResult()

    def handle_challenge(
        self,
        client: Any,
        userdata: Any,
        phase: AuthPhase,
        authentication_data: bytes | None,
        properties: Any,
    ) -> AuthResult:
        """Process a server challenge (AUTH with ``Continue authentication``).

        :param AuthPhase phase: whether the challenge belongs to the initial
            connection or a re-authentication.
        :param bytes|None authentication_data: the server's
            ``Authentication Data`` property, or ``None`` if it was absent.
        :param properties: the full `paho.mqtt.properties.Properties` of the
            received packet, giving access to ``User Property`` and similar.

        Allowed return values:

        * ``AuthResult(data=b"...")``: answer with another
          ``Continue authentication`` AUTH carrying this Authentication Data.
        * ``AuthResult()``: continue without Authentication Data.
        * ``AuthResult(reason_code=<AUTH failure code>)`` or raising an
          exception: reject the challenge.  During initial authentication the
          connection is closed; during re-authentication the existing
          connection is closed and the re-authentication is reported as failed.
        """
        raise NotImplementedError(
            "AuthController.handle_challenge must be implemented"
        )

    def handle_success(
        self,
        client: Any,
        userdata: Any,
        phase: AuthPhase,
        authentication_data: bytes | None,
        properties: Any,
    ) -> AuthResult | None:
        """Observe the terminal success packet of an exchange.

        For initial authentication this is the successful CONNACK (reason code
        0); for re-authentication it is the AUTH packet with reason code
        ``Success``.

        Allowed return values:

        * ``None`` or ``AuthResult()``: accept the successful exchange.  During
          initial authentication nothing further is sent (the CONNECT/CONNACK
          exchange is complete).  During re-authentication an AUTH with
          ``Success`` is sent if the method requires a final client packet.
        * ``AuthResult(data=b"...")``: attach Authentication Data to that final
          success AUTH (re-authentication only; ignored for CONNACK).
        * ``AuthResult(reason_code=<AUTH failure code>)`` or raising an
          exception: reject the outcome.  The connection is closed and is never
          reported as authenticated/re-authenticated.
        """
        return None

    def handle_failure(
        self,
        client: Any,
        userdata: Any,
        phase: AuthPhase,
        reason_code: ReasonCode,
        properties: Any,
    ) -> None:
        """Observe a server reported authentication failure.

        Called when the server ends the exchange with a failure reason code
        (AUTH value >= 0x80, or a non-zero CONNACK during initial
        authentication).  The library closes the connection afterwards; this
        callback exists for application bookkeeping and logging.

        Allowed return values: the return value is ignored.  Raising an
        exception is logged but does not change the outcome: the connection is
        still closed and never reported as authenticated.
        """

    def re_authentication(
        self,
        client: Any,
        userdata: Any,
    ) -> AuthResult:
        """Build the material that starts a re-authentication.

        Called once when `paho.mqtt.client.Client.re_authenticate` is invoked
        on an established connection.  The returned data is sent in the first
        client AUTH packet, which uses the ``Re-authenticate`` reason code.

        Allowed return values:

        * ``AuthResult()``: start re-authentication without Authentication
          Data.
        * ``AuthResult(data=b"...")``: include Authentication Data.
        * ``AuthResult(reason_code=<AUTH failure code>)`` or raising an
          exception: refuse to start; the established connection is left
          untouched and the re-authentication is reported as failed locally.
        """
        return AuthResult()


def is_auth_failure(reason_code: ReasonCode) -> bool:
    """Return whether an AUTH reason code denotes failure (section 2.4)."""
    return reason_code.value >= 0x80


def make_auth_reason_code(identifier: int) -> ReasonCode:
    """Create a `ReasonCode` valid for an AUTH packet.

    Raises ``ValueError`` for codes that are not defined for AUTH.
    """
    code = ReasonCode(PacketTypes.AUTH, identifier=identifier)
    return code
