"""Deterministic fake enhanced authentication controller for tests.

This is *not* a real identity service and performs no cryptographic work. It
implements a fully deterministic challenge/response scheme so the MQTT v5.0
enhanced authentication state machine can be exercised repeatably:

* the Authentication Method is ``"FAKE-AUTH"``;
* every step echoes the received Authentication Data back prefixed with a step
  marker, so the exchange is easy to assert on the wire;
* a controller created with ``fail_at`` aborts at a chosen lifecycle point,
  demonstrating failure handling.
"""
from __future__ import annotations

from typing import Any

from paho.mqtt.auth import AuthController, AuthPhase, AuthResult
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.reasoncodes import ReasonCode

FAKE_METHOD = "FAKE-AUTH"
NOT_AUTHORIZED = 135


class FakeAuthController(AuthController):
    """Deterministic controller used by the enhanced authentication tests."""

    method = FAKE_METHOD

    def __init__(
        self,
        *,
        initial_data: bytes | None = b"client-initial",
        fail_at: str | None = None,
        raise_at: str | None = None,
        require_final_success_data: bool = False,
    ) -> None:
        # fail_at / raise_at select one of: "initial", "challenge",
        # "success", "reauth", "reauth-challenge".
        self.initial_data = initial_data
        self.fail_at = fail_at
        self.raise_at = raise_at
        self.require_final_success_data = require_final_success_data
        self.events: list[tuple[str, Any]] = []
        self.challenge_count = 0
        self.reauth_challenge_count = 0

    def _maybe_failure(self, point: str) -> ReasonCode | None:
        if self.fail_at == point:
            self.events.append(("failure", point))
            return ReasonCode(PacketTypes.AUTH, identifier=NOT_AUTHORIZED)
        if self.raise_at == point:
            self.events.append(("raise", point))
            raise RuntimeError(f"synthetic failure at {point}")
        return None

    def initial_authentication(self, client, userdata) -> AuthResult:
        self.events.append(("initial", self.initial_data))
        failure = self._maybe_failure("initial")
        if failure is not None:
            return AuthResult(reason_code=failure)
        return AuthResult(data=self.initial_data)

    def handle_challenge(self, client, userdata, phase, authentication_data, properties) -> AuthResult:
        point = "reauth-challenge" if phase == AuthPhase.REAUTHENTICATE else "challenge"
        if phase == AuthPhase.REAUTHENTICATE:
            self.reauth_challenge_count += 1
            step = self.reauth_challenge_count
        else:
            self.challenge_count += 1
            step = self.challenge_count
        self.events.append((point, authentication_data))
        failure = self._maybe_failure(point)
        if failure is not None:
            return AuthResult(reason_code=failure)
        response = f"response-{step}".encode()
        if authentication_data is not None:
            response = authentication_data + b"|" + response
        return AuthResult(data=response)

    def handle_success(self, client, userdata, phase, authentication_data, properties):
        point = "reauth-success" if phase == AuthPhase.REAUTHENTICATE else "success"
        self.events.append((point, authentication_data))
        failure = self._maybe_failure(point)
        if failure is not None:
            return AuthResult(reason_code=failure)
        if phase == AuthPhase.REAUTHENTICATE and self.require_final_success_data:
            return AuthResult(data=b"client-final-success")
        return None

    def re_authentication(self, client, userdata) -> AuthResult:
        self.events.append(("reauth", None))
        failure = self._maybe_failure("reauth")
        if failure is not None:
            return AuthResult(reason_code=failure)
        return AuthResult(data=b"reauth-initial")
