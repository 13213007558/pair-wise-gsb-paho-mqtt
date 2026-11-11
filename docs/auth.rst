auth module
===========

MQTT version 5.0 enhanced authentication (specification section 4.12).

The library never implements an authentication algorithm or an identity
service. It carries the ``Authentication Method`` and ``Authentication Data``
properties, sends the correct AUTH packets and maintains the state machine.
The application supplies an :class:`paho.mqtt.auth.AuthController` subclass
implementing the challenge/response algorithm.

Lifecycle
---------

Two distinct lifecycles are exposed through :class:`paho.mqtt.auth.AuthPhase`:

* ``AuthPhase.INITIAL`` runs during connection establishment, between the
  CONNECT packet and the successful CONNACK. The connection is not reported as
  authenticated until the controller's ``handle_success`` accepts the CONNACK.
* ``AuthPhase.REAUTHENTICATE`` is started on an established connection through
  ``Client.re_authenticate()``. The existing connection keeps working until the
  exchange completes.

The Authentication Method is fixed for the whole exchange: a packet that
changes it, or carries Authentication Data without an Authentication Method,
is rejected as a protocol error. Controller exceptions, rejected challenges and
an early transport close all abort the exchange and never leave the client
reporting itself as authenticated.

Each controller callback documents the values it is allowed to return.

.. automodule:: paho.mqtt.auth
   :members:
   :undoc-members:
