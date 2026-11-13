"""VizAI's side of the connector flow.

A connector agent runs in the client's network, dials out to the gateway
(vizai_connector_gateway) and pushes the database schema. This package is what VizAI does with
that: check the connector's token, save the pushed schema in the same shape the direct flow
produces, track online/offline, and offer the onboarding API.

The gateway is transport only. It never touches VizAI's database; everything it needs goes
through GatewayHooks (hooks_impl.py). VizAI never holds the client's database login.

Everything here is inactive unless CONNECTOR_GATEWAY_ENABLED is true.
"""
