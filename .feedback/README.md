# Feedback task starting environment

Upstream: https://github.com/eclipse-paho/paho.mqtt.python
Upstream commit: `e3eab734a576e68c224f71b06e96368b17c205ff`
Prepared on macOS with CPython 3.12.11. No external database, queue, or cloud account is required.
The dependency file pins the installed runtime and test packages for the selected baseline. It is not a claim that all optional upstream test suites are installed.

From the repository root:

```sh
export NO_INTERNET=1 TZ=UTC UV_NO_CONFIG=1

uv venv --python 3.12 .venv
uv pip install --python .venv/bin/python -r .feedback/requirements.txt -e .
.venv/bin/python -m pytest -q -o addopts= tests/test_packet.py tests/test_mqttv5.py tests/test_matcher.py tests/test_properties_variable_byte_integer.py
```

Scope note: The scope is the command below, not all optional upstream integrations.

Observed baseline (2026-11-11): `43 passed, 24 skipped in 0.09s`
Whole-command wall time: 0.410 seconds, exit code 0. This is one local measurement, not a runtime guarantee. The first install is excluded.

This seed adds preparation metadata only. Application sources and upstream tests are unchanged; there is no task solution in this commit.
