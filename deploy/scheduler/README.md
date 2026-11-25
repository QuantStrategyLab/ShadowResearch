# Deprecated Mac mini / Launchd / Systemd Templates (Superseded by Item 18)

The host templates in this directory (`shadow-drill.*`, `shadow-missed-run.*`, `shadow-watchdog.sh`, and `.plist` files) were authored for the retired Mac mini (`SH-1`) topology and are **deprecated / superseded** per Roadmap Item 18.

Do not deploy these units to a workstation or Mac mini host. For `SH-2` runtime execution, use the secret-free GCP Cloud Run Jobs (`us-east1`) container and template under [`deploy/cloudrun/`](../cloudrun/) (`Dockerfile` + `shadow.cloudrun_job`). These archived templates remain solely so existing SH-1 unit tests (`tests/test_scheduler.py`) stay passing.
