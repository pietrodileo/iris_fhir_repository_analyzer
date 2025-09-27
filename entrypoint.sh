#!/bin/sh
set -e

# Start IRIS in the background
iris start IRIS quietly

# Run your import script every container start
iris session IRIS < /opt/irisapp/iris.script >> /opt/irisapp/logs/entrypoint.log 2>&1

# Stop background if needed (optional, for clean start)
iris stop IRIS quietly

# Hand over control to the default IRIS entrypoint
exec /iris-main "$@"
