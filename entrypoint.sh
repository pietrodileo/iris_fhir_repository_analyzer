#!/bin/bash
set -e

# Start IRIS in the background
iris start IRIS quietly

# Run your import script every container start. 
#  - uncomment the last line to redirect log output to a log file (logs won't be shown on Docker Desktop console).
iris session IRIS < /opt/irisapp/iris.script >> /opt/irisapp/logs/entrypoint.log 2>&1

# Stop background if needed (optional, for clean start)
iris stop IRIS quietly

# Now run the default iris entrypoint
exec /iris-main "$@"
# Note: The above script assumes that the IRIS instance is named "IRIS"
# and that the import script is located at /opt/irisapp/iris.script.