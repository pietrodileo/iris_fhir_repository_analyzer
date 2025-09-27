# ------------------------------------------------------------------------------
# Start from the official InterSystems IRIS Community Edition image
# ------------------------------------------------------------------------------
FROM intersystems/iris-community:latest-cd

# ------------------------------------------------------------------------------
# Define environment variables (reusable everywhere)
# ------------------------------------------------------------------------------
ENV APP_HOME=/opt/irisapp \
    APP_LOGS=/opt/irisapp/logs

# ------------------------------------------------------------------------------
# Work as root temporarily to create directories and adjust permissions
# ------------------------------------------------------------------------------
USER root

# ------------------------------------------------------------------------------
# Set a working directory inside the container for your app code
# ------------------------------------------------------------------------------
WORKDIR $APP_HOME
RUN chown ${ISC_PACKAGE_MGRUSER}:${ISC_PACKAGE_IRISGROUP} $APP_HOME

# ------------------------------------------------------------------------------
# Copy your application files in the container
# ------------------------------------------------------------------------------

# Script that will be executed by entrypoint at runtime to run the installer 
COPY iris.script .

# Startup script wrapper (will run at *container start*)
COPY entrypoint.sh $APP_HOME/entrypoint.sh
RUN chmod +x $APP_HOME/entrypoint.sh \
    && chown ${ISC_PACKAGE_MGRUSER}:${ISC_PACKAGE_IRISGROUP} $APP_HOME/entrypoint.sh

# ------------------------------------------------------------------------------
# Create logs folder and fix ownership
# ------------------------------------------------------------------------------
RUN mkdir -p $APP_LOGS \
    && chown -R ${ISC_PACKAGE_MGRUSER}:${ISC_PACKAGE_IRISGROUP} $APP_LOGS

# ------------------------------------------------------------------------------
# Switch back to the IRIS user to run entrypoint (never run IRIS as root!)
# ------------------------------------------------------------------------------
USER ${ISC_PACKAGE_MGRUSER}

# ------------------------------------------------------------------------------
# Entrypoint: this runs your entrypoint.sh, which will start IRIS,
# import your code into the current USER namespace, and then hand over
# control to the default IRIS entrypoint.
# Logs can be found in /opt/irisapp/logs/entrypoint.log and in Docker Desktop either.
# Logs can be print in terminal by using command: docker exec -it <container_name> cat /opt/irisapp/logs/entrypoint.log
# ------------------------------------------------------------------------------
ENTRYPOINT ["/opt/irisapp/entrypoint.sh"] 
# can't use the $APP_HOME here

# ------------------------------------------------------------------------------
# Ports
#   1972  -> IRIS SuperServer (ODBC, JDBC, etc.)
#   52773 -> Management Portal / Web Apps
# ------------------------------------------------------------------------------
EXPOSE 1972 52773  