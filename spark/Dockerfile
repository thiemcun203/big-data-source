# Use the Bitnami Spark base image
FROM docker.io/bitnami/spark:3.5.3-debian-12-r1

# Switch to root to set permissions and install dependencies
USER root

# Install redis library for Python (dependency installation layer)
RUN mkdir -p /var/lib/apt/lists/partial && \
    apt-get update && \
    apt-get install -y python3-pip && \
    pip3 install redis && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Set the SPARK_HOME environment variable
ENV SPARK_HOME=/opt/bitnami/spark

# Ensure proper permissions for all directories upfront
RUN chmod -R 755 $SPARK_HOME

# Switch back to the default user
USER 1001

# Set the working directory to SPARK_HOME
WORKDIR $SPARK_HOME

# Copy custom job files last (changes to these will only rebuild this layer)
COPY jobs/ $SPARK_HOME/examples/src/main/python

# Retain the default entrypoint
ENTRYPOINT ["/opt/bitnami/scripts/spark/entrypoint.sh"]

# Use a working command to start the Spark master
CMD ["/opt/bitnami/spark/bin/spark-class", "org.apache.spark.deploy.master.Master"]