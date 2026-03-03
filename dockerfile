FROM apache/spark:3.5.0-python3

# Spark user is 185 by default
USER root

# Set Spark home and path
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

# Create a writable home directory for the spark user and Ivy cache
RUN mkdir -p /app/.ivy2 && chmod -R 777 /app/.ivy2
ENV HOME=/app

# Install python dependencies
RUN pip install kafka-python-ng

# Link python3 to python (just in case)
RUN ln -sf /usr/bin/python3 /usr/bin/python

# Set working directory
WORKDIR /app

# Copy project files
COPY . /app
# Ensure permissions are correct for the spark user
RUN chmod -R 777 /app

# Back to spark user
USER 185