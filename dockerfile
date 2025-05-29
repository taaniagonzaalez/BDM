FROM apache/airflow:2.7.2

# Install Java 11 and other required tools
USER root
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk wget curl unzip && \
    apt-get clean

# Set JAVA environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

# Install Python dependencies
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

