FROM debezium/connect:1.8

# Install required tools
USER root
RUN apt-get update && apt-get install -y unzip curl tar && rm -rf /var/lib/apt/lists/*

# Install Confluent Hub Client
RUN curl -sSL https://packages.confluent.io/confluent-hub-client/confluent-hub-client-latest.tar.gz | tar -xz -C /usr/local && ln -s /usr/local/confluent-hub/bin/confluent-hub /usr/local/bin/confluent-hub

# Install S3 Sink Connector into /kafka/connect
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-s3:10.5.3 --component-dir /kafka/connect

USER kafka
