FROM debezium/connect:2.2

# Install Confluent Hub Client
RUN curl -sSL --output /tmp/confluent-hub-client.tar.gz \
    https://packages.confluent.io/archive/tools/confluent-hub-client-latest.tar.gz && \
    tar -xzf /tmp/confluent-hub-client.tar.gz -C /usr/local && \
    ln -s /usr/local/confluent-hub-client/bin/confluent-hub /usr/local/bin/confluent-hub && \
    rm /tmp/confluent-hub-client.tar.gz

# Install Confluent S3 Sink Connector
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-s3:10.5.5
