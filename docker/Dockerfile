FROM --platform=linux/arm64 flink:1.17.0-java11

# Install Python 3.9 and system dependencies
RUN apt-get update -y && apt-get install -y python3 python3-pip python3-dev build-essential default-jdk && ln -s /usr/bin/python3 /usr/bin/python

# Upgrade pip
RUN pip3 install --upgrade pip

# Install dependencies
RUN JAVA_HOME=/usr/lib/jvm/default-java pip3 install kafka-python-ng numpy pulp pandas apache-flink==1.17.0

# Download Kafka Connector JAR
RUN wget -P /opt/flink/lib https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.17.0/flink-sql-connector-kafka-1.17.0.jar
