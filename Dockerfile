FROM apache/airflow:3.0.0

# Zainstaluj Java + Spark + inne zależności
USER root

RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get clean

# Ustaw JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

# Zainstaluj pakiety pip
COPY requirements.txt /requirements.txt

USER airflow

RUN pip install --no-cache-dir -r /requirements.txt