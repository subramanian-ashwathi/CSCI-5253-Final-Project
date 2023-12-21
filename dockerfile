FROM apache/airflow:2.7.3
# FROM python:3.6

USER root

# RUN pip install "apache-airflow[celery]==2.2.5" #--constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.8.0/constraints-3.8.txt"

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         openjdk-11-jre-headless wget\
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# RUN apt-get update \
#   && apt-get install -y software-properties-common \
#   && add-apt-repository -y ppa:deadsnakes/ppa \
#   && apt-get install python3.6

ARG SPARK_VERSION="3.1.2"

ARG HADOOP_VERSION="3.2"

ENV SPARK_HOME /usr/local/spark

# Spark submit binaries and jars (Spark binaries must be the same version of spark cluster)
RUN cd "/tmp" && \
        wget --no-verbose "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" && \
        tar -xvzf "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" && \
        mkdir -p "${SPARK_HOME}/bin" && \
        mkdir -p "${SPARK_HOME}/assembly/target/scala-2.12/jars" && \
        cp -a "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}/bin/." "${SPARK_HOME}/bin/" && \
        cp -a "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}/jars/." "${SPARK_HOME}/assembly/target/scala-2.12/jars/" && \
        rm "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz"

# Create SPARK_HOME env var
RUN export SPARK_HOME
ENV PATH $PATH:/usr/local/spark/bin

USER airflow

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

COPY requirements.txt /tmp/requirements.txt

RUN pip install -r /tmp/requirements.txt