FROM apache/airflow:2.10.4

RUN pip install apache-airflow-providers-apache-spark
RUN pip install minio

USER root
RUN apt-get update
RUN apt-get install -y --no-install-recommends openjdk-17-jdk
RUN apt-get install -y --no-install-recommends wget
RUN apt-get clean && rm -rf /var/lib/apt/lists/*

ENV SPARK_HOME="/opt/spark"
ENV JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"
ENV PATH="${JAVA_HOME}:${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"

# Port master will be exposed
ENV SPARK_MASTER_PORT="7077"
# Name of master container and also counts as hostname
ENV SPARK_MASTER_HOST="spark-master"

RUN mkdir -p ${SPARK_HOME}
# If it breaks in this step go to https://dlcdn.apache.org/spark/ and choose higher spark version instead
RUN curl https://dlcdn.apache.org/spark/spark-3.5.6/spark-3.5.6-bin-hadoop3.tgz -o spark-3.5.6-bin-hadoop3.tgz \
    && tar xvzf spark-3.5.6-bin-hadoop3.tgz --directory ${SPARK_HOME} --strip-components 1 \
    && rm -rf spark-3.5.6-bin-hadoop3.tgz

# Jars for aws s3 support
RUN wget -P ${SPARK_HOME}/jars/ https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar;
RUN wget -P ${SPARK_HOME}/jars/ https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar;

COPY spark-defaults.conf "${SPARK_HOME}/conf"

RUN chown -R airflow ${SPARK_HOME}

RUN apt update
RUN apt-get -y install procps

USER airflow
