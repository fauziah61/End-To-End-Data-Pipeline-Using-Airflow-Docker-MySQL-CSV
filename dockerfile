FROM apache/airflow:3.1.8

ARG MYSQL_CONNECTOR_J_VERSION=9.6.0

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /opt/spark-jars \
    && curl --fail --location --retry 3 --silent --show-error \
        "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/${MYSQL_CONNECTOR_J_VERSION}/mysql-connector-j-${MYSQL_CONNECTOR_J_VERSION}.jar" \
        --output /opt/spark-jars/mysql-connector-j.jar \
    && chown -R airflow:root /opt/spark-jars

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    SPARK_JDBC_JAR=/opt/spark-jars/mysql-connector-j.jar

COPY --chown=airflow:root requirements.txt /requirements.txt

USER airflow
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt
