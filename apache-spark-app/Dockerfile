# ------------------------------------------------------------------
# Imagen Custom de PySpark (Ubuntu 22.04 + Java 11 + Spark 3.2.1)
# ------------------------------------------------------------------

# 1. Usamos una base de Ubuntu 22.04 (Jammy)
FROM ubuntu:22.04

# Argumentos para las versiones
ARG SPARK_VERSION=3.2.1
ARG HADOOP_PROFILE=3.2
ARG SPARK_TGZ="spark-${SPARK_VERSION}-bin-hadoop${HADOOP_PROFILE}.tgz"
ARG SPARK_URL="https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_TGZ}"

# Variables de entorno para que todo funcione
ENV DEBIAN_FRONTEND=noninteractive
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin
# ¡Importante! Definimos dónde Spark encontrará la config de HDFS
ENV HADOOP_CONF_DIR=/opt/hadoop-config

# 2. Instalamos dependencias:
# - python3, python3-pip: para PySpark
# - openjdk-11-jdk: Disponible en los repositorios de Ubuntu 22.04
# - curl: para descargar Spark
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    curl \
    python3 \
    python3-pip \
    openjdk-11-jdk && \
    rm -rf /var/lib/apt/lists/*

# 4. Instalamos Spark
# Descargamos los binarios pre-compilados para Hadoop 3.2
RUN curl -fSL ${SPARK_URL} -o /tmp/spark.tgz && \
    tar -xzf /tmp/spark.tgz -C /opt/ && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_PROFILE} ${SPARK_HOME} && \
    rm /tmp/spark.tgz

# 5. Instalamos la librería PySpark
# La versión debe coincidir con los binarios de Spark
RUN pip3 install pyspark==${SPARK_VERSION}

# 6. Preparamos el directorio de la app y el de la config
WORKDIR /app
COPY stream-writer.py .
RUN mkdir -p ${HADOOP_CONF_DIR}

# El CMD será el mismo, pero lo definimos en docker-compose
# para mayor flexibilidad.