    # ------------------------------------------------------------------
# Imagen Base de Hadoop (Estilo Producción)
# ------------------------------------------------------------------

# Paso 1: Usar una base segura y oficial de Java 8 (Hadoop 3.2.x la prefiere)
FROM eclipse-temurin:8-jre-focal

# Argumentos para versiones (fácil de actualizar)
ARG HADOOP_VERSION=3.2.1
ARG HADOOP_URL=https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz

# Variables de entorno
ENV HADOOP_VERSION=${HADOOP_VERSION}
ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
ENV PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH
ENV HDFS_NAMENODE_USER="root"
ENV HDFS_DATANODE_USER="root"
ENV HDFS_SECONDARYNAMENODE_USER="root"
ENV YARN_RESOURCEMANAGER_USER="root"
ENV YARN_NODEMANAGER_USER="root"

# Paso 2: Instalar dependencias
# - curl: para descargar
# - net-tools, iputils-ping: para diagnósticos de red
# - openssh-server: Hadoop usa SSH para scripts de clúster (aunque no los usaremos, es buena práctica)
RUN apt-get update && \
    apt-get install -y curl net-tools iputils-ping openssh-server && \
    rm -rf /var/lib/apt/lists/*

# Paso 3: Descargar y descomprimir Hadoop
RUN curl -fSL ${HADOOP_URL} -o /tmp/hadoop.tar.gz && \
    tar -xzf /tmp/hadoop.tar.gz -C /opt/ && \
    # Renombramos para que sea genérico (de /opt/hadoop-3.2.1 a /opt/hadoop)
    mv /opt/hadoop-${HADOOP_VERSION} ${HADOOP_HOME} && \
    rm /tmp/hadoop.tar.gz

# Paso 4: Crear directorios para datos (serán volúmenes)
# Le damos permisos al usuario 'root' (o al usuario que corra el proceso)
RUN mkdir -p /data/namenode && \
    mkdir -p /data/datanode && \
    chown -R root:root /data

# Paso 5: Configurar SSH (necesario para los daemons de Hadoop)
# Permitir login de root (solo para este contenedor, no es un riesgo externo)
RUN sed -i 's/PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config && \
    sed -i 's/#PermitRootLogin yes/PermitRootLogin yes/' /etc/ssh/sshd_config && \
    # Generar claves SSH para que el namenode pueda contactar datanodes (si usamos scripts)
    ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa && \
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && \
    chmod 0600 ~/.ssh/authorized_keys

# Definir el directorio de trabajo
WORKDIR ${HADOOP_HOME}

# ------------------------------------------------------------------
# Nota: No exponemos puertos ni definimos un ENTRYPOINT aquí.
# Esto hace que la imagen sea una "base" flexible.
# El comando (namenode, datanode) se definirá en docker-compose.
# ------------------------------------------------------------------