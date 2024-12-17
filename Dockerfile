FROM apache/airflow:2.10.3
USER root

# # Install Java
# RUN apt-get update && apt-get install -y openjdk-17-jdk

# # Install procps to get 'ps' command
# RUN apt-get install -y procps

# # Install wget (Add this line)
# RUN apt-get install -y wget

# # Install Spark
# RUN wget https://archive.apache.org/dist/spark/spark-3.4.1/spark-3.4.1-bin-hadoop3.tgz && \
#     tar -xzf spark-3.4.1-bin-hadoop3.tgz -C /opt/ && \
#     ln -s /opt/spark-3.4.1-bin-hadoop3 /opt/spark

    
# # Set SPARK_HOME
# ENV SPARK_HOME=/opt/spark
# ENV PATH=$PATH:$SPARK_HOME/bin


USER root

# Install OpenJDK-17
RUN apt update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get install -y ant && \
    apt-get clean;

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64/
RUN export JAVA_HOME



USER airflow

COPY requirements.txt .
RUN pip install --upgrade pip
RUN pip install apache-airflow==${AIRFLOW_VERSION} --no-cache-dir -r requirements.txt

