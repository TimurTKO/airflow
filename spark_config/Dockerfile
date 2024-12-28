FROM bitnami/spark:latest

#RUN pip install --no-cache-dir notebook

# Указываем рабочую директорию
#WORKDIR /opt/airflow

# Добавляем в PATH
#ENV PATH="/usr/local/bin:${PATH}"

RUN pip install --upgrade pip
COPY requirements.txt /requirements.txt
RUN pip install -r /requirements.txt
RUN pip install findspark
