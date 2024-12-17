FROM bitnami/spark:latest


RUN pip install --upgrade pip
COPY requirements.txt /requirements.txt
RUN pip install -r /requirements.txt
RUN pip install findspark