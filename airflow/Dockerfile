FROM apache/airflow:latest

## Install Java 17 and procps
#RUN apt-get update && \
#    apt-get install -y openjdk-17-jre-headless procps && \
#    apt-get clean;
#
## Set JAVA_HOME environment variable
#ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64

COPY requirements.txt /requirements.txt
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r /requirements.txt