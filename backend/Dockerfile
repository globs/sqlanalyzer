FROM ubuntu:18.04

 

LABEL author="Vincent GAUTIER" \
      name="SQL Analyzer Docker image build" \
      version="v1.0-BETA"

 

#ADD user
RUN useradd -ms /bin/bash sql_analyzer

 

#Install image dependencies

RUN apt-get -y -qq update \
    && apt-get -y -qq install make gcc g++ \
    && apt-get install -y python3.8 python3-pip python3.8-dev\
    build-essential unixodbc-dev libpq-dev curl\
    vim nano


# Install UnixODBC

COPY ["./config/pip/requirements.txt", \
      "/opt/"]

 

# Install python requirements

COPY ./config/pip/pip.conf /etc/pip.conf
RUN python3.8 -m pip install --upgrade pip \
    && python3.8 -m pip install -r /opt/requirements.txt \
    && rm /opt/requirements.txt \
    && rm /etc/pip.conf


RUN mkdir /home/sql_analyzer/project/
COPY src/ /home/sql_analyzer/project/src/
COPY samples/ /home/sql_analyzer/project/samples/
WORKDIR /home/sql_analyzer/project/src/facade/

#ENV PYTHONPATH="/home/sql_analyzer/project/src/:${PYTHONPATH}"
RUN mkdir /tmp/result_files &&  chown -R sql_analyzer /tmp/result_files
RUN  chown -R sql_analyzer /home/sql_analyzer/project/src/logs/
RUN  chown -R sql_analyzer /home/sql_analyzer/project/samples/
USER sql_analyzer
#CMD ["python3.8", "./facade/main.py"]