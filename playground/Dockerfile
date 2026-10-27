FROM confluentinc/cp-server-connect:7.9.2

# x64 default; for arm64 build with:
#   --build-arg IC_ZIP=instantclient-basic-linux.arm64-19.32.0.0.0dbru.zip --build-arg IC_DIR=instantclient_19_32
ARG IC_ZIP=instantclient-basic-linux.x64-19.26.0.0.0dbru.zip
ARG IC_DIR=instantclient_19_26

USER root
RUN yum --nodocs -q update -y --disablerepo=temurin-jdk && yum --nodocs --disablerepo=temurin-jdk -q install -y wget unzip libaio libnsl2 

# for 7.9.2
RUN ln -s /lib64/libnsl.so.2 /lib64/libnsl.so.1


RUN   confluent-hub install --no-prompt confluentinc/kafka-connect-oracle-xstream-cdc-source:latest
COPY  ${IC_ZIP} /tmp/${IC_ZIP}

RUN   cd  /tmp && unzip  /tmp/${IC_ZIP} 
RUN   mv /tmp/${IC_DIR} /usr/share/

RUN   mkdir -p /usr/share/confluent-hub-components/confluentinc-kafka-connect-oracle-xstream-cdc-source/lib

RUN   cd /usr/share/${IC_DIR} \
  && cp  ojdbc8.jar xstreams.jar /usr/share/confluent-hub-components/confluentinc-kafka-connect-oracle-xstream-cdc-source/lib

ENV   LD_LIBRARY_PATH="/usr/share/${IC_DIR}"






