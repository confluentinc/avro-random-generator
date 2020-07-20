FROM adoptopenjdk/openjdk8

RUN apt-get update && \
  apt-get install -y python3.6 python3-pip jq gettext-base

RUN python3 -m pip install deepmerge

COPY . /opt/random-avro-generator

RUN cd /opt/random-avro-generator/ && \
  ./gradlew build && \
  ./gradlew standalone

COPY ./delivery/dist/avro-generator /usr/bin
RUN chmod +x /usr/bin/avro-generator

COPY ./delivery/docker/docker-entrypoint.sh /
ENTRYPOINT ["/docker-entrypoint.sh"]
WORKDIR /opt/random-avro-generator/delivery
CMD ["/bin/bash"]
