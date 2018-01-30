# docker build -t "nifi-example" ./
# docker run -p 8080:8080 nifi-example

from apache/nifi:latest

ADD --chown=nifi:nifi ./sample-bundle-nar/target/*.nar lib/
ADD --chown=nifi:nifi ./sample-controller-service-api-nar/target/*.nar lib/


