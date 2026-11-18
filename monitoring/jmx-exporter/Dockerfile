FROM eclipse-temurin:17-jre
ARG JMX_EXPORTER_VERSION=1.0.1
ADD https://repo.maven.apache.org/maven2/io/prometheus/jmx/jmx_prometheus_httpserver/${JMX_EXPORTER_VERSION}/jmx_prometheus_httpserver-${JMX_EXPORTER_VERSION}.jar /opt/jmx-prometheus-httpserver.jar
ENTRYPOINT ["java", "-jar", "/opt/jmx-prometheus-httpserver.jar"]
CMD ["5556", "/etc/jmx-exporter/config.yml"]
