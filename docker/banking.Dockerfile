# One image for both banking agent roles (role chosen by A2A_BANKING_ROLE at runtime).
# It bundles BOTH processes of a container:
#   - the Quarkus A2A gateway (quarkus-app)  — the spec message/send front
#   - the embedded Flink MiniCluster job (banking-job.jar) — the routed graph + LLM operators
# joined by Redis. The entrypoint starts the job, waits for it to subscribe, then the gateway.
#
# Build the jars first (they land in the gitignored target/ dirs), then build the image:
#   mvn -o clean install -DskipTests
#   mvn -o -f a2a-gateway/pom.xml package -DskipTests
#   mvn -o -f banking-job/pom.xml package -DskipTests
#   podman build -f docker/banking.Dockerfile -t agentic-flink-banking .
FROM eclipse-temurin:17-jre

# The Redis A2A bridge is non-lossy (RPUSH/BLPOP), so the entrypoint just starts both processes —
# no redis-cli readiness polling needed.
WORKDIR /deployments

# The Quarkus gateway (exploded quarkus-app layout) and the self-contained Flink job uber-jar.
COPY a2a-gateway/target/quarkus-app/ /deployments/quarkus-app/
COPY banking-job/target/banking-job.jar /deployments/banking-job.jar

# Public knowledge base (CS agent RAG) bundled into the image so startup is self-contained.
COPY banking-kb/ /app/kb/

COPY docker/banking-entrypoint.sh /deployments/run.sh
RUN chmod +x /deployments/run.sh

# 9001 (personal) / 9002 (cs) — the role's gateway binds QUARKUS_HTTP_PORT.
EXPOSE 9001 9002
ENTRYPOINT ["/deployments/run.sh"]
