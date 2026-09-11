# Reflective-access opens that Flink (Kryo / Twitter Chill) needs on Java 17+.
# Mirrors the surefire <argLine> in the root pom, and .mvn/jvm.config for the
# `mvn exec:java` launchers. Source this before any bare `java -jar`.
AGENTIC_ADD_OPENS="\
--add-opens=java.base/java.util=ALL-UNNAMED \
--add-opens=java.base/java.lang=ALL-UNNAMED \
--add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
--add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
--add-opens=java.base/java.io=ALL-UNNAMED \
--add-opens=java.base/java.net=ALL-UNNAMED \
--add-opens=java.base/java.nio=ALL-UNNAMED \
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
--add-opens=java.base/sun.nio.cs=ALL-UNNAMED \
--add-opens=java.base/sun.security.action=ALL-UNNAMED \
--add-opens=java.base/sun.util.calendar=ALL-UNNAMED \
--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED \
--add-opens=java.base/java.text=ALL-UNNAMED"
export AGENTIC_ADD_OPENS
