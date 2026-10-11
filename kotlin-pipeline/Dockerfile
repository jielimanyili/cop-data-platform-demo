# ---- build stage (context = repo root) ----
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /app
COPY pom.xml .
COPY kotlin-pipeline/pom.xml kotlin-pipeline/pom.xml
COPY kotlin-pipeline/src kotlin-pipeline/src
RUN mvn -q -f kotlin-pipeline/pom.xml package -DskipTests

# ---- runtime stage ----
FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /app/kotlin-pipeline/target/kotlin-pipeline-*.jar app.jar
ENTRYPOINT ["java", "-cp", "app.jar", "com.jieli.cop.risk.RiskScoringAppKt"]