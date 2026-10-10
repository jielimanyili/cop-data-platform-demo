# ---- build stage (context = repo root) ----
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /app
COPY pom.xml .
COPY java-pipeline/pom.xml java-pipeline/pom.xml
COPY java-pipeline/src java-pipeline/src
RUN mvn -q -f java-pipeline/pom.xml package -DskipTests

# ---- runtime stage ----
FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /app/java-pipeline/target/*.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
