FROM maven:3.9.11-eclipse-temurin-21 AS build
WORKDIR /build
COPY pom.xml .
RUN mvn -B -q dependency:go-offline
COPY src ./src
RUN mvn -B -q package -DskipTests

FROM eclipse-temurin:21-jre-alpine
RUN addgroup -S jobs && adduser -S jobs -G jobs
WORKDIR /app
COPY --from=build --chown=jobs:jobs /build/target/distributed-job-platform-0.1.0.jar app.jar
USER jobs
EXPOSE 8080
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75.0", "-jar", "app.jar"]
