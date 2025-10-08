# Build stage
FROM gradle:7.6-jdk11 AS builder

# Copy source code
WORKDIR /app
COPY . .

# Build the application
RUN gradle clean build -x test

# Runtime stage
FROM adoptopenjdk:11-jre-hotspot

# DlSync app
RUN mkdir /opt/app
WORKDIR /opt/app

# Copy the built jar from the builder stage
COPY --from=builder /app/build/libs/dlsync-*.jar dlsync.jar

# Set the entrypoint
ENTRYPOINT ["java", "-jar", "dlsync.jar"]
