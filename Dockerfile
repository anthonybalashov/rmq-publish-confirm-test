ARG VERSION=8u151

FROM openjdk:${VERSION}-jdk as BUILD

COPY . /src
WORKDIR /src
RUN ./gradlew --no-daemon -i shadowJar

FROM openjdk:${VERSION}-jre

COPY --from=BUILD /src/build/libs/innMQPubWConfirm-0.0.1.jar /bin/runner/run.jar
WORKDIR /bin/runner

CMD ["java","-jar","run.jar"]