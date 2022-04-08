FROM openjdk:12-alpine

ENV XDG_CONFIG_HOME /home/app

RUN addgroup -S ids && adduser -S -g ids ids
RUN mkdir -p /home/app/ && chown -R ids /home/app/ && chmod -R g+w /home/app/
RUN mkdir -p /ids/repo/ && chown -R ids /ids/repo/ && chmod -R g+w /ids/repo/
WORKDIR /home/app/
RUN chown -R ids: ./ && chmod -R u+w ./
RUN chown -R ids: /ids/repo/ && chmod -R u+w /ids/repo/
RUN mkdir -p /ids/certs/ && chown -R ids: /ids/certs/ && chmod -R u+w /ids/certs/

COPY /target/public-data-space-connector-2.1.0-fat.jar .
EXPOSE 8080
USER ids
ENTRYPOINT ["java", "-jar", "./public-data-space-connector-2.1.0-fat.jar"]
