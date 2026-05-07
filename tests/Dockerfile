FROM ghcr.io/orange-opensource/hurl:latest
COPY . /tests
WORKDIR /tests
ENTRYPOINT ["hurl", "--test", "--variable", "host=aquifer:8080"]
