FROM golang:1.12 as builder

WORKDIR /src

# build the app
ADD . /src

RUN make build

CMD ["integrationtest/test.sh"]
