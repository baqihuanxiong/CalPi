ARG GO_VERSION=1.12

FROM golang:${GO_VERSION}-stretch AS builder

WORKDIR /src

COPY ./ ./

RUN ln -sf `pwd` ${GOPATH}/src/CalPi \
    && go build -o /cal-pi ${GOPATH}/src/CalPi

FROM scratch as fianl

COPY --from=builder /cal-pi /cal-pi

COPY --from=builder /go/src/CalPi/config.json /config.json

EXPOSE 8989

ENTRYPOINT ["/cal-pi"]