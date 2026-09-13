FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ringcache ./cmd/ringcache

FROM alpine:3.20
RUN apk add --no-cache ca-certificates wget
COPY --from=build /out/ringcache /usr/local/bin/ringcache
EXPOSE 8080
ENTRYPOINT ["ringcache"]
