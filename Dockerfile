FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ringcache ./cmd/ringcache

# Distroless-style: no shell, no wget. Health is `ringcache -healthcheck`.
FROM scratch
COPY --from=build /out/ringcache /ringcache
EXPOSE 8080
ENTRYPOINT ["/ringcache"]
