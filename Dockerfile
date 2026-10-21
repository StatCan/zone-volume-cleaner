FROM golang:1.26.0 AS builder

WORKDIR /app

# Copy Go modules and dependencies to leverage docker caching
COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o zone-volume-cleaner .

FROM alpine:3.24.2

COPY --from=builder /app/zone-volume-cleaner /zone-volume-cleaner
CMD ["/zone-volume-cleaner"]
