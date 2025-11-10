FROM golang:1.24-alpine AS build
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o binance_ws binance_ws.go

FROM alpine:3.19
WORKDIR /app
COPY --from=build /app/binance_ws .
COPY config.json .
ENV HL_CONFIG_PATH=/app/config.json
CMD ["./binance_ws"]
