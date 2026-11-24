FROM golang:1.26.11-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/crep ./cmd/crep

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/crep /crep
ENTRYPOINT ["/crep"]
