FROM golang:latest

COPY . .

RUN go mod download
RUN go build

ENTRYPOINT ["./schemaflow"]
