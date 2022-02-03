# multi stage build
#FROM golang:1.17
#WORKDIR /go/src/github.com/pbudner/argosminer/  
#COPY . .
#RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o app .

FROM alpine:3.15
ARG VERSION
RUN apk --no-cache add ca-certificates
RUN apk --no-cache add tzdata
ADD ./dist/argosminer-${VERSION}-linux-amd64 /etc/argosminer
RUN chmod +x /etc/argosminer
ENTRYPOINT ["/etc/argosminer"]  
CMD []
