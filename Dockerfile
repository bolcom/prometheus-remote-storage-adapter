FROM golang:latest 
RUN mkdir /app 
ADD prometheus-remote-storage-adapter /app/ 
WORKDIR /app 
ENTRYPOINT ["/app/prometheus-remote-storage-adapter"]
