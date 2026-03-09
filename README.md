A multi module gradle spring boot kafka streams project

kafa-ui - http://localhost:8080/

Intellij http client requests [here](kafka-streams.http)

Sample curl requests
```shell
curl -X POST --location "http://localhost:9330/api/metric/add" \
    -H "Accept: application/json" \
    -H "Content-Type: application/json" \
    -d '{
          "machineId": "server-rack-02",
          "metricType": "CPU_USAGE",
          "value": 42,
          "timestamp": 1773066416
        }'
```