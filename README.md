# emporia-collector
This is a simple collector that pulls data from the Emporia APIs and loads it to a local database through 
a call to a local API.  The process will pull the current day and previous days daily metrics once an hour.  Future 
enhancements could reduce the number of pulls from the previous day, however, this will do a full day overlap to
ensure that no records are missed.  

This project is very specific to my Home Lab setup and uses a defined local API that is defined in a separate project. 
The API provides CRUD operations around my local PostgreSQL collection of this data.  More details can be found at: https://github.com/jaysuzi5-organization/emporia

This code is heavily borrowed from PyEmVue but simplified specific to my needs.  PyEmVue was not working for me with 
Python 3.12 and instead of running an older version, I pulled out or rewrote some portion to work for my needs.  I 
STRONGLY recommend using their version as it is a much more robust solution.
https://pypi.org/project/pyemvue/0.9.5/

## Project Structure

```bash
.
├── Dockerfile
├── requirements.txt
├── src/
│   └── emporia-collector.py
└── .env
```

## .env
The following environment variables need to be defined, these can be in an .env file.  

```bash
USERNAME='<<Emporia username>>'
PASSWORD='<<Emporia password>>'
CLIENT_ID='<Emporia Client Id>
```

## Error Handling

There is no specific retry logic at this time. If there are errors with one session, this should be logged and it will
retry the same pull for a full 24 hours. 

## Traces, Logs, and Metrics

Logs are exposed as OpenTelemetry.  When running locally, the collector will capture Traces to Tempo, Logs to Splunk, 
and metrics to Prometheus. 

## Docker File

```bash
docker login
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  -t jaysuzi5/emporia-collector:latest \
  --push .
```
