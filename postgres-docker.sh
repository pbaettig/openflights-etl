#!/bin/bash
docker run \
    --name openflights-etl-postgres \
    -d \
    --rm \
    -p 5432:5432 \
    -e POSTGRES_PASSWORD=1234 \
    -e POSTGRES_DB=openflights \
    postgres:11