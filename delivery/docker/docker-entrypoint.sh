#!/usr/bin/env bash

# This entrypoint can be used to run other processes at container start time before invoking the correspondnig cmd.

# Keep this line to ensure that the cmd is executed in a way that signals can be correctly handled (https://hynek.me/articles/docker-signals/)
exec "$@"
