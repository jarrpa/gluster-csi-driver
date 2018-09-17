#!/bin/bash

REPO="localhost:5000/jarrpa" DOCKER_CMD="sudo docker" make "${@:-all-push}"
