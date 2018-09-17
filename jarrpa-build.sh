#!/bin/bash

RUNTIME_CMD="sudo docker" REPO="localhost:5000/jarrpa/" DRIVERS="${@}" ./build.sh
