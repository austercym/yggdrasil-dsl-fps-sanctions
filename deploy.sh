#!/usr/bin/env bash
FILE=yggdrasil-dsl-fps-sanctions-0.1.1.jar

mvn clean package -P ovh-sid
scp target/$FILE jump:$FILE
ssh jump