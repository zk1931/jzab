#!/bin/bash

if [ "$TRAVIS_PULL_REQUEST" == "false" ]; then
  mvn clean deploy
else
  mvn clean verify
fi

