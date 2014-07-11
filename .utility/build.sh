#!/bin/bash

if [ "$TRAVIS_JDK_VERSION" == "oraclejdk7" ] &&
   [ "$TRAVIS_PULL_REQUEST" == "false" ] &&
   [ "$TRAVIS_BRANCH" == "master" ]; then
  env
  mvn clean deploy
else
  env
  mvn clean verify
fi

