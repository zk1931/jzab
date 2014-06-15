#!/bin/bash
#
# Ideally we should run FindBugs at compile time, but findbugs-maven-plugin
# 2.5.4 uses FindBugs 2.0.3, which does not work well with Java 1.8. From
# http://findbugs.sourceforge.net/:
#
# The current version of FindBugs may encounter errors when analyzing Java 1.8
# bytecode, due to changes in the classfile format. After FindBugs 2.0.3 is
# released, work will start on the next major release of FindBugs, which will
# be able to analyze Java 1.8 (and will require Java 1.7 to compile and run).
#
if [ "$TRAVIS_JDK_VERSION" != "oraclejdk8" ]; then
  mvn findbugs:check
fi
