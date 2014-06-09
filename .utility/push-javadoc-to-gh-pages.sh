#!/bin/bash
#
# http://benlimmer.com/2013/12/26/automatically-publish-javadoc-to-gh-pages-with-travis-ci/
#

if [ "$TRAVIS_REPO_SLUG" == "ZK-1931/javazab" ] &&
   [ "$TRAVIS_JDK_VERSION" == "oraclejdk7" ] &&
   [ "$TRAVIS_PULL_REQUEST" == "false" ] &&
   [ "$TRAVIS_BRANCH" == "master" ]; then

  echo -e "Publishing javadoc...\n"

  cp -R target/site/apidocs $HOME/javadoc-latest
  cd $HOME
  git config --global user.email "travis@travis-ci.org"
  git config --global user.name "travis-ci"
  git clone --quiet --branch=gh-pages https://${GH_TOKEN}@github.com/ZK-1931/javazab gh-pages > /dev/null

  cd gh-pages
  git rm -rf ./javadoc
  cp -Rf $HOME/javadoc-latest ./javadoc
  git add -f .
  git commit -m "Push javadoc from travis build $TRAVIS_BUILD_NUMBER to gh-pages."
  git push -fq origin gh-pages > /dev/null

  echo -e "Published javadoc to gh-pages.\n"
fi
