#!/bin/bash -ex

if [ "$TRAVIS_REPO_SLUG" == "zk1931/jzab" ] &&
   [ "$TRAVIS_JDK_VERSION" == "oraclejdk7" ] &&
   [ "$TRAVIS_PULL_REQUEST" == "false" ]; then

  # Push javadoc.
  # http://benlimmer.com/2013/12/26/automatically-publish-javadoc-to-gh-pages-with-travis-ci/
  echo "Publishing javadoc..."

  cp -R target/site/apidocs $HOME/javadoc-latest
  cd $HOME
  git config --global user.email "travis@travis-ci.org"
  git config --global user.name "travis-ci"
  git clone --quiet --branch=gh-pages https://${GH_TOKEN}@github.com/zk1931/jzab gh-pages > /dev/null

  cd gh-pages
  mkdir -p $TRAVIS_BRANCH
  git rm -rf --ignore-unmatch $TRAVIS_BRANCH/javadoc
  cp -Rf $HOME/javadoc-latest $TRAVIS_BRANCH/javadoc
  git add -f .
  git commit -m "Push javadoc from travis build $TRAVIS_BUILD_NUMBER to gh-pages."
  git push -fq origin gh-pages > /dev/null

  echo "Published javadoc to gh-pages."
fi
