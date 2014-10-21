#!/bin/bash -ex

if [ "$TRAVIS_REPO_SLUG" == "zk1931/jzab" ] &&
   [ "$TRAVIS_JDK_VERSION" == "oraclejdk7" ] &&
   [ "$TRAVIS_PULL_REQUEST" == "false" ]; then

  # Push javadoc.
  # http://benlimmer.com/2013/12/26/automatically-publish-javadoc-to-gh-pages-with-travis-ci/
  echo "Publishing documentation"

  cp -R target/site/apidocs $HOME/javadoc-latest
  cp -R doc $HOME/doc-latest
  cd $HOME
  git config --global user.email "travis@travis-ci.org"
  git config --global user.name "travis-ci"
  git clone --quiet --branch=gh-pages https://${GH_TOKEN}@github.com/zk1931/jzab gh-pages > /dev/null
  cd gh-pages
  git rm -rf --ignore-unmatch $TRAVIS_BRANCH
  mv $HOME/doc-latest $TRAVIS_BRANCH
  cp -Rf $HOME/javadoc-latest $TRAVIS_BRANCH/javadoc
  git add -f .
  git commit -m "Push documentation from travis build $TRAVIS_BUILD_NUMBER to gh-pages."
  git push -fq origin gh-pages > /dev/null

  echo "Published documentation to gh-pages."
fi
