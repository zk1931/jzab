#!/bin/bash -ex

if [ "$TRAVIS_REPO_SLUG" == "zk1931/jzab" ] &&
   [ "$TRAVIS_JDK_VERSION" == "oraclejdk7" ] &&
   [ "$TRAVIS_PULL_REQUEST" == "false" ]; then

  # Push javadoc.
  # http://benlimmer.com/2013/12/26/automatically-publish-javadoc-to-gh-pages-with-travis-ci/
  echo "Publishing documentation"

  # Get the project version from pom.xml.
  VERSION=`mvn -q -Dexec.executable="echo" -Dexec.args='${project.version}' --non-recursive org.codehaus.mojo:exec-maven-plugin:1.3.1:exec`

  cp -R target/site/apidocs $HOME/javadoc-latest

  # Generate HTML files.
  cd doc
  ./generate_config_yml $VERSION
  bundle install
  bundle exec jekyll build
  cp -R _site $HOME/doc-latest

  cd $HOME
  git config --global user.email "travis@travis-ci.org"
  git config --global user.name "travis-ci"
  git clone --quiet --branch=gh-pages https://${GH_TOKEN}@github.com/zk1931/jzab gh-pages > /dev/null
  cd gh-pages
  git rm -rf --ignore-unmatch $VERSION
  mv $HOME/doc-latest $VERSION
  cp -Rf $HOME/javadoc-latest $VERSION/javadoc
  # put the doc from the master branch to http://zk1931.github.io/jzab/master/
  if [ "$TRAVIS_BRANCH" == "master" ]; then
    git rm -rf --ignore-unmatch master
    cp -Rf $VERSION master
  fi
  git add -f .
  git commit -m "Push documentation from travis build $TRAVIS_BUILD_NUMBER to gh-pages."
  git push -fq origin gh-pages > /dev/null

  echo "Published documentation to gh-pages."
fi
