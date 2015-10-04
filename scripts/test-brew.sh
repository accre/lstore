#!/bin/bash
[ -e homebrew-test ] && rm -rf homebrew-test
git clone git@github.com:Homebrew/homebrew.git homebrew-test
homebrew-test/bin/brew tap accre/accre
homebrew-test/bin/brew install lstore-lio --verbose --HEAD
