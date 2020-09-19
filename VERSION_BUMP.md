
# Version bump
Currently, there are some manual steps needed in order to release a new version:

* Make sure that you're in a branch
* Change the version in the following three files: `bin/q.py`, `setup.py` and `do-manual-release.sh` and commit them to the branch
* perform merge into master of that branch
* add a tag of the release version
* `git push --tags origin master`
* create a release in github with the tag you've just created

Pushing to master will trigger a build/release, and will push the artifacts to the new release as assets.

The reason for this is related to limitations in the way that pyci uploads the binaries to github.

#

TBD - Continue with the flow of wrapping the artifacts with rpm/deb, copying the files to packages-for-q, and updating the web site.
