
#  Releasing a new version
Currently, there are some manual steps needed in order to release a new version:

* Make sure that you're in a branch
* Change the version in the following three files: `bin/q.py`, `setup.py` and `do-manual-release.sh` and commit them to the branch
* Push the commit to the branch/PR
* perform merge into master of that branch from the github UI
* create a new release with a new tag that has the same name as the new version (x.y.z)

The merge will trigger a build/release, and will push the artifacts to the new release as assets.

Now, create the relevant rpm and deb packages:

* Run `./package-release <tag> <version>`. In most cases, both will be the same.
* This will download all the released artifacts for the release into `./packages/`, and will create an rpm and a deb
* Test that the two new artifacts (inside `./packages/`) 
* Run `./upload-release <tag> <version>`

The rpm and deb will be added to the assets of the release


Update the website to match the new version.

# Requirements
Requires a logged in github-cli (`gh`) to work

