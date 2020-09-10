
# Generate web site

# mkdocs folder under project root
$ `cd mkdocs`

* create a pyenv virtual environment 

$ `pip install -r requirements.txt`

$ `./generate-web-site.sh` (static files will be generated into `./generated-site`)

$ `git checkout gh-pages`

$ `cd ../`   # back to project root

$ `scp -r mkdocs/generated-site/* ./`

$ `git add` all modified files

* commit to git 

$ `git push origin gh-pages`

