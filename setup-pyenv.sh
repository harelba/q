#!/usr/bin/env bash
# NOTE: This script needs to be sourced so it can modify the environment.
#
# Environment variables that can be set:
# - PYENV_VERSION
#     Python to install [required]
# - PYENV_VERSION_STRING
#     String to `grep -F` against the output of `python --version` to validate
#     that the correct Python was installed (recommended) [default: none]
# - PYENV_ROOT
#     Directory in which to install pyenv [default: ~/.travis-pyenv]
# - PYENV_RELEASE
#     Release tag of pyenv to download [default: clone from master]
# - PYENV_CACHE_PATH
#     Directory where full Python builds are cached (i.e., for Travis)

# PYENV_ROOT is exported because pyenv uses it
export PYENV_ROOT="${PYENV_ROOT:-$HOME/.travis-pyenv}"
export PYTHON_CONFIGURE_OPTS="--enable-shared"
PYENV_CACHE_PATH="${PYENV_CACHE_PATH:-$HOME/.pyenv_cache}"
version_cache_path="$PYENV_CACHE_PATH/$PYENV_VERSION"
version_pyenv_path="$PYENV_ROOT/versions/$PYENV_VERSION"

# Functions
#
# verify_python -- attempts to call the Python command or binary
# supplied in the first argument with the --version flag. If
# PYENV_VERSION_STRING is set, then it validates the returned version string
# as well (using grep -F). Returns whatever status code the command returns.
verify_python() {
  local python_bin="$1"; shift

  if [[ -n "$PYENV_VERSION_STRING" ]]; then
    "$python_bin" --version 2>&1 | grep -F "$PYENV_VERSION_STRING" &>/dev/null
  else
    "$python_bin" --version &>/dev/null
  fi
}

# use_cached_python -- Tries symlinking to the cached PYENV_VERSION and
# verifying that it's a working build. Returns 0 if it's found and it
# verifies, otherwise returns 1.
use_cached_python() {
  if [[ -d "$version_cache_path" ]]; then
    printf "Cached python found, %s. Verifying..." "$PYENV_VERSION"
    ln -s "$version_cache_path" "$version_pyenv_path"
    if verify_python "$version_pyenv_path/bin/python"; then
      printf "success!\n"
      return 0
    else
      printf "FAILED.\nClearing cached version..."
      rm -f "$version_pyenv_path"
      rm -rf "$version_cache_path"
      printf "done.\n"
      return 1
    fi
  else
    echo "No cached python found."
    return 1
  fi
}

# output_debugging_info -- Outputs useful debugging information
output_debugging_info() {
  echo "**** Debugging information"
  printf "PYENV_VERSION\n%s\n" "$PYENV_VERSION"
  printf "PYENV_VERSION_STRING\n%s\n" "$PYENV_VERSION_STRING"
  printf "PYENV_CACHE_PATH\n%s\n" "$PYENV_CACHE_PATH"
  set -x
  python --version
  "$version_cache_path/bin/python" --version
  which python
  pyenv which python
  set +x
}

# Main script begins.

if [[ -z "$PYENV_VERSION" ]]; then
  echo "PYENV_VERSION is not set. Not installing a pyenv."
  return 0
fi

# Get out of the virtualenv we're in (if we're in one).
[[ -z "$VIRTUAL_ENV" ]] || deactivate

# Install pyenv
echo "**** Installing pyenv."
if [[ -n "$PYENV_RELEASE" ]]; then
  # Fetch the release archive from Github (slightly faster than cloning)
  mkdir "$PYENV_ROOT"
  curl -fsSL "https://github.com/yyuu/pyenv/archive/$PYENV_RELEASE.tar.gz" \
    | tar -xz -C "$PYENV_ROOT" --strip-components 1
else
  # Don't have a release to fetch, so just clone directly
  git clone --depth 1 https://github.com/yyuu/pyenv.git "$PYENV_ROOT"
fi

export PATH="$PYENV_ROOT/bin:$PATH"
eval "$(pyenv init -)"

# Make sure the cache directory exists
mkdir -p "$PYENV_CACHE_PATH"

# Try using an already cached PYENV_VERSION. If it fails or is not found,
# then install from scratch.
echo "**** Trying to find and use cached python $PYENV_VERSION."
if ! use_cached_python; then
  echo "**** Installing python $PYENV_VERSION with pyenv now."
  if pyenv install "$PYENV_VERSION"; then
    if mv "$version_pyenv_path" "$PYENV_CACHE_PATH"; then
      echo "Python was successfully built and moved to cache."
      echo "**** Trying to find and use cached python $PYENV_VERSION."
      if ! use_cached_python; then
        echo "Python version $PYENV_VERSION was apparently successfully built"
        echo "with pyenv, but, once cached, it could not be verified."
        output_debugging_info
        return 1
      fi
    else
      echo "**** Warning: Python was succesfully built, but moving to cache"
      echo "failed. Proceeding anyway without caching."
    fi
  else
    echo "Python version $PYENV_VERSION build FAILED."
    return 1
  fi
fi

# Now we have to reinitialize pyenv, as we need the shims etc to be created so
# the pyenv activates correctly.
echo "**** Activating python $PYENV_VERSION and generating new virtualenv."
eval "$(pyenv init -)"
pyenv global "$PYENV_VERSION"
