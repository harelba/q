#!/usr/bin/env python3

import sys
import os
import re
import subprocess
from pathlib import Path

INIT_FILE = Path("qtextasdata/__init__.py")

def get_current_version():
    """Read the current version from __init__.py"""
    with open(INIT_FILE, "r") as f:
        content = f.read()
    match = re.search(r"q_version\s*=\s*['\"]([^'\"]+)['\"]", content)
    if not match:
        raise ValueError(f"Could not find version in {INIT_FILE}")
    return match.group(1)

def update_version(new_version):
    """Update the version in __init__.py"""
    with open(INIT_FILE, "r") as f:
        content = f.read()
    
    updated_content = re.sub(r"(q_version\s*=\s*['\"])[^'\"]+(['\"])", 
                            r"\g<1>" + new_version + r"\g<2>", 
                            content)
    
    with open(INIT_FILE, "w") as f:
        f.write(updated_content)
    
    print(f"Updated version to {new_version} in {INIT_FILE}")

def bump_version(current_version, bump_type):
    """Bump the version according to semver rules"""
    if bump_type not in ["major", "minor", "patch", "prerelease"]:
        raise ValueError("Bump type must be 'major', 'minor', 'patch', or 'prerelease'")
    
    # Handle prerelease versions
    if "-" in current_version:
        base, prerelease = current_version.split("-", 1)
        if bump_type == "prerelease":
            # Increment prerelease number
            pre_type, pre_num = re.match(r"([a-zA-Z]+)\.?(\d+)?", prerelease).groups()
            if pre_num is None:
                pre_num = 1
            else:
                pre_num = int(pre_num) + 1
            return f"{base}-{pre_type}.{pre_num}"
        else:
            # If bumping a prerelease to release, use the base version
            major, minor, patch = map(int, base.split("."))
    else:
        major, minor, patch = map(int, current_version.split("."))
    
    if bump_type == "major":
        return f"{major + 1}.0.0"
    elif bump_type == "minor":
        return f"{major}.{minor + 1}.0"
    elif bump_type == "patch":
        return f"{major}.{minor}.{patch + 1}"
    elif bump_type == "prerelease":
        # Create a new prerelease
        return f"{major}.{minor}.{patch}-beta.1"

def create_git_tag(version):
    """Create a git tag for the new version"""
    tag = f"v{version}"
    
    try:
        # Check if tag already exists
        result = subprocess.run(["git", "tag", "-l", tag], 
                               capture_output=True, text=True, check=True)
        if tag in result.stdout:
            print(f"Warning: Tag {tag} already exists!")
            return False
        
        # Create and push tag
        subprocess.run(["git", "tag", "-a", tag, "-m", f"Release {version}"], check=True)
        print(f"Created git tag: {tag}")
        
        print("\nTo push the tag, run:")
        print(f"  git push origin {tag}")
        
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error creating git tag: {e}")
        return False

def show_help():
    """Display help information"""
    print("Usage: python bump-version.py <major|minor|patch|prerelease> [--tag]")
    print("\nOptions:")
    print("  major        Increment the MAJOR version (incompatible API changes)")
    print("  minor        Increment the MINOR version (add functionality, backwards compatible)")
    print("  patch        Increment the PATCH version (bug fixes, backwards compatible)")
    print("  prerelease   Create or increment a prerelease version")
    print("  --tag        Create a git tag for the new version")
    print("  --help       Show this help message")
    print("\nExamples:")
    print("  python bump-version.py patch          # 4.0.0 -> 4.0.1")
    print("  python bump-version.py minor          # 4.0.0 -> 4.1.0")
    print("  python bump-version.py prerelease     # 4.0.0 -> 4.0.0-beta.1")
    print("  python bump-version.py patch --tag    # 4.0.0 -> 4.0.1 and create tag v4.0.1")
    sys.exit(0)

def main():
    if len(sys.argv) < 2 or "--help" in sys.argv or "-h" in sys.argv:
        show_help()
    
    bump_type = sys.argv[1].lower()
    create_tag = "--tag" in sys.argv
    
    try:
        current_version = get_current_version()
        print(f"Current version: {current_version}")
        
        new_version = bump_version(current_version, bump_type)
        print(f"New version: {new_version}")
        
        confirm = input(f"Update version from {current_version} to {new_version}? [y/N] ")
        if confirm.lower() != "y":
            print("Version update cancelled.")
            sys.exit(0)
        
        update_version(new_version)
        
        if create_tag:
            create_git_tag(new_version)
    
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 