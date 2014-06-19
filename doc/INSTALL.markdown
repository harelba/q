# q - Treating Text as a Database 

## Requirements
* Just Python 2.5 and up or Python 2.4 with sqlite3 module installed. Python 3.x is not supported yet.

## Installation
Current stable version is `1.4.0`. 

Requirements: Just Python 2.5 and up or Python 2.4 with sqlite3 module installed. Python 3.x is not supported yet.

### Mac Users
Make sure you run `brew update` first and then just run `brew install q`. 

Thanks [@stuartcarnie](https://github.com/stuartcarnie) for the initial homebrew formula

### Manual installation (very simple, since there are no dependencies)

1. Download the main q executable from **[here](https://raw.github.com/harelba/q/1.4.0/bin/q)** into a folder in the PATH.
2. Make the file executable.

For `Windows` machines, also download q.bat **[here](https://raw.github.com/harelba/q/1.4.0/bin/q.bat)** into the same folder and use it to run q.

### RPM-Base Linux distributions
Download the version `1.4.0` RPM here **[here](https://github.com/harelba/packages-for-q/raw/master/rpms/q-text-as-data-1.4.0-1.noarch.rpm)**. 

Install using `rpm -ivh <rpm-name>`.

RPM Releases also contain a man page. Just enter `man q`.

**NOTE** In Version `1.4.0`, the RPM package name has been changed. If you already have the old version, just remove it with `rpm -e q` before installing.

### Debian-based Linux distributions
Debian packaing is in progress. In the mean time install manually. See the section above.

### Arch Linux

A `PKGBUILD` is [available](https://aur.archlinux.org/packages/q/) in AUR for Arch Linux users. See [installing packages](https://wiki.archlinux.org/index.php/Arch_User_Repository#Installing_packages) in the wiki or use your favorite [AUR helper](https://wiki.archlinux.org/index.php/Aur_helpers).

The man page is also provided (extracted _as is_ from the RPM): `man q`

## Default settings file
q supports an option file in ~/.qrc or in the working directory (with the name .qrc) which provides defaults for some or all of the command line options. A sample .qrc file with commented-out options is included. Just put it in your home folder and modify it according to your needs.

## Contact
Any feedback/suggestions/complaints regarding this tool would be much appreciated. Contributions are most welcome as well, of course.

Harel Ben-Attia, harelba@gmail.com, [@harelba](https://twitter.com/harelba) on Twitter

