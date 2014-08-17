%global githubsite harelba
%global shortname q
# sometimes v%{version}
%global githubversion %{version}

Name:		q-text-as-data
Version:	1.4.0
Release:	1%{?dist}.1sunshine
Summary:	q - Text as Data

Group:		Applications/Text
License:	GPLv3
URL:		https://github.com/%{githubsite}/%{shortname}
Source0:	https://github.com/%{githubsite}/%{shortname}/archive/%{githubversion}.tar.gz#/%{name}-%{version}.tar.gz
BuildArch:	noarch
BuildRequires:	/usr/bin/ronn

%description
q allows to perform SQL-like statements on tabular text data such as CSV
files.

%prep
%setup -q -n %{shortname}-%{version}

%build
# create documentation
for file in doc/*.markdown *.markdown; do
  ronn "$file"
done

%install
rm -rf ${RPM_BUILD_ROOT}
install -d -m 0755 ${RPM_BUILD_ROOT}%{_bindir}
install -p -Dm 755 bin/q ${RPM_BUILD_ROOT}%{_bindir}/
install -d -m 0755 ${RPM_BUILD_ROOT}%{_mandir}/man1/
install -p -m 0644 doc/USAGE ${RPM_BUILD_ROOT}%{_mandir}/man1/q.1

%files
%defattr(-,root,root,-)
%doc README.html
%doc doc/AUTHORS doc/CHANGELOG.html doc/IMPLEMENTATION.html doc/LICENSE doc/RATIONALE.html doc/THANKS doc/USAGE.html

%{_bindir}/%{shortname}
%{_mandir}/man1/%{shortname}.1*

%changelog
* Thu Aug 14 2014 Moritz Barsnick <moritz+rpm@barsnick.net> 1.4.0-1.1sunshine
- provide a downloadable Source0 URL
- make %%setup quiet
- preserve modification dates
- don't gzip man file explicitly
- man page does not need to be marked as %%doc
- process markdown %%docs and package them as HTML

* Sat Jun 14 2014 Harel Ben-Attia <harelba@gmail.com> 1.4.0-1
- Changed RPM package name to q-text-as-data
- Fixed RPM creation logic after folder restructuring
- Man page is now taken directly from USAGE.markdown

* Mon Mar 03 2014 Harel Ben-Attia <harelba@gmail.com> 1.3.0-1
- Version 1.3.0 packaging

* Thu Feb 20 2014 Harel Ben-Attia <harelba@gmail.com> 1.1.7-1
- Added man page

* Wed Feb 19 2014 Jens Neu <jens@zeeroos.de> 1.1.5-1
- initial release
