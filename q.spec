Name:           q
Version:        QVERSION
Release:        1%{?dist}
Summary:        Command-line utility for processing tabular input

License:        GPLv2
URL:           https://github.com/harelba/q 
Source0:	%{name}-%{version}.tar.gz

Requires:       python >= 2.7
Requires:       python-libs

%description

%prep
%setup -q -n src

%build

%install
install -Dm 755 %{_builddir}/src/q %{buildroot}/usr/bin/q

%files
%defattr(-, root, root, -)
%doc README.markdown exampledatafile
%dir %attr(-, root, root) /usr/bin
/usr/bin/q

%changelog
