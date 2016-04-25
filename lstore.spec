# -*- rpm-spec -*-
%define _basename lstore
%define _version 0.5.1
%define _release 1
%define _prefix /usr

URL: http://www.lstore.org
Name: %{_basename}
Version: %{_version}
Release: %{_release}
Summary: LStore - Logistical Storage
License: Apache2
BuildRoot: %{_builddir}/%{_basename}-root
Source: https://github.com/accre/lstore-release/archive/v${_version}.zip

%description
LStore - Logistical Storage.

%prep
%setup -q -n LStore-v%{_version}-Source

%build
CFLAGS="-I%{_prefix}/include $RPM_OPT_FLAGS"
CMAKE_FLAGS="-DCMAKE_INSTALL_PREFIX=%{_prefix}"
cmake $CMAKE_FLAGS .
make

%install
make DESTDIR=$RPM_BUILD_ROOT install

%clean
rm -rf $RPM_BUILD_ROOT

%post -p /sbin/ldconfig

%postun -p /sbin/ldconfig

%files
%defattr(-,root,root,-)
/usr/bin
/usr/include
/usr/lib64

%changelog
* Sat Apr 23 2016 Andrew Melo <andrew.m.melo@vanderbilt.edu> 0.5.1-1
- Several bug fixed.
