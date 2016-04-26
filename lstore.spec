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
Source: https://github.com/accre/lstore-release/archive/LStore-%{_version}.tar.gz

%description
LStore - Logistical Storage.

%prep
%setup -n LStore-%{_version}

%build
CFLAGS="-I%{_prefix}/include $RPM_OPT_FLAGS"
CMAKE_FLAGS="-DCMAKE_INSTALL_PREFIX=%{_prefix}"
CMAKE_FLAGS="$CMAKE_FLAGS -DINSTALL_YUM_RELEASE:BOOL=ON"
CMAKE_FLAGS="$CMAKE_FLAGS -DINSTALL_META:BOOL=ON"
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
/usr/lib64

%changelog
* Sat Apr 23 2016 Andrew Melo <andrew.m.melo@vanderbilt.edu> 0.5.1-1
- Several bug fixed.

%package devel
Summary: Development files for LStore
Group: Development/System
Requires: lstore
%description devel
Development files for LStore
%files devel
/usr/include

%package meta
Summary: Default LStore configuration
Group: Development/System
Requires: lstore
%description meta
Default LStore configuration
%files meta
%config(noreplace) /etc/lio
%config(noreplace) /etc/logrotate.d/lstore

%package release
Version: 1.0.0
Summary: LStore repository meta-package
Group: Development/System
%description release
Installs LStore yum repository
%files release
%config /etc/yum.repos.d/lstore.repo

