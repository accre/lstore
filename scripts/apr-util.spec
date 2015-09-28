
%define apuver 1

Summary: Apache Portable Runtime Utility library - stripped
Name: apr-util-ACCRE
Version: 1.5.3
Release: 1
License: Apache Software License
Group: System Environment/Libraries
URL: http://apr.apache.org/
Source0: http://www.apache.org/dist/apr/%{name}-%{version}.tar.gz
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-buildroot
BuildRequires: autoconf, libtool, doxygen, apr-ACCRE-devel >= 1.4.0
BuildRequires: expat-devel, libuuid-devel

%description
The mission of the Apache Portable Runtime (APR) is to provide a
free library of C data structures and routines.  This library
contains additional utility interfaces for APR; including support
for XML, LDAP, database interfaces, URI parsing and more.

%package devel
Group: Development/Libraries
Summary: APR utility library development kit
Requires: apr-util-ACCRE = %{version}-%{release}, apr-ACCRE-devel
Requires: db4-devel, expat-devel
Conflicts: apr-util-devel

%description devel
This package provides the support files which can be used to 
build applications using the APR utility library.  The mission 
of the Apache Portable Runtime (APR) is to provide a free 
library of C data structures and routines.

%prep
%setup -q

%build
%configure --with-apr=/usr/bin/apr-ACCRE-1-config \
        --includedir=%{_includedir}/apr-%{apuver} \
        --without-ldap --without-gdbm \
        --without-sqlite3 --without-pgsql --without-mysql --without-freetds --without-odbc \
        --without-berkeley-db \
        --without-crypto --with-openssl --without-nss \
        --without-sqlite2
# don't need docs
#make %{?_smp_mflags} && make dox
make %{?_smp_mflags}

# temporarily diable
#%check
# Run non-interactive tests
#pushd test
#make %{?_smp_mflags} all CFLAGS=-fno-strict-aliasing
#make check || exit 1
#popd

%install
rm -rf $RPM_BUILD_ROOT
make install DESTDIR=$RPM_BUILD_ROOT

# Documentation don't do this
#mv docs/dox/html html

# Unpackaged files
rm -f $RPM_BUILD_ROOT%{_libdir}/aprutil.exp
rm -f $RPM_BUILD_ROOT%{_libdir}/aprutil-ACCRE.exp

%clean
rm -rf $RPM_BUILD_ROOT

%post -p /sbin/ldconfig

%postun -p /sbin/ldconfig

%files
%defattr(-,root,root,-)
%doc CHANGES LICENSE NOTICE
%{_libdir}/libaprutil-ACCRE-%{apuver}.so.*
#%dir %{_libdir}/apr-util-ACCRE-%{apuver}

%files devel
%defattr(-,root,root,-)
%{_bindir}/apu-ACCRE-%{apuver}-config
%{_libdir}/libaprutil-ACCRE-%{apuver}.*a
%{_libdir}/libaprutil-ACCRE-%{apuver}.so
%{_libdir}/pkgconfig/apr-util-ACCRE-%{apuver}.pc
%{_includedir}/apr-%{apuver}/*.h
#%doc --parents html

%changelog
* Tue Jun 22 2004 Graham Leggett <minfrin@sharp.fm> 1.0.0-1
- update to support v1.0.0 of APR
                                                                                
* Tue Jun 22 2004 Graham Leggett <minfrin@sharp.fm> 1.0.0-1
- derived from Fedora Core apr.spec

