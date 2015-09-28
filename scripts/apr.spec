
%define aprver 1

Summary: Apache Portable Runtime library - Stripped
Name: apr-ACCRE
Version: 1.5.0
Release: 1
License: Apache Software License
Group: System Environment/Libraries
URL: http://apr.apache.org/
Source0: http://www.apache.org/dist/apr/%{name}-%{version}.tar.gz
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-buildroot
BuildRequires: autoconf, libtool, doxygen, python

%description
The mission of the Apache Portable Runtime (APR) is to provide a
free library of C data structures and routines, forming a system
portability layer to as many operating systems as possible,
including Unices, MS Win32, BeOS and OS/2.

Note, this library compliments the apr library and contains ace patches that
haven't made it upstream yet

%package devel
Group: Development/Libraries
Summary: APR library development kit
Requires: apr-ACCRE = %{version}
Conflicts: apr-devel

%description devel
This package provides the support files which can be used to 
build applications using the APR library.  The mission of the
Apache Portable Runtime (APR) is to provide a free library of 
C data structures and routines.

Note, this library compliments the apr library and contains ace patches that
haven't made it upstream yet

%prep
%setup -q

%build
# regenerate configure script etc.
#./buildconf
%configure \
        --prefix=/usr \
        --includedir=%{_includedir}/apr-%{aprver} \
        --with-installbuilddir=%{_libdir}/apr-%{aprver}/build \
        --enable-static --enable-shared \
        CC=gcc CXX=g++
# don't build documentation
#make %{?_smp_mflags} && make dox
make %{?_smp_mflags}

#%check
## Run non-interactive tests
#pushd test
#make %{?_smp_mflags} all CFLAGS=-fno-strict-aliasing
#make check || exit 1
#popd

%install
rm -rf $RPM_BUILD_ROOT
make install DESTDIR=$RPM_BUILD_ROOT
find $RPM_BUILD_ROOT -name *.so

# Move docs to more convenient location
#mv docs/dox/html html

# Unpackaged files:
rm -f $RPM_BUILD_ROOT%{_libdir}/apr-ACCRE.exp
#rm -rf $RPM_BUILD_ROOT%{_includedir}

%clean
rm -rf $RPM_BUILD_ROOT

%post -p /sbin/ldconfig

%postun -p /sbin/ldconfig

%files
%defattr(-,root,root,-)
%doc CHANGES LICENSE NOTICE
%{_libdir}/libapr-ACCRE-%{aprver}.so.*

%files devel
%defattr(-,root,root,-)
#%doc docs/APRDesign.html docs/canonical_filenames.html
#%doc docs/incomplete_types docs/non_apr_programs
#%doc --parents html
%{_bindir}/apr*config
%{_libdir}/libapr-ACCRE-%{aprver}.*a
%{_libdir}/libapr-ACCRE-%{aprver}.so
%{_includedir}/*
%dir %{_libdir}/apr-%{aprver}
%dir %{_libdir}/apr-%{aprver}/build
%{_libdir}/apr-%{aprver}/build/*
%{_libdir}/pkgconfig/apr-ACCRE-%{aprver}.pc
%{_datarootdir}/*

%changelog
* Sat Aug 30 2008 Graham Leggett <minfrin@sharp.fm> 1.3.3
- update to depend on the bzip2 binary
- build depends on python

* Tue Jun 22 2004 Graham Leggett <minfrin@sharp.fm> 1.0.0-1
- update to support v1.0.0 of APR

* Tue Jun 22 2004 Graham Leggett <minfrin@sharp.fm> 1.0.0-1
- derived from Fedora Core apr.spec

