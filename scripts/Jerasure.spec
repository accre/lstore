Name: jerasure		
Version: 1.2a	
Release:	1%{?dist}
Summary: Jerasure, an implementation of reed-solomon coding	

Group:		unknown
License:	unknown
URL:		https://github.com/tsuraan/Jerasure
Source0:	Jerasure-1.2A.tar.gz
BuildRoot:	%(mktemp -ud %{_tmppath}/%{name}-%{version}-%{release}-XXXXXX)
BuildRequires:  cmake28

%description
Jerasure, an implementation of reed-solmon coding

%prep
%setup -q

%build
cmake28 -DCMAKE_SKIP_RPATH=ON \
        -DCMAKE_INSTALL_PREFIX=%{_prefix} .
make %{?_smp_mflags}

%install
rm -rf %{buildroot}
make install DESTDIR=%{buildroot}

%clean
rm -rf %{buildroot}

%files
%defattr(-,root,root,-)
/usr/lib/libjerasure.*
%{_includedir}/*.h
