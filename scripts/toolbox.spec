Name:	accre-toolbox
Version: 1.0.0
Release:	1%{?dist}
Summary: A collection of ACCRE tools	

Group:	unknown
License: unknown	
URL: http://www.accre.vanderbilt.edu
Source0: toolbox.tar.gz	
BuildRoot:	%(mktemp -ud %{_tmppath}/%{name}-%{version}-%{release}-XXXXXX)
BuildRequires: cmake28

%description
A collection of ACCRE tools

%prep
%setup -q

%build
%configure
#CMAKE_PREFIX_PATH=${PREFIX} 
cmake28 -D CMAKE_INSTALL_PREFIX=%{buildroot} -D CMAKE_INCLUDE_CURRENT_DIR=on -D CMAKE_VERBOSE_MAKEFILE=on CMakeLists.txt
make %{?_smp_mflags}

%install
rm -rf %{buildroot}
make install

%clean
rm -rf %{buildroot}

%files
%defattr(-,root,root,-)
