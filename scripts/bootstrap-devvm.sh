rpm -uvh http://repo.grid.iu.edu/osg/3.2/osg-3.2-el6-release-latest.rpm
yum install yum-priorities
yum groupinstall "Development Tools"
yum install -y vim gcc gcc-c++ cmake28 git globus-xio-devel globus-gridftp-server-devel fuse-devel libattr-devel globus-gridftp-server-progs uberftp
debuginfo-install globus-gridftp-server-progs man osg-ca-certs
