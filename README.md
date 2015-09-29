LStore Release Tools
==============================================

Structure
-----------------------------------------------
* scripts  - Build scripts
* build    - Location where all sources are built
* logs     - Build logs
* local    - Installation path for packages built with build-*.sh

Build process
----------------------------------------------
The build is broken into 2 steps.

1. Building the external dependencies
2. Building local packages

If the external packages are already installed you can skip step 1 and 
proceed directly to step 2. 

Building the external packages
----------------------------------------------
All the external dependencies can be built using:
>    ./scripts/build-external.sh

These only include ACCRE-modified externals. You will need to bring your own
copies of:

* openssl-devel
* czmq-devel
* zmq-devel
* zlib-devel
* fuse-devel

In addition, LStore has build-time dependencies on

* C, C++ compiler
* cmake

For centos, at least, these dependencies can be installed with:

```
yum groupinstall "Development Tools"
yum install cmake openssl-devel czmq-devel zmq-devel zlib-devel fuse-devel
```

Building the local project packages
----------------------------------------------
All of the local dependencies can be built using:
>    ./scripts/build-local.sh

