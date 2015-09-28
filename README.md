Structure
-----------------------------------------------
scripts  - Build scripts
build    - Location where all sources are built
logs     - Build logs
tarballs - Source tar balls


Build process
----------------------------------------------
The build is broken into 2 steps.
  1) Building the external dependencies
  2) Building local packages

If the external packages are already installed you can skip step 1 and 
proceed directly to step 2.  It's not required to use the specific versions 
listed in the tarballs.  These versions were used in testing the release.
By default ALL packages are build using static libraries.  This is easily changed
by making the appropriate tweaks to the scripts/*-build.sh.  The local packages build
both static and dynamic versions of the libraries with the executables all being
statically linked.


Building the external packages
----------------------------------------------
All the external dependencies can be built using:
    ./scripts/build-external.sh /path/to/install/files

(note: install path must be absolute)

This script (and most of the others as well) are designed to be run from the base
and will fail if run from somewhere else.  The exception to this rule are the scripts/*-build.sh
used for each package.  These are all designed to be run from withing the package
base directory.  Each package has it's own build script.


Building the local project packages
----------------------------------------------
These can be built from either the provided tarballs or the latest version
can be downloaded directly from the GIT repository.

Building from provided tarballs:
    ./scripts/build-local.sh /path/to/install/files tar

and to build from GIT:
    ./scripts/build-local.sh /path/to/install/files git git-user-id

The local projects are all built using scripts/my-build.sh to drive each build and 
scripts/bootstrap to configure CMake for the project.


Miscellaneous scripts
---------------------------------------------
There are a couple of miscellaneous scripts to help manage things.

dist-clean           - Completely wipes out all the builds.  Same as doing a fresh checkout/untar.
                       Should be run from the base release directory.
make_tarball.sh      - Makes a local project's tarball.  Does a fresh GIT checkout and adds 
                       a little branch info by creating a HISTORY file.
                       Can be run from anywhere.
make_all_tarballs.sh - Makes all the local tarballs and places them in the tarballs directory
                       Should be run from the base release directory.
                      

