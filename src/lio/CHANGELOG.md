# **[0.4.0](https://github.com/accre/lstore-lio/tree/ACCRE_0.4.0)** (2015-11-02)

## Changes ([full changelog](https://github.com/accre/lstore-lio/compare/ACCRE_0.3.0...ACCRE_0.4.0))



# **[0.3.0](https://github.com/accre/lstore-lio/tree/ACCRE_0.3.0)** (2015-10-29)

## Changes ([full changelog](https://github.com/accre/lstore-lio/compare/ACCRE_0.1.1...ACCRE_0.3.0))
*  1ec778a Sync common cmake files


# **[0.1.1](https://github.com/accre/lstore-lio/tree/ACCRE_0.1.1)** (2015-10-28)

## Changes ([full changelog](https://github.com/accre/lstore-lio/compare/ACCRE_0.1.0...ACCRE_0.1.1))
*  75bc0c9 Fix comment
*  7063b47 Fix bug on a failed file removal leading to a double free() op


# **[0.1.0](https://github.com/accre/lstore-lio/tree/ACCRE_0.1.0)** (2015-10-28)

## Changes ([full changelog](https://github.com/accre/lstore-lio/compare/ACCRE_0.0.2...ACCRE_0.1.0))



# **[0.0.3](https://github.com/accre/lstore-lio/tree/ACCRE_0.0.3)** (2015-10-27)

## Changes ([full changelog](https://github.com/accre/lstore-lio/compare/ACCRE_0.0.1...ACCRE_0.0.3))
*  2509ab5 Add blank CHANGELOG for building
*  0077866 Partially revert version commit
*  b42241b Replace APR file locks with rename()
*  96a1468 Make sure lfs_release handles a renamed file properly
*  573b93c Migrate LFS to just use ino's instead of a file handle into the LIO core
*  5eedc2f Properly update the cache attributes on update
*  733abe0 Make OS timecache pass os_test_suite
*  0b2da87 Delay open on cache miss
*  d4eb804 Add recursion detection, set attribute expiration
*  8272c0a Minimally populate file path
*  00b0b29 Make clang happy
*  00159a4 Simplify getting the version of lib & executables
*  63d2fbb Fix assert() side effects
*  37b60f8 Remove exceptional cases from CMakeLists
*  bf2b6b5 Allow CPack generator to be set on the cmdline
*  bc411db Add missing blacklist.h header file to install list
*  15686ef Make sure and use the name in the FD instead of the path provided by FUSE since a rename operation could have taken place
*  3e4495b Preserve the stack pointer by moving up the stack when deleting off the bottom followed by an insert
*  ebe8870 Add the ability to abort an RS update instead of waiting for it to fully timeout
*  8f3898a Remove exceptional cases from CMakeLists
*  2402e87 CMake version compatibility fix involving cmake_policy, synchronize across sub-projects



