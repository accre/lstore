# LStore handler for tcmu-runner
`tcmu-runner` provides a daemon to handle the userspace side of the Linux I/O backstore (TCM).  The LStore
handler allows any LStore file to be used as a TCM backstore which can then be exported as a taqrget using
any of the available fabrics (FC, iSCSI, loopback, etc).

## Packages
The handler is designed to work with the *-fb variants of configshell, rtslib, and targetcli. These projects
are changing rapidly and it may be required to download them directry from github - `https://github.com/open-iscsi`

`tcmu-runner` does not appear to be fully baked for installation with many header files missing during the install.
As a result the CMake FindTCMU.cmake helper has a few hacks to add the paths for compilation.  You need to specify the
TCMU runner source code path when invoking CMake using the `TCMU_PREFIX` variable. As the TCMU-runner package matures
THe helper will invariably need to be changed.

## Loading the module
Just place the handler_lstore.so in the handler directory.  This defaults to

```
${PREFIX}/lib/tcmu-runner
```

where `${PREFIX}` is the install path for `tcmu-runner`. For debugging you can symlink the `.so` in your working
directory to that in the `tcmu-runner` handler directory.  Additionally you can pass arguments to the plugin via 
the normal environment variables:

```
LIO_OPTIONS="-d 1 -log /tmp/tcmu.log" tcmu-runner
```

## Creating a backstore
The frist step is to create an LStore file of the appropriate size.  The easist way to do it is using `truncate`
on the file via the LFS mount.

Next you need to create a backstore.  To do this run `targetcli` and change to the `/backstores/user:lstore` directory.
If this directory doesn't exist then the LStore handler didn't load.

Once in the directory you can run `help create` to see the options.  The `cfgstring` format is just the LStore path.
`@:` are not allowed characters so just provide the path.  You also need to provide the size and a backstore name:

```
create name=lst0 size=1G cfgstring=/tcmu/foo.dev
```

This will create a backstore labeld `lst0` of size `1Gb` using the LStore file `/tcmu/foo.dev`.  The file must be 
of equal size of larger than the size provided for the create.

## Mapping it to a target
Change to the fabric directory you want. For the example we'll use the `loopback` fabric.  Once in the directory
you need to create the WNN.  Just run `create` with no options to create a random WWN.  You can specify the WWN
if you want.

Now change into the WNN's `luns` directory.  Here's where you'll create the target and activate the backstore.

```
create /backstores/user:lstore/lst0 lun0
```

The command above uses the backstore created earlier and creates a LUN called `lun0`.

This should create a new `/dev` entry for use.

