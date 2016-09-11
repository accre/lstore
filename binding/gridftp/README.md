# OSG Extensions for the Globus GridFTP server
OSG-specific extensions for the Globus GridFTP server

This Globus DSI module layers on top of the existing `file` DSI in order to add OSG-specific commands.

The initial use case is adding a space usage command so remote users can query for their usage information.

## Configuring the module

To configure the extension module, add the following line to a new file in `/etc/gridftp.d`:

```
load_dsi_module osg
```

This will cause the extension module to be used; this module layers on top of the `file` module.

If you do not use the built-in `file` module, you may override a different one using the following:

```
$OSG_EXTENSIONS_OVERRIDE_DSI foo
```
where `foo` is the DSI module you currently use.

## Site Usage
The site usage extension allows the GridFTP server to provide information about usage of space in the server.  The sysadmin must provide a script that, given a space name, returns the number of bytes used and free space available.

The semantics of this is left purposely undefined: the "space name" may refer to a directory (`/path/to/some/files`) or some other site construct ("all data written by user ABC").  Further, "usage" is similarly ill-defined when it comes to various storage backends such as tape systems.  It is expected the site works with its user community to define this.

To configure, in a configuration file in `/etc/gridftp.d`, add an environment variable defining the site usage script:
```
$OSG_SITE_USAGE_SCRIPT /usr/bin/my_site_usage.sh
```

The script `my_site_usage.sh` above should expect two arguments (the space name and path) and return:
- Space usage in bytes (required)
- Available space in bytes (required)
- Total space in bytes.  This is optional: if not provided, the total is assumed to be the sum of the used and available space.

With this enabled, one can test the server's response using `uberftp`:
```
$ uberftp gridftp.example.com
220 gridftp.example.com GridFTP Server 10.4 (gcc64, 1463669416-85) [Globus Toolkit 6.0] ready.
230 User johndoe logged in.
UberFTP (2.8)> quote SITE USAGE TOKEN foo /path
250 USAGE 86028 FREE 1234 TOTAL 87263
```

(In the above session, `quote` indicates we are using a command that `uberftp` is not familiar with.)

We can also test the command by hand; the following is equivalent to the above remote session:
```
$ /usr/bin/my_site_usage.sh foo /path
86028 1234 87263
```

The SITE USAGE command has the following syntax:

```
SITE <sp> USAGE <sp> [TOKEN <sp> $token <sp>] $path
```
If not specified, the value of `$token` is `default`.  The response has the following syntax:

```
250 USAGE <sp> $val FREE $val2 TOTAL $val3
```

where `$val`, `$val2`, and `$val3` are integers.  The successful response code is 250; any other response code indicates an error.

## Logging changes

The extensions DSI will automatically add extra information about any present VOMS extension to the `TRANSFER` log level.

For proxies without VOMS extensions, this line is expected:

```
[1582694] Tue Jul 19 22:02:05 2016 :: No VOMS info in credential.
```

For a single VOMS extensions:

```
[1582658] Tue Jul 19 22:01:04 2016 :: VO osg /osg/Role=NULL/Capability=NULL,/osg/ligo/Role=NULL/Capability=NULL
```

And, for multiple VOMS extensions:

```
[1582658] Tue Jul 19 22:01:04 2016 :: VO osg /osg/Role=NULL/Capability=NULL,/osg/ligo/Role=NULL/Capability=NULL
[1582658] Tue Jul 19 22:01:04 2016 :: VO cms /cms/Role=NULL/Capability=NULL,/cms/integration/Role=NULL/Capability=NULL,/cms/uscms/Role=NULL/Capability=NULL
```

To see the transfer-level logging, we recommend the following log statement in `gridftp.conf`:
```
log_level ERROR,WARN,INFO,TRANSFER
```
