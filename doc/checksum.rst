.. _chksum:

:c:type:`chksum_t` --- Checksum Wrapper
=======================================
Wraps platform-specific checksum routines. Declared in `tbx/chksum.h`

API
----
.. doxygenfunction:: convert_bin2hex
    :outline:
.. doxygenfunction:: chksum_valid_type
    :outline:
.. doxygenfunction:: chksum_set
    :outline:
.. doxygenfunction:: chksum_name_type
    :outline:
.. doxygenfunction:: blank_chksum_set
    :outline:
.. doxygendefine:: chksum_name
    :outline:
.. doxygendefine:: chksum_type
    :outline:
.. doxygendefine:: chksum_reset
    :outline:
.. doxygendefine:: chksum_size
    :outline:
.. doxygendefine:: chksum_get
    :outline:
.. doxygendefine:: chksum_clear
    :outline:

Structs
-------
.. doxygenstruct:: chksum_t
    :members:

Typedefs
--------
.. doxygentypedef:: chksum_reset_fn_t

.. doxygentypedef:: chksum_size_fn_t

.. doxygentypedef:: chksum_get_fn_t

.. doxygentypedef:: chksum_add_fn_t

Preprocessor Constants
----------------------
.. doxygendefine:: CHKSUM_STATE_SIZE

.. doxygengroup:: CHECKSUM_ALGORITHM

.. doxygengroup:: CHECKSUM_DIGEST


