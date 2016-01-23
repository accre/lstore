################################################################################
#  THIS FILE IS 100% GENERATED BY ZPROJECT; DO NOT EDIT EXCEPT EXPERIMENTALLY  #
#  Please refer to the README for information about making permanent changes.  #
################################################################################
TEMPLATE = lib
VERSION = 3.0.3
CONFIG += qt dll qczmq-buildlib
mac:CONFIG += absolute_library_soname
win32|mac:!wince*:!win32-msvc:!macx-xcode:CONFIG += debug_and_release build_all

include(../src/qczmq.pri)

TARGET = $$QCZMQ_LIBNAME
DESTDIR = $$QCZMQ_LIBDIR

!packagesExist (libzmq): error ("cannot link with -lzmq, install libzmq.")

LIBS += \
    -lzmq \
    -lczmq

win32 {
    DLLDESTDIR = $$[QT_INSTALL_BINS]
    QMAKE_DISTCLEAN += $$[QT_INSTALL_BINS]\$${QCZMQ_LIBNAME}.dll
}
unix {
    isEmpty(PREFIX): PREFIX = /usr/local
    header.files = $$PWD/../src/*.h
    header.path = $$PREFIX/include
    target.path = $$PREFIX/lib
    INSTALLS += target header
}
################################################################################
#  THIS FILE IS 100% GENERATED BY ZPROJECT; DO NOT EDIT EXCEPT EXPERIMENTALLY  #
#  Please refer to the README for information about making permanent changes.  #
################################################################################
