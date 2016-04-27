#ifndef LIO_VISIBILITY_H
#define LIO_VISIBILITY_H

// Handle symbol visibility
// From: https://gcc.gnu.org/wiki/Visibility

// Generic helper definitions for shared library support
#if defined _WIN32 || defined __CYGWIN__
  #define LIO_HELPER_DLL_IMPORT __declspec(dllimport)
  #define LIO_HELPER_DLL_EXPORT __declspec(dllexport)
  #define LIO_HELPER_DLL_LOCAL
#else
  #if __GNUC__ >= 4
    #define LIO_HELPER_DLL_IMPORT __attribute__ ((visibility ("default")))
    #define LIO_HELPER_DLL_EXPORT __attribute__ ((visibility ("default")))
    #define LIO_HELPER_DLL_LOCAL  __attribute__ ((visibility ("hidden")))
  #else
    #define LIO_HELPER_DLL_IMPORT
    #define LIO_HELPER_DLL_EXPORT
    #define LIO_HELPER_DLL_LOCAL
  #endif
#endif

// Now we use the generic helper definitions above to define LIO_API and LIO_LOCAL.
// LIO_API is used for the public API symbols. It either DLL imports or DLL exports (or does nothing for static build)
// LIO_LOCAL is used for non-api symbols.

#ifdef lio_EXPORTS // defined if we are building the LIO DLL (instead of using it)
#define LIO_API LIO_HELPER_DLL_EXPORT
#else
#define LIO_API LIO_HELPER_DLL_IMPORT
#endif // LIO_DLL_EXPORTS
#define LIO_LOCAL LIO_HELPER_DLL_LOCAL
#endif
