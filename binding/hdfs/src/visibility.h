#ifndef HDFSL_VISIBILITY_H
#define HDFSL_VISIBILITY_H

// Handle symbol visibility
// From: https://gcc.gnu.org/wiki/Visibility

// Generic helper definitions for shared library support
#if defined _WIN32 || defined __CYGWIN__
  #define HDFSL_HELPER_DLL_IMPORT __declspec(dllimport)
  #define HDFSL_HELPER_DLL_EXPORT __declspec(dllexport)
  #define HDFSL_HELPER_DLL_LOCAL
#else
  #if __GNUC__ >= 4
    #define HDFSL_HELPER_DLL_IMPORT __attribute__ ((visibility ("default")))
    #define HDFSL_HELPER_DLL_EXPORT __attribute__ ((visibility ("default")))
    #define HDFSL_HELPER_DLL_LOCAL  __attribute__ ((visibility ("hidden")))
  #else
    #define HDFSL_HELPER_DLL_IMPORT
    #define HDFSL_HELPER_DLL_EXPORT
    #define HDFSL_HELPER_DLL_LOCAL
  #endif
#endif

// Now we use the generic helper definitions above to define HDFSL_API and HDFSL_LOCAL.
// HDFSL_API is used for the public API symbols. It either DLL imports or DLL exports (or does nothing for static build)
// HDFSL_LOCAL is used for non-api symbols.

#ifdef HDFSL_EXPORTS // defined if we are building the HDFSL DLL (instead of using it)
#define HDFSL_API HDFSL_HELPER_DLL_EXPORT
#else
#define HDFSL_API HDFSL_HELPER_DLL_IMPORT
#endif // HDFSL_DLL_EXPORTS
#define HDFSL_LOCAL HDFSL_HELPER_DLL_LOCAL
#endif
