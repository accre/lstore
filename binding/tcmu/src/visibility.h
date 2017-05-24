#ifndef TCMUL_VISIBILITY_H
#define TCMUL_VISIBILITY_H

// Handle symbol visibility
// From: https://gcc.gnu.org/wiki/Visibility

// Generic helper definitions for shared library support
#if defined _WIN32 || defined __CYGWIN__
  #define TCMUL_HELPER_DLL_IMPORT __declspec(dllimport)
  #define TCMUL_HELPER_DLL_EXPORT __declspec(dllexport)
  #define TCMUL_HELPER_DLL_LOCAL
#else
  #if __GNUC__ >= 4
    #define TCMUL_HELPER_DLL_IMPORT __attribute__ ((visibility ("default")))
    #define TCMUL_HELPER_DLL_EXPORT __attribute__ ((visibility ("default")))
    #define TCMUL_HELPER_DLL_LOCAL  __attribute__ ((visibility ("hidden")))
  #else
    #define TCMUL_HELPER_DLL_IMPORT
    #define TCMUL_HELPER_DLL_EXPORT
    #define TCMUL_HELPER_DLL_LOCAL
  #endif
#endif

// Now we use the generic helper definitions above to define TCMUL_API and TCMUL_LOCAL.
// TCMUL_API is used for the public API symbols. It either DLL imports or DLL exports (or does nothing for static build)
// TCMUL_LOCAL is used for non-api symbols.

#ifdef TCMUL_EXPORTS // defined if we are building the TCMUL DLL (instead of using it)
#define TCMUL_API TCMUL_HELPER_DLL_EXPORT
#else
#define TCMUL_API TCMUL_HELPER_DLL_IMPORT
#endif // TCMUL_DLL_EXPORTS
#define TCMUL_LOCAL TCMUL_HELPER_DLL_LOCAL
#endif
