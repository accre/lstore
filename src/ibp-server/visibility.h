#ifndef IBPSERVER_VISIBILITY_H
#define IBPSERVER_VISIBILITY_H

// Handle symbol visibility
// From: https://gcc.gnu.org/wiki/Visibility

// Generic helper definitions for shared library support
#if defined _WIN32 || defined __CYGWIN__
  #define IBPS_HELPER_DLL_IMPORT __declspec(dllimport)
  #define IBPS_HELPER_DLL_EXPORT __declspec(dllexport)
  #define IBPS_HELPER_DLL_LOCAL
#else
  #if __GNUC__ >= 4
    #define IBPS_HELPER_DLL_IMPORT __attribute__ ((visibility ("default")))
    #define IBPS_HELPER_DLL_EXPORT __attribute__ ((visibility ("default")))
    #define IBPS_HELPER_DLL_LOCAL  __attribute__ ((visibility ("hidden")))
  #else
    #define IBPS_HELPER_DLL_IMPORT
    #define IBPS_HELPER_DLL_EXPORT
    #define IBPS_HELPER_DLL_LOCAL
  #endif
#endif

// Now we use the generic helper definitions above to define IBPS_API and IBPS_LOCAL.
// IBPS_API is used for the public API symbols. It either DLL imports or DLL exports (or does nothing for static build)
// IBPS_LOCAL is used for non-api symbols.

#ifdef ibpserver_EXPORTS // defined if we are building the IBPS DLL (instead of using it)
#define IBPS_API IBPS_HELPER_DLL_EXPORT
#else
#define IBPS_API IBPS_HELPER_DLL_IMPORT
#endif // IBPS_DLL_EXPORTS
#define IBPS_LOCAL IBPS_HELPER_DLL_LOCAL
#endif
