#ifndef IBP_VISIBILITY_H
#define IBP_VISIBILITY_H

// Handle symbol visibility
// From: https://gcc.gnu.org/wiki/Visibility

// Generic helper definitions for shared library support
#if defined _WIN32 || defined __CYGWIN__
  #define IBP_HELPER_DLL_IMPORT __declspec(dllimport)
  #define IBP_HELPER_DLL_EXPORT __declspec(dllexport)
  #define IBP_HELPER_DLL_LOCAL
#else
  #if __GNUC__ >= 4
    #define IBP_HELPER_DLL_IMPORT __attribute__ ((visibility ("default")))
    #define IBP_HELPER_DLL_EXPORT __attribute__ ((visibility ("default")))
    #define IBP_HELPER_DLL_LOCAL  __attribute__ ((visibility ("hidden")))
  #else
    #define IBP_HELPER_DLL_IMPORT
    #define IBP_HELPER_DLL_EXPORT
    #define IBP_HELPER_DLL_LOCAL
  #endif
#endif

// Now we use the generic helper definitions above to define IBP_API and IBP_LOCAL.
// IBP_API is used for the public API symbols. It either DLL imports or DLL exports (or does nothing for static build)
// IBP_LOCAL is used for non-api symbols.

#ifdef ibp_EXPORTS // defined if we are building the IBP DLL (instead of using it)
#define IBP_API IBP_HELPER_DLL_EXPORT
#else
#define IBP_API IBP_HELPER_DLL_IMPORT
#endif // IBP_DLL_EXPORTS
#define IBP_LOCAL IBP_HELPER_DLL_LOCAL
#endif
