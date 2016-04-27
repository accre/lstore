#ifndef GOP_VISIBILITY_H
#define GOP_VISIBILITY_H

// Handle symbol visibility
// From: https://gcc.gnu.org/wiki/Visibility

// Generic helper definitions for shared library support
#if defined _WIN32 || defined __CYGWIN__
  #define GOP_HELPER_DLL_IMPORT __declspec(dllimport)
  #define GOP_HELPER_DLL_EXPORT __declspec(dllexport)
  #define GOP_HELPER_DLL_LOCAL
#else
  #if __GNUC__ >= 4
    #define GOP_HELPER_DLL_IMPORT __attribute__ ((visibility ("default")))
    #define GOP_HELPER_DLL_EXPORT __attribute__ ((visibility ("default")))
    #define GOP_HELPER_DLL_LOCAL  __attribute__ ((visibility ("hidden")))
  #else
    #define GOP_HELPER_DLL_IMPORT
    #define GOP_HELPER_DLL_EXPORT
    #define GOP_HELPER_DLL_LOCAL
  #endif
#endif

// Now we use the generic helper definitions above to define GOP_API and GOP_LOCAL.
// GOP_API is used for the public API symbols. It either DLL imports or DLL exports (or does nothing for static build)
// GOP_LOCAL is used for non-api symbols.

#ifdef gop_EXPORTS // defined if we are building the GOP DLL (instead of using it)
#define GOP_API GOP_HELPER_DLL_EXPORT
#else
#define GOP_API GOP_HELPER_DLL_IMPORT
#endif // GOP_DLL_EXPORTS
#define GOP_LOCAL GOP_HELPER_DLL_LOCAL
#endif
