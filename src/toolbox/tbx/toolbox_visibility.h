#ifndef TOOLBOX_VISIBILITY_H
#define TOOLBOX_VISIBILITY_H

// Handle symbol visibility
// From: https://gcc.gnu.org/wiki/Visibility

// Generic helper definitions for shared library support
#if defined _WIN32 || defined __CYGWIN__
  #define TBX_HELPER_DLL_IMPORT __declspec(dllimport)
  #define TBX_HELPER_DLL_EXPORT __declspec(dllexport)
  #define TBX_HELPER_DLL_LOCAL
#else
  #if __GNUC__ >= 4
    #define TBX_HELPER_DLL_IMPORT __attribute__ ((visibility ("default")))
    #define TBX_HELPER_DLL_EXPORT __attribute__ ((visibility ("default")))
    #define TBX_HELPER_DLL_LOCAL  __attribute__ ((visibility ("hidden")))
  #else
    #define TBX_HELPER_DLL_IMPORT
    #define TBX_HELPER_DLL_EXPORT
    #define TBX_HELPER_DLL_LOCAL
  #endif
#endif

// Now we use the generic helper definitions above to define TBX_API and TBX_LOCAL.
// TBX_API is used for the public API symbols. It either DLL imports or DLL exports (or does nothing for static build)
// TBX_LOCAL is used for non-api symbols.

#ifdef toolbox_EXPORTS // defined if we are building the TBX DLL (instead of using it)
#define TBX_API TBX_HELPER_DLL_EXPORT
#else
#define TBX_API TBX_HELPER_DLL_IMPORT
#endif // TBX_DLL_EXPORTS
#define TBX_LOCAL TBX_HELPER_DLL_LOCAL
#endif
