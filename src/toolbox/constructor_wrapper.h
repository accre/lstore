/*
 * We occasionally need to perform menial initialization/destruction tasks to
 * set up the environment for subsystems. For instance, subsystem 'foo' needs
 * an APR pool to work, so it has init_foo()/destroy_foo(). Problem is, any
 * code using these subsystems need to both know about this requirement and
 * implement it properly. Done right, it's obnoxious. Done wrong, it's either a
 * leak or a crash waiting to happen.
 *
 * Standard C doesn't have any way around it, but most (all?) compilers/targets
 * support marking certain functions to be run before/after main() is executed.
 * Subsystems can exploit this functionality to ensure their
 * constructors/destructors are properly executed without depending on someone
 * else.
 *
 * The following example use will execute construct_fn/destruct_fn before/after
 * main():
 * 
 *    #include "constructor_wrapper.h"
 *    #ifdef ACCRE_CONSTRUCTOR_PREPRAGMA_ARGS
 *    #pragma ACCRE_CONSTRUCTOR_PREPRAGMA_ARGS(construct_fn)
 *    #endif
 *    ACCRE_DEFINE_CONSTRUCTOR(construct_fn)
 *    #ifdef ACCRE_CONSTRUCTOR_POSTPRAGMA_ARGS
 *    #pragma ACCRE_CONSTRUCTOR_POSTPRAGMA_ARGS(construct_fn)
 *    #endif
 *    
 *    #ifdef ACCRE_DESTRUCTOR_PREPRAGMA_ARGS
 *    #pragma ACCRE_DESTRUCTOR_PREPRAGMA_ARGS(destruct_fn)
 *    #endif
 *    ACCRE_DEFINE_DESTRUCTOR(destruct_fn)
 *    #ifdef ACCRE_DESTRUCTOR_POSTPRAGMA_ARGS
 *    #pragma ACCRE_DESTRUCTOR_POSTPRAGMA_ARGS(destruct_fn)
 *    #endif
 *    
 *    static void construct_fn() {  }
 *    
 *    static void destruct_fn() {  }
 * 
 * Unfortunately, implementing this under some compilers requires using
 * #pragma, which cannot be properly nested in a #define. Later C standards
 * declare a _Pragma() token, which can be used properly (it is handled
 * properly by the preprocessor). Support isn't universal, so we must do the
 * #ifdef/#pragma/#endif sequence to properly push/pop pragmas around the
 * declarations.
 *    
 */

#ifndef INCLUDED_ACCRE_TOOLBOX_CONSTRUCTOR_WRAPPER_H
#define INCLUDED_ACCRE_TOOLBOX_CONSTRUCTOR_WRAPPER_H
#include <stdlib.h>

#if  __GNUC__ > 2 || (__GNUC__ == 2 && __GNUC_MINOR__ >= 7) // Building with GCC
#  define ACCRE_DEFINE_CONSTRUCTOR(func_name) \
     static void __attribute__((constructor)) func_name(void);
#  define ACCRE_DEFINE_DESTRUCTOR(func_name) \
     static void __attribute__((destructor)) func_name(void);
#elif defined(_MSC_VER) // Building with MSVC
#  warn "Constructor/Destructor support for MSVC is untested"
#  if(_MSC_VER >= 1500)
#    warn "Constructors/Destructors under VS2015 may behave weird under \n" \
          "Whole-program optimization."
#    ifndef _WIN64
#      warn "Constructors/Destructors may not work under x86 due to mangling"
#    endif
#    define ACCRE_DEFINE_CONSTRUCTOR(func_name) \
       static void func_name(void); \
       int func_name ## _accre_wrapper(void) { func_name(); return 0; } \
	   __pragma(comment(linker,"/include:" func_name "_accre_wrapper")) \
	   __pragma(section(".CRT$XCU",read)) \
	   __declspec(allocate(".CRT$XCU")) \
       static int (* _KEEP ## func_name)(void) = func_name ## _accre_wrapper;

#    define ACCRE_DEFINE_DESTRUCTOR(func_name) \
       static void func_name(void); \
       int func_name ## _accre_wrapper(void) { \
                                                    atexit(func_name); \
                                                    return 0; } \
       __pragma(comment(linker,"/include:" func_name "_accre_wrapper")) \
	   __pragma(section(".CRT$XCU",read)) \
	   __declspec(allocate(".CRT$XCU")) \
       static int (* _KEEP ## func_name)(void) = func_name ## _accre_wrapper;

#  elif(_MSC_VER >= 1400) // MSVC8
#    define ACCRE_DEFINE_CONSTRUCTOR(func_name) \
       static void func_name(void); \
       static int func_name ## _accre_wrapper(void) { func_name(); return 0; } \
       __declspec(allocate(".CRT$XCU")) \
         static int (* _KEEP ## func_name)(void) = func_name ## _accre_wrapper;

#    define ACCRE_DEFINE_DESTRUCTOR(func_name) \
       static void func_name(void); \
       static int func_name ## _accre_wrapper(void) { \
                                                    atexit(func_name); \
                                                    return 0; } \
       __declspec(allocate(".CRT$XPU")) \
         static int (* _KEEP ## func_name)(void) = func_name ## _accre_wrapper;
#    define ACCRE_CONSTRUCTOR_PREPRAGMA_ARGS(func_name) section(".CRT$XCU",read)
#    define ACCRE_DESTRUCTOR_PREPRAGMA_ARGS(func_name) section(".CRT$XPU",read)
#  else // MSVC < MSVC8
#    define ACCRE_DEFINE_CONSTRUCTOR(func_name) \
       static void func_name(void); \
       static int func_name ## _accre_wrapper(void) { func_name(); return 0; } \
       static int (* _KEEP ## func_name)(void) = func_name ## _accre_wrapper;

#    define ACCRE_DEFINE_DESTRUCTOR(func_name) \
       static void func_name(void); \
       static int func_name ## _accre_wrapper(void) { \
                                                    atexit(func_name); \
                                                    return 0; } \
       static int (* _KEEP ## func_name)(void) = func_name ## _accre_wrapper;
#    define ACCRE_CONSTRUCTOR_PREPRAGMA_ARGS(func_name) data_seg(".CRT$XCU")
#    define ACCRE_DESTRUCTOR_PREPRAGMA_ARGS(func_name) data_seg(".CRT$XPU")
#    define ACCRE_CONSTRUCTOR_POSTPRAGMA_ARGS(func_name) data_seg()
#    define ACCRE_DESTRUCTOR_POSTPRAGMA_ARGS(func_name) data_seg()
#  endif // MSVC version selection
#endif // Compiler selection

#ifndef ACCRE_DEFINE_CONSTRUCTOR
#  error "This platform lacks Constructor/Destructor support"
#endif

#endif // Include guard
