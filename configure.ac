#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.69])
AC_INIT([Millstone - rsync for binary files], [0.1], [hnarkaytis@gmail.com])
AM_INIT_AUTOMAKE([millstone], [1.0])
AC_CONFIG_SRCDIR([src32/millstone.c])
AC_CONFIG_HEADERS([src32/config.h])

# Checks for programs.
AC_PROG_CC

# Checks for libraries.

# Checks for header files.
AC_CHECK_HEADERS([stdlib.h string.h])

# Checks for typedefs, structures, and compiler characteristics.
AC_C_INLINE

PKG_CHECK_MODULES([LIBXML2], [libxml-2.0 >= 2.0], [AC_DEFINE(HAVE_LIBXML2, [], [define if libxml2 is present])] )
AC_CHECK_LIB([z], [compress], [AC_DEFINE(HAVE_ZLIB, [], [define if zlib is present]) LIBS+=-lz ])

AC_ARG_ENABLE(compile-log-level, [  --enable-compile-log-level		set compile time log level], COMPILE_LOG_LEVEL=$enableval, COMPILE_LOG_LEVEL=INFO)
AC_MSG_CHECKING(compile time log level)
AC_MSG_RESULT(${COMPILE_LOG_LEVEL})
if test x${COMPILE_LOG_LEVEL} = xALL; then
  AC_DEFINE_UNQUOTED(COMPILE_LOG_LEVEL_LL_ALL, , [Compile log level])
fi
if test x${COMPILE_LOG_LEVEL} = xTRACE; then
  AC_DEFINE_UNQUOTED(COMPILE_LOG_LEVEL_LL_TRACE, , [Compile log level])
fi
if test x${COMPILE_LOG_LEVEL} = xDEBUG; then
  AC_DEFINE_UNQUOTED(COMPILE_LOG_LEVEL_LL_DEBUG, , [Compile log level])
fi
if test x${COMPILE_LOG_LEVEL} = xINFO; then
  AC_DEFINE_UNQUOTED(COMPILE_LOG_LEVEL_LL_INFO, , [Compile log level])
fi
if test x${COMPILE_LOG_LEVEL} = xWARN; then
  AC_DEFINE_UNQUOTED(COMPILE_LOG_LEVEL_LL_WARN, , [Compile log level])
fi
if test x${COMPILE_LOG_LEVEL} = xERROR; then
  AC_DEFINE_UNQUOTED(COMPILE_LOG_LEVEL_LL_ERROR, , [Compile log level])
fi
if test x${COMPILE_LOG_LEVEL} = xFATAL; then
  AC_DEFINE_UNQUOTED(COMPILE_LOG_LEVEL_LL_FATAL, , [Compile log level])
fi
if test x${COMPILE_LOG_LEVEL} = xOFF; then
  AC_DEFINE_UNQUOTED(COMPILE_LOG_LEVEL_LL_OFF, , [Compile log level])
fi

# Checks for library functions.
AC_FUNC_ALLOCA
AC_FUNC_MALLOC
AC_CHECK_FUNCS([memset])

AC_CONFIG_FILES([Makefile src32/Makefile src64/Makefile])
AC_OUTPUT
