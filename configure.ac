#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.69])
AC_INIT([Millstone - rsync for binary files], [0.1], [hnarkaytis@gmail.com])
AM_INIT_AUTOMAKE([millstone], [1.0])
AC_CONFIG_SRCDIR([src/millstone.c])
AC_CONFIG_HEADERS([config.h])

# Checks for programs.
AC_PROG_CC

# Checks for libraries.

# Checks for header files.
AC_CHECK_HEADERS([stdlib.h string.h])

# Checks for typedefs, structures, and compiler characteristics.
AC_C_INLINE

PKG_CHECK_MODULES([LIBXML2], [libxml-2.0 >= 2.0], [ HAVE_LIBXML2=yes ], [] )
if test x${HAVE_LIBXML2} = xyes ; then
  AC_DEFINE(HAVE_LIBXML2, [], [define if libxml2 is present])
fi

# Checks for library functions.
AC_FUNC_ALLOCA
AC_FUNC_MALLOC
AC_CHECK_FUNCS([memset])

AC_CONFIG_FILES([Makefile src/Makefile])
AC_OUTPUT
