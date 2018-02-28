# Process this file with autoconf to produce a configure script.
AC_PREREQ([2.69])
AC_INIT([btree], [1.0], [git@schenk.biz])
AM_INIT_AUTOMAKE
AC_CONFIG_SRCDIR([src/memory.c])
AC_CONFIG_HEADERS([config.h])
AC_CONFIG_MACRO_DIR([m4])

# Checks for programs.
AC_PROG_CC

# Checks for libraries.
LT_INIT
#AC_ENABLE_SHARED
#AC_DISABLE_STATIC
AC_ENABLE_STATIC
AC_PROG_LIBTOOL(libtool)

# Checks for typedefs, structures, and compiler characteristics.
AC_HEADER_STDBOOL
AC_C_INLINE
AC_TYPE_SIZE_T

# Output Makefile files.
#AC_CONFIG_FILES([Makefile src/Makefile include/Makefile])
AC_CONFIG_FILES([Makefile src/Makefile include/Makefile])
AC_OUTPUT


