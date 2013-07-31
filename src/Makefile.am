# -*- makefile -*-

bin_PROGRAMS = millstone
millstone_SOURCES = millstone.c

AM_CFLAGS = -O2 -Wall -Werror -g --fast-math $(LIBXML2_CFLAGS)
AM_LDFLAGS = -rdynamic
INCLUDE = -I../../Metaresc/src -I$(top_srcdir)/src
LIBS += -L../../Metaresc/src/.libs -lmetaresc -ldl $(LIBXML2_LIBS)

static:
	$(MAKE) clean-binPROGRAMS
	$(MAKE) LDFLAGS="-s --static"