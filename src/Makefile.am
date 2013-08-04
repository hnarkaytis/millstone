# -*- makefile -*-

bin_PROGRAMS = millstone
millstone_SOURCES = millstone.c logging.c msg.c file_meta.c client.c queue.c

AM_CFLAGS = -O2 -Wall -Werror -g --fast-math $(LIBXML2_CFLAGS)
AM_LDFLAGS = -rdynamic
INCLUDES = -I../../Metaresc/src -I$(top_srcdir)/src
LIBS += ../../Metaresc/src/.libs/libmetaresc.a -ldl $(LIBXML2_LIBS) -lpthread -lcrypto

static:
	$(MAKE) clean-binPROGRAMS
	$(MAKE) LDFLAGS="-s --static"