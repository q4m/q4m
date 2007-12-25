EXTRA_DIST=	ha_queue.h
INCLUDES=	$(MYSQL_INC)
noinst_HEADER=	ha_queue.h

lib_LTLIBRARIES=	libqueue_engine.la
libqueue_engine_la_SOURCES=	ha_queue.cc
libqueue_engine_la_LIBADD=

libqueue_engine_la_LDFLAGS=	-module
libqueue_engine_la_CFLAGS=	$(AM_CFLAGS) -DMYSQL_DYNAMIC_PLUGIN
libqueue_engine_la_CXXFLAGS=	$(AM_CFLAGS) -DMYSQL_DYNAMIC_PLUGIN
