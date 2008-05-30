INCLUDES=			$(MYSQL_INC)
lib_LTLIBRARIES=		libqueue_engine.la
noinst_HEADER=			ha_queue.h queue_cond.h dllist.h
noinst_LIBRARIES=		libqueue_cond.a
EXTRA_DIST=			adler32.c $(noinst_HEADER)

libqueue_cond_a_SOURCES=	queue_cond.cc
libqueue_cond_a_CFLAGS=		$(AM_CFLAGS) -fPIC
libqueue_cond_a_CXXFLAGS=	$(AM_CFLAGS) -fPIC

libqueue_engine_la_SOURCES=	ha_queue.cc
libqueue_engine_la_LIBADD=	queue_cond.o
libqueue_engine_la_LDFLAGS=	-module
libqueue_engine_la_CFLAGS=	$(AM_CFLAGS) -DMYSQL_DYNAMIC_PLUGIN
libqueue_engine_la_CXXFLAGS=	$(AM_CFLAGS) -DMYSQL_DYNAMIC_PLUGIN -fno-rtti -fno-exceptions
