EXTRA_DIST=	*.t
bin_PROGRAMS=	05-multireader-read 05-multirw-core 05-multiwait-core

INCLUDES=	$(MYSQL_INC)
LDFLAGS=	-L$(MYSQL_LIBDIR)
LDADD=		-lmysqlclient

05_multireader_read_SOURCES=	05-multireader-read.c
05_multireader_read_CFLAGS=	$($AM_CFLAGS)

05_multirw_core_SOURCES=	05-multirw-core.c
05_multirw_core_CFLAGS=		$($AM_CFLAGS)

05_multiwait_core_SOURCES=	05-multiwait-core.c
05_multiwait_core_CFLAGS=	$($AM_CFLAGS)
