SUBDIRS=	src
EXTRA_DIST=	config/ac_mysql.m4

test:
	perl -MTest::Harness -e 'runtests(@ARGV)' t/*.t
