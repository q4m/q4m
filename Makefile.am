SUBDIRS=	src
EXTRA_DIST=	config/ac_mysql.m4 t/*.t support_files/install.sql support_files/install-exec-hook.txt

install-exec-hook:
	@cat support-files/install-exec-hook.txt

test:
	perl -MTest::Harness -e 'runtests(@ARGV)' t/*.t
