SUBDIRS=	src
EXTRA_DIST=	run_tests.pl config/ac_mysql.m4 t/*.t support-files/install.sql support-files/install-exec-hook.txt

BINARY_ARCH=	@BINARY_ARCH@
bindistdir=	$(PACKAGE)-$(VERSION)-$(BINARY_ARCH)

remove_bindistdir = \
  { test ! -d $(bindistdir) \
    || { find $(bindistdir) -type d ! -perm -200 -exec chmod u+w {} ';' \
         && rm -fr $(bindistdir); }; }

install-exec-hook:
	@cat support-files/install-exec-hook.txt

test:
	./run_tests.pl

binary-dist-dir: all distdir
	$(remove_bindistdir)
	mv $(distdir) $(bindistdir)
	cat src/.libs/libqueue_engine.so > $(bindistdir)/libqueue_engine.so

binary-dist: binary-dist-dir
	$(AMTAR) chof - $(bindistdir) | GZIP=$(GZIP_ENV) gzip -c > $(bindistdir).tar.gz
	$(remove_bindistdir)
