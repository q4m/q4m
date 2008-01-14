SUBDIRS=	src
EXTRA_DIST=	run_tests.pl config/ac_mysql.m4 t/*.t support-files/install.sql support-files/install-exec-hook.txt
DOC_ROOT=	@DOC_ROOT@

BINARY_ARCH=	@BINARY_ARCH@
bindistdir=	$(PACKAGE)-$(VERSION)-$(BINARY_ARCH)

remove_bindistdir = \
  { test ! -d $(bindistdir) \
    || { find $(bindistdir) -type d ! -perm -200 -exec chmod u+w {} ';' \
         && rm -fr $(bindistdir); }; }

install-exec-hook:
	@cat support-files/install-exec-hook.txt

dist-doc:
	rm -rf $(distdir)/doc doc.tmp
	mkdir -p $(distdir)/doc doc.tmp
	(cd doc.tmp && wget -r $(DOC_ROOT))
	find doc.tmp -type f -exec mv {} $(distdir)/doc \;
	for i in $(distdir)/doc/*.php ; do \
	    mv $$i `echo $$i | perl -pe 's/\.php$$/.html/'` ; \
	done
	perl -i.bak -pe 's/href="([a-z0-9_]*)\.php"/href="$$1.html"/g' $(distdir)/doc/*.html
	perl -i.bak -pe 's|href="\./"|href="index.html"|g' $(distdir)/doc/*.html
	rm -f $(distdir)/doc/*.bak
	(cd $(distdir)/doc && wget $(DOC_ROOT)/top.jpg)
	rm -rf doc.tmp

dist-hook: dist-doc

test:
	./run_tests.pl

binary-dist-dir: all distdir
	$(remove_bindistdir)
	mv $(distdir) $(bindistdir)
	cat src/.libs/libqueue_engine.so > $(bindistdir)/libqueue_engine.so

binary-dist: binary-dist-dir
	$(AMTAR) chof - $(bindistdir) | GZIP=$(GZIP_ENV) gzip -c > $(bindistdir).tar.gz
	$(remove_bindistdir)
