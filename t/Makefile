EXECUTABLES=	05-multireader-read 05-multirw-core 05-multiwait-core
CFLAGS=		-Wall -O2 -g
LDFLAGS=	-lmysqlclient

all: $(EXECUTABLES)

clean:
	rm -f $(EXECUTABLES)

05-multireader-read: 05-multireader-read.c
	$(CC) $(CFLAGS) $? $(LDFLAGS) -o $@

05-multirw-core: 05-multirw-core.c
	$(CC) $(CFLAGS) $? $(LDFLAGS) -o $@

05-multiwait-core: 05-multiwait-core.c
	$(CC) $(CFLAGS) $? $(LDFLAGS) -o $@
