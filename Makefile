all: build

build: nanomsg_estp.so

nanomsg_estp.so: nanomsg_estp.c
	gcc nanomsg_estp.c -o nanomsg_estp.so -shared -fPIC -lnanomsg -Wall $(CFLAGS) $(LDFLAGS)

clean:
	-rm -rf collectd
	-rm nanomsg_estp.so

install: build
	install -d $(DESTDIR)/usr/lib/collectd
	install nanomsg_estp.so $(DESTDIR)/usr/lib/collectd

