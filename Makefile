all: build

build: nanomsg_estp.so

nanomsg_estp.so: nanomsg_estp.c collectd/src/config.h
	gcc nanomsg_estp.c -o nanomsg_estp.so -shared -fPIC -lnanomsg -Icollectd/src -DHAVE_CONFIG_H -Wall $(CFLAGS) $(LDFLAGS)

collectd/configure:
	version=$$(collectd -h | grep -Po 'collectd \K\d+\.\d+\.\d+') && \
	curl http://collectd.org/files/collectd-$$version.tar.gz > collectd-$$version.tar.gz && \
	tar -xzf collectd-$$version.tar.gz && \
	mv collectd-$$version collectd

collectd/src/config.h: collectd/configure
	cd collectd; ./configure $(CONFIGURE_ARGS)

clean:
	-rm nanomsg_estp.so

install: build
	install -d $(DESTDIR)/usr/lib/collectd
	install nanomsg_estp.so $(DESTDIR)/usr/lib/collectd

