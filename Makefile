SO=dll
SHARED=--shared
CFLAGS=-g -Wall
LUAINC=-I/usr/local/include
LUALIB=-L/usr/local/bin -llua54

all : ltask.$(SO)

ltask.$(SO) : src/ltask.c src/queue.c src/sysinfo.c src/service.c src/config.c src/lua-seri.c src/message.c src/systime.c src/timer.c
	$(CC) $(CFLAGS) $(SHARED) $(LUAINC) -Isrc -o $@ $^ $(LUALIB)

clean :
	rm -rf *.$(SO)


