SO=dll
SHARED=--shared
CFLAGS=-g -Wall
LUAINC=-I/usr/local/include
LUALIB=-L/usr/local/bin -llua54

ifeq ($(OS),Windows_NT)
  WINLIB=-lwinmm
endif

all : ltask.$(SO)

SRCS=\
 src/ltask.c \
 src/queue.c \
 src/sysinfo.c \
 src/service.c \
 src/config.c \
 src/lua-seri.c \
 src/message.c \
 src/systime.c \
 src/timer.c \
 src/sysapi.c

ltask.$(SO) : $(SRCS)
	$(CC) $(CFLAGS) $(SHARED) $(LUAINC) -Isrc -o $@ $^ $(LUALIB) $(WINLIB)

clean :
	rm -rf *.$(SO)


