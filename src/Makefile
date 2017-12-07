# Makefile for building the Wool library.
#
# User configuration and variables defined in $(TOP)/Makefile.inc which
# is shared for building the wool library, examples and tests.

TOP = ..
include $(TOP)/Makefile.inc

OBJS = wool.o
TARGET=libwool.a
EXTRA_OBJS=wool-main.o
CFLAGS += -W -Wno-unused-parameter

all: $(TARGET) $(EXTRA_OBJS)

wool.h: wool-gen.sh
	./wool-gen.sh $(MAX_ARITY) > wool.h

$(TARGET): $(OBJS)
	$(AR) rcs $@ $^

clean :
	$(RM) wool.h $(OBJS) $(EXTRA_OBJS) $(TARGET)



wool.o: wool.h wool-common.h
wool-main.o: wool.h wool-common.h

