# Makefile for building the Wool test suite.
#
# User configuration and variables defined in $(TOP)/Makefile.inc which
# is shared for building the wool library, examples and tests.

TOP = ..
include $(TOP)/Makefile.inc

CFLAGS += -I$(TOP)/src -I$(TOP)/examples -Icheckfiles
LDLIBS += -lwool -lcheck $(threadsflag) -lm -lrt
LDFLAGS += -L$(TOP)/src

TARGETS = generated/wool-core
OBJS = $(TARGETS:%=%.o)
COBJS = $(OBJS:%.o=%.c)

all: $(TOP)/src/libwool.a $(TARGETS) test

test: $(OBJS)
	$(foreach target,$(TARGETS),./$(target);)

clean :
	$(RM) $(OBJS) $(TARGETS) $(COBJS)

generated/%.c: checkfiles/%.check
	$(TOP)/scripts/checkmk $< > $@


$(TOP)/src/libwool.a: $(TOP)/src/wool.h $(TOP)/src/wool.c $(TOP)/src/wool-common.h
	cd $(TOP) && $(MAKE) -C src libwool.a

$(TOP)/src/wool.h:
	cd $(TOP) && $(MAKE) -C src wool.h

