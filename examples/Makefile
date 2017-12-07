# Makefile for building the Wool examples.
#
# User configuration and variables defined in $(TOP)/Makefile.inc which
# is shared for building the wool library, examples and tests.

TOP = ..
include $(TOP)/Makefile.inc

CFLAGS += -I$(TOP)/src
LDLIBS += $(TOP)/src/wool-main.o -lwool $(threadsflag)
LDFLAGS += -L$(TOP)/src

# Remove inclusion of wool-main.o from LDLIBS.
LDLIBS_OMP = `echo "$(LDLIBS)" | $(SED) "s\#$(TOP)/src/wool-main.o\#\#g"`

# Keep fib alone on the last line of TARGETS to avoid merge conflicts
# with other branches.
TARGETS = stress loop2 mm1 mm2 mm3 mm4 memstress mm5 mm6 mm7 skew \
          cholesky_r_opt multisort nqueens nqueens-syncall \
          fib seqfib dynamic-loop

ifdef WOOL_OPENMP
  TARGETS += fib-omp
endif

OBJS = $(TARGETS:%=%.o)

EXTRA_OBJS = loop.o reads.o getoptions.o

all: $(TOP)/src/libwool.a $(TOP)/src/wool-main.o $(TARGETS)

clean :
	$(RM) $(OBJS) $(EXTRA_OBJS) $(TARGETS)

nqueens-syncall.o:
	$(CC) $(CFLAGS) -DUSE_SYNC_ALL nqueens.c -c -o nqueens-syncall.o

fib-omp: fib-omp.o
	$(CC) $(LDFLAGS) -o fib-omp fib-omp.o $(LDLIBS_OMP)

seqfib: LDLIBS=
dynamic-loop: CFLAGS+= -std=gnu99

stress: stress.o loop.o
loop2: loop2.o loop.o
skew: skew.o loop.o
memstress: memstress.o reads.o
cholesky_r_opt: cholesky_r_opt.o getoptions.o
cholesky_r_opt: LDLIBS += -lm
fib-omp.o: CFLAGS += -fopenmp
dynamic-loop: dynamic-loop.o loop.o

$(TOP)/src/libwool.a: $(TOP)/src/wool.h
	cd $(TOP) && $(MAKE) -C src libwool.a

$(TOP)/src/wool.h:
	cd $(TOP) && $(MAKE) -C src wool.h

$(TOP)/src/wool-main.o: $(TOP)/src/wool.h
	cd $(TOP) && $(MAKE) -C src wool-main.o

