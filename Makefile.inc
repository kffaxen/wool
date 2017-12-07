CC = gcc
RM = rm -f
AR = ar
SED = sed

ifdef CCOMP
  CC = ${CCOMP}
endif

threadsflag = -lpthread
buildparams =

CFLAGS = -g -O3 -Wall $(buildparams)
MAX_ARITY = 30

ifdef TASK_PAYLOAD
  buildparams += -DTASK_PAYLOAD=$(TASK_PAYLOAD)
endif

ifdef FINEST_GRAIN
  buildparams += -DFINEST_GRAIN=$(FINEST_GRAIN)
endif

ifdef COUNT_EVENTS
  buildparams += -DCOUNT_EVENTS=$(COUNT_EVENTS)
endif

ifdef WOOL_PIE_TIMES
  buildparams += -DWOOL_PIE_TIMES=$(WOOL_PIE_TIMES)
endif

ifdef WOOL_STAT
  buildparams += -DWOOL_STAT=$(WOOL_STAT)
endif

ifdef LOG_EVENTS
  buildparams += -DLOG_EVENTS=$(LOG_EVENTS)
  LDFLAGS += -lrt
endif

ifdef WOOL_MEASURE_SPAN
  buildparams += -DWOOL_MEASURE_SPAN=$(WOOL_MEASURE_SPAN)
  LDFLAGS += -lrt
endif

ifdef WOOL_TRLF
  buildparams += -DWOOL_TRLF=$(WOOL_TRLF)
endif

ifdef WOOL_DEBUG
  buildparams += -DWOOL_DEBUG=$(WOOL_DEBUG)
endif
