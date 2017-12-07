# Top level Makefile.
#
# User configuration and variables defined in Makefile.inc which
# is shared for building the wool library, examples and tests.

TOP = .
include $(TOP)/Makefile.inc

SUBDIRS=src examples test
CLEANDIRS=$(SUBDIRS:%=clean-%)

.PHONY: subdirs $(SUBDIRS)
.PHONY: cleandirs $(CLEANDIRS)

subdirs: $(SUBDIRS)

$(SUBDIRS):
	$(MAKE) -C $@

clean: $(CLEANDIRS)
$(CLEANDIRS):
	$(MAKE) -C $(@:clean-%=%) clean

examples: src

