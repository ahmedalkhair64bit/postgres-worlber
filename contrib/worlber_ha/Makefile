# contrib/worlber_ha/Makefile

MODULE_big = worlber_ha
OBJS = \
	$(WIN32RES) \
	worlber_ha.o

EXTENSION = worlber_ha
DATA = worlber_ha--1.0.sql worlber_ha--1.0--1.1.sql worlber_ha--1.1--1.2.sql
PGFILEDESC = "worlber_ha - availability groups for PostgreSQL managed from SQL"

REGRESS = worlber_ha

TAP_TESTS = 1

# The node agent is a separate Go program; build it with "make agent".
GO ?= go
AGENT = agent/worlber-ha-agent
EXTRA_CLEAN = $(AGENT)

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/worlber_ha
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

.PHONY: agent install-agent agent-check

agent:
	cd $(srcdir)/agent && GOTOOLCHAIN=local $(GO) build -trimpath -o $(abspath $(AGENT)) ./cmd/worlber-ha-agent

agent-check:
	cd $(srcdir)/agent && GOTOOLCHAIN=local $(GO) vet ./... && GOTOOLCHAIN=local $(GO) test ./...

install-agent: agent
	$(INSTALL_PROGRAM) $(AGENT) '$(DESTDIR)$(bindir)/worlber-ha-agent'
