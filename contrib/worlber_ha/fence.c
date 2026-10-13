/*-------------------------------------------------------------------------
 *
 * fence.c
 *		Fence worker: stop a primary whose agent stopped renewing its lease.
 *
 * The node agent renews $PGDATA/worlber_ha.lease about once a second, but
 * only while it can confirm with the cluster (Raft) that this node is still
 * the primary.  If the agent dies, hangs, or loses contact with the cluster,
 * the lease goes stale.  This worker runs inside PostgreSQL, so it keeps
 * working when the agent is dead, and shuts the server down (fast shutdown)
 * once the lease is older than its TTL.  The cluster promotes a standby only
 * after the old primary's lease is guaranteed to have expired, so two
 * primaries never accept writes at the same time.
 *
 * Lease file: one line "v1 <boot_id> <boottime_ns> <ttl_ms> <term> <epoch>".
 * boottime_ns is CLOCK_BOOTTIME when the agent last confirmed it is the
 * primary; CLOCK_BOOTTIME is not changed by NTP or by hand and keeps
 * counting while the machine or container is suspended.
 *
 * The worker only acts when worlber_ha.fence = on (the agent turns it on
 * after writing the first lease) and never on a standby.  It needs
 * worlber_ha in shared_preload_libraries.
 *
 * contrib/worlber_ha/fence.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <signal.h>
#include <time.h>
#include <unistd.h>

#include "access/xlog.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/latch.h"
#include "tcop/tcopprot.h"
#include "utils/guc.h"
#include "utils/wait_event.h"

#include "worlber_ha.h"

#define LEASE_FILE "worlber_ha.lease"
#define CHECK_INTERVAL_MS 500

bool		worlber_ha_fence = false;
int			worlber_ha_lease_ttl = 10000;	/* ms; used when there is no lease
											 * file yet */

PGDLLEXPORT void worlber_ha_fence_main(Datum main_arg);

static int64
boottime_ns(void)
{
	struct timespec ts;

	if (clock_gettime(CLOCK_BOOTTIME, &ts) != 0)
		return -1;
	return (int64) ts.tv_sec * INT64CONST(1000000000) + ts.tv_nsec;
}

static bool
read_boot_id(char *buf, size_t len)
{
	FILE	   *f = fopen("/proc/sys/kernel/random/boot_id", "r");
	bool		ok;

	if (f == NULL)
		return false;
	ok = fgets(buf, len, f) != NULL;
	fclose(f);
	if (ok)
		buf[strcspn(buf, "\r\n")] = '\0';
	return ok && buf[0] != '\0';
}

/*
 * Decide whether the lease is valid.  Returns true when PostgreSQL may keep
 * running as primary; otherwise fills "why".
 */
static bool
lease_valid(int64 now, int64 started, const char *boot_id, char *why, size_t whylen)
{
	char		path[MAXPGPATH];
	FILE	   *f;
	char		line[256];
	char		ver[8];
	char		lboot[64];
	long long	ts;
	long long	ttl_ms;
	unsigned long long term;
	unsigned long long epoch;

	snprintf(path, sizeof(path), "%s/%s", DataDir, LEASE_FILE);
	f = fopen(path, "r");
	if (f == NULL)
	{
		/* no lease yet: allow lease_ttl after this server started */
		if (now - started <= (int64) worlber_ha_lease_ttl * 1000000)
			return true;
		snprintf(why, whylen, "no lease file %s after %d ms", LEASE_FILE, worlber_ha_lease_ttl);
		return false;
	}
	if (fgets(line, sizeof(line), f) == NULL)
		line[0] = '\0';
	fclose(f);

	if (sscanf(line, "%7s %63s %lld %lld %llu %llu", ver, lboot, &ts, &ttl_ms, &term, &epoch) != 6 ||
		strcmp(ver, "v1") != 0 || ttl_ms <= 0)
	{
		snprintf(why, whylen, "lease file is not valid");
		return false;
	}
	if (strcmp(lboot, boot_id) != 0)
	{
		snprintf(why, whylen, "lease was written during another boot");
		return false;
	}
	if (ts > now)
	{
		snprintf(why, whylen, "lease timestamp is in the future");
		return false;
	}
	if (now - ts > (int64) ttl_ms * 1000000)
	{
		snprintf(why, whylen, "lease expired %lld ms ago (epoch %llu)",
				 (long long) ((now - ts) / 1000000 - ttl_ms), epoch);
		return false;
	}
	return true;
}

void
worlber_ha_fence_main(Datum main_arg)
{
	char		boot_id[64];
	int64		started;
	bool		fenced = false;
	uint32		wait_event;

	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGTERM, die);
	BackgroundWorkerUnblockSignals();

	if (!read_boot_id(boot_id, sizeof(boot_id)))
		ereport(FATAL,
				(errmsg("worlber_ha fence: cannot read the boot id")));
	started = boottime_ns();
	if (started < 0)
		ereport(FATAL,
				(errmsg("worlber_ha fence: CLOCK_BOOTTIME is not available")));
	wait_event = WaitEventExtensionNew("WorlberHaFence");

	ereport(LOG,
			(errmsg("worlber_ha fence worker started (fence = %s)",
					worlber_ha_fence ? "on" : "off")));

	for (;;)
	{
		char		why[160];
		int64		now;

		(void) WaitLatch(MyLatch,
						 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 CHECK_INTERVAL_MS, wait_event);
		ResetLatch(MyLatch);
		CHECK_FOR_INTERRUPTS();

		if (ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
		}
		if (!worlber_ha_fence || fenced || RecoveryInProgress())
			continue;

		now = boottime_ns();
		if (lease_valid(now, started, boot_id, why, sizeof(why)))
			continue;

		/*
		 * Fast shutdown: new connections are refused and running
		 * transactions are aborted, so nothing more can commit here.  The
		 * agent's fencing goroutine escalates to SIGKILL if the shutdown
		 * hangs.
		 */
		ereport(LOG,
				(errmsg("worlber_ha fence: stopping this primary: %s", why),
				 errhint("The worlber_ha agent did not confirm that this node is still the primary.")));
		if (kill(PostmasterPid, SIGINT) != 0)
			ereport(WARNING,
					(errmsg("worlber_ha fence: could not signal the postmaster: %m")));
		fenced = true;
	}
}

/* Register the worker; called from _PG_init while preloading. */
void
worlber_ha_register_fence(void)
{
	BackgroundWorker worker;

	memset(&worker, 0, sizeof(worker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS;
	worker.bgw_start_time = BgWorkerStart_PostmasterStart;
	worker.bgw_restart_time = 1;
	snprintf(worker.bgw_library_name, MAXPGPATH, "worlber_ha");
	snprintf(worker.bgw_function_name, BGW_MAXLEN, "worlber_ha_fence_main");
	snprintf(worker.bgw_name, BGW_MAXLEN, "worlber_ha fence");
	snprintf(worker.bgw_type, BGW_MAXLEN, "worlber_ha fence");
	RegisterBackgroundWorker(&worker);
}
