/*-------------------------------------------------------------------------
 *
 * worlber_ha.c
 *		SQL interface to the worlber_ha node agent.
 *
 * The extension contains no high-availability logic itself: failover must
 * keep working when this server is down, so all decisions are taken by
 * worlber-ha-agent processes that run next to every node and agree through
 * Raft.  The functions here send a JSON request to the local agent over a
 * Unix-domain socket and return the agent's JSON reply; the SQL wrappers in
 * worlber_ha--1.0.sql turn that into friendly functions and views.
 *
 * contrib/worlber_ha/worlber_ha.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <poll.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include "fmgr.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "postmaster/postmaster.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/jsonb.h"
#include "utils/timestamp.h"
#include "utils/varlena.h"

PG_MODULE_MAGIC_EXT(
					.name = "worlber_ha",
					.version = PG_VERSION
);

static char *agent_socket = NULL;
static int	agent_timeout = 30;

PG_FUNCTION_INFO_V1(worlber_ha_agent_request);
PG_FUNCTION_INFO_V1(worlber_ha_agent_socket_path);

void
_PG_init(void)
{
	DefineCustomStringVariable("worlber_ha.agent_socket",
							   "Path of the worlber_ha agent's Unix socket.",
							   "Empty means <first unix_socket_directories entry>/.s.WORLBER_HA.<port>.",
							   &agent_socket,
							   "",
							   PGC_SUSET,
							   0,
							   NULL, NULL, NULL);

	DefineCustomIntVariable("worlber_ha.agent_timeout",
							"Default time to wait for an answer from the agent.",
							NULL,
							&agent_timeout,
							30,
							1, 86400,
							PGC_USERSET,
							GUC_UNIT_S,
							NULL, NULL, NULL);

	MarkGUCPrefixReserved("worlber_ha");
}

/*
 * Resolve the agent socket path.
 */
static char *
socket_path(void)
{
	char	   *rawstring;
	List	   *dirs;
	char	   *dir = NULL;

	if (agent_socket && agent_socket[0] != '\0')
		return pstrdup(agent_socket);

	if (Unix_socket_directories && Unix_socket_directories[0] != '\0')
	{
		rawstring = pstrdup(Unix_socket_directories);
		if (SplitDirectoriesString(rawstring, ',', &dirs) && dirs != NIL)
		{
			ListCell   *lc;

			/* skip abstract-namespace sockets ("@name") */
			foreach(lc, dirs)
			{
				char	   *d = (char *) lfirst(lc);

				if (d[0] != '@')
				{
					dir = d;
					break;
				}
			}
		}
	}
	if (dir == NULL)
		dir = DEFAULT_PGSOCKET_DIR;

	return psprintf("%s/.s.WORLBER_HA.%d", dir, PostPortNumber);
}

/*
 * Wait until fd is ready for the given events, honoring interrupts and an
 * absolute deadline (in ms since epoch-ish via GetCurrentTimestamp).
 */
static void
wait_fd(int fd, short events, TimestampTz deadline)
{
	for (;;)
	{
		struct pollfd pfd;
		int			rc;
		long		remaining;

		CHECK_FOR_INTERRUPTS();

		remaining = TimestampDifferenceMilliseconds(GetCurrentTimestamp(), deadline);
		if (remaining <= 0)
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_FAILURE),
					 errmsg("timed out waiting for the worlber_ha agent"),
					 errhint("The operation may still complete in the background; check worlber_ha.cluster_status.")));

		pfd.fd = fd;
		pfd.events = events;
		pfd.revents = 0;
		rc = poll(&pfd, 1, Min(remaining, 200));
		if (rc > 0)
			return;
		if (rc < 0 && errno != EINTR)
			ereport(ERROR,
					(errcode_for_socket_access(),
					 errmsg("could not poll worlber_ha agent socket: %m")));
	}
}

/*
 * Send one HTTP/1.0 request and return the response body.
 */
static char *
agent_call(const char *op, const char *body, int timeout_s)
{
	char	   *path = socket_path();
	struct sockaddr_un addr;
	StringInfoData req;
	StringInfoData resp;
	TimestampTz deadline;
	int			fd;
	char	   *payload;

	if (strlen(path) >= sizeof(addr.sun_path))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("worlber_ha agent socket path \"%s\" is too long", path)));

	memset(&addr, 0, sizeof(addr));
	addr.sun_family = AF_UNIX;
	strlcpy(addr.sun_path, path, sizeof(addr.sun_path));

	fd = socket(AF_UNIX, SOCK_STREAM, 0);
	if (fd < 0)
		ereport(ERROR,
				(errcode_for_socket_access(),
				 errmsg("could not create socket: %m")));

	initStringInfo(&req);
	appendStringInfo(&req,
					 "POST /v1/%s HTTP/1.0\r\n"
					 "Host: worlber-ha-agent\r\n"
					 "Content-Type: application/json\r\n"
					 "Content-Length: %zu\r\n"
					 "\r\n%s",
					 op, strlen(body), body);
	initStringInfo(&resp);

	deadline = TimestampTzPlusSeconds(GetCurrentTimestamp(), timeout_s);

	PG_TRY();
	{
		int			sent = 0;

		if (connect(fd, (struct sockaddr *) &addr, sizeof(addr)) < 0)
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_FAILURE),
					 errmsg("could not connect to the worlber_ha agent at \"%s\": %m", path),
					 errhint("Start worlber-ha-agent on this node, or set worlber_ha.agent_socket.")));

		while (sent < req.len)
		{
			ssize_t		n;

			wait_fd(fd, POLLOUT, deadline);
			n = send(fd, req.data + sent, req.len - sent, 0);
			if (n < 0)
			{
				if (errno == EINTR || errno == EAGAIN)
					continue;
				ereport(ERROR,
						(errcode(ERRCODE_CONNECTION_FAILURE),
						 errmsg("could not send request to the worlber_ha agent: %m")));
			}
			sent += n;
		}

		for (;;)
		{
			char		buf[8192];
			ssize_t		n;

			wait_fd(fd, POLLIN, deadline);
			n = recv(fd, buf, sizeof(buf), 0);
			if (n < 0)
			{
				if (errno == EINTR || errno == EAGAIN)
					continue;
				ereport(ERROR,
						(errcode(ERRCODE_CONNECTION_FAILURE),
						 errmsg("could not read reply from the worlber_ha agent: %m")));
			}
			if (n == 0)
				break;
			appendBinaryStringInfo(&resp, buf, n);
			if (resp.len > 64 * 1024 * 1024)
				ereport(ERROR,
						(errcode(ERRCODE_PROTOCOL_VIOLATION),
						 errmsg("reply from the worlber_ha agent is too large")));
		}
	}
	PG_FINALLY();
	{
		close(fd);
	}
	PG_END_TRY();

	payload = strstr(resp.data, "\r\n\r\n");
	if (payload == NULL || strncmp(resp.data, "HTTP/1.", 7) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("invalid reply from the worlber_ha agent")));
	return payload + 4;
}

/*
 * worlber_ha.agent_request(op text, args jsonb, timeout_s int) RETURNS jsonb
 *
 * Returns the agent's reply envelope: {"ok": bool, "result": ..., "error":
 * ..., "hint": ..., "warnings": [...]}.
 */
Datum
worlber_ha_agent_request(PG_FUNCTION_ARGS)
{
	text	   *op_t;
	char	   *op;
	char	   *args = "{}";
	int			timeout_s = agent_timeout;
	char	   *reply;
	const char *p;

	if (PG_ARGISNULL(0))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("operation must not be null")));
	op_t = PG_GETARG_TEXT_PP(0);
	op = text_to_cstring(op_t);
	for (p = op; *p; p++)
	{
		if (!((*p >= 'a' && *p <= 'z') || *p == '_'))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid worlber_ha operation \"%s\"", op)));
	}
	if (!PG_ARGISNULL(1))
		args = JsonbToCString(NULL, &PG_GETARG_JSONB_P(1)->root, VARSIZE(PG_GETARG_JSONB_P(1)));
	if (!PG_ARGISNULL(2) && PG_GETARG_INT32(2) > 0)
		timeout_s = PG_GETARG_INT32(2);

	reply = agent_call(op, args, timeout_s);

	PG_RETURN_DATUM(DirectFunctionCall1(jsonb_in, CStringGetDatum(reply)));
}

/*
 * worlber_ha.agent_socket_path() RETURNS text
 */
Datum
worlber_ha_agent_socket_path(PG_FUNCTION_ARGS)
{
	PG_RETURN_TEXT_P(cstring_to_text(socket_path()));
}
