/* contrib/worlber_ha/worlber_ha--1.1--1.2.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION worlber_ha UPDATE TO '1.2'" to load this file. \quit

--
-- Re-initialize a standby: wipe its data directory and seed it again from
-- the primary (pg_basebackup).  Needed when a standby was down so long that
-- the primary no longer has the WAL it needs; with auto_reinit = true (the
-- default) the agent does this by itself.
--
CREATE FUNCTION reinit_node(node text)
RETURNS jsonb
LANGUAGE sql VOLATILE
AS $$ SELECT @extschema@.call('reinit_node', jsonb_build_object('name', node)); $$;

--
-- Prove that the replicas hold exactly the same data as the primary:
-- checksum of a table on the primary, then on every standby once it has
-- replayed up to the primary's position.  Reads the whole table on every
-- node, so use it on quiet tables or during maintenance windows.
--
CREATE FUNCTION verify_table(table_name text, dbname text DEFAULT current_database())
RETURNS TABLE (node text, role text, lsn pg_lsn, row_count bigint, checksum text, matches_primary boolean, error text)
LANGUAGE sql VOLATILE
AS $$
	SELECT f.node, f.role, nullif(f.lsn, '')::pg_lsn, f.rows, nullif(f.md5, ''), f.matches_primary, f.error
	FROM jsonb_to_recordset(@extschema@.call('verify_table',
			jsonb_build_object('table', table_name, 'dbname', dbname), 900))
		 AS f(node text, role text, lsn text, rows bigint, md5 text, matches_primary boolean, error text);
$$;

--
-- replication: add how much data would be lost if each standby were
-- promoted right now (like "estimated data loss" in the AG dashboard).
--
DROP VIEW replication;
CREATE VIEW replication AS
	SELECT r.node, r.client_addr, r.state, r.sync_state,
		   CASE WHEN r.sync_state IN ('sync', 'quorum') THEN 'none (synchronous)'
				WHEN r.state <> 'streaming' THEN 'unknown (not streaming)'
				ELSE pg_size_pretty(r.send_queue_bytes) || ' of WAL' END AS data_loss_if_failover_now,
		   r.primary_lsn::pg_lsn AS primary_lsn,
		   r.sent_lsn::pg_lsn AS sent_lsn,
		   r.write_lsn::pg_lsn AS write_lsn,
		   r.flush_lsn::pg_lsn AS flush_lsn,
		   r.replay_lsn::pg_lsn AS replay_lsn,
		   pg_size_pretty(r.send_queue_bytes) AS log_send_queue,
		   pg_size_pretty(r.redo_queue_bytes) AS redo_queue,
		   r.send_queue_bytes, r.redo_queue_bytes, r.total_lag_bytes,
		   make_interval(secs => r.write_lag_s) AS write_lag,
		   make_interval(secs => r.flush_lag_s) AS flush_lag,
		   make_interval(secs => r.replay_lag_s) AS replay_lag
	FROM jsonb_to_recordset(@extschema@.replication_raw())
		 AS r(node text, client_addr text, state text, sync_state text,
			  primary_lsn text, sent_lsn text, write_lsn text, flush_lsn text, replay_lsn text,
			  send_queue_bytes bigint, redo_queue_bytes bigint, total_lag_bytes bigint,
			  write_lag_s float8, flush_lag_s float8, replay_lag_s float8);

REVOKE ALL ON FUNCTION reinit_node(text), verify_table(text, text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION reinit_node(text), verify_table(text, text) TO worlber_ha_admin;
REVOKE ALL ON replication FROM PUBLIC;
GRANT SELECT ON replication TO worlber_ha_admin, pg_monitor;

-- health_check: the primary now always lists every standby in
-- synchronous_standby_names as "FIRST 1 (sync-marked..., others...)", so
-- exactly one standby is synchronous.  Report when none is (commits are on
-- one server only) and when a fallback standby, not one marked sync, holds
-- that role.
CREATE OR REPLACE FUNCTION health_check()
RETURNS TABLE (severity text, node text, problem text)
LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
	st jsonb;
	n jsonb;
	cfg jsonb;
	members int := 0;
	streaming int := 0;
	marked_replicas int := 0;
	sync_node text;
	sync_marked boolean;
	found_problem boolean := false;
	maxlag bigint;
BEGIN
	st := @extschema@.call('status');
	cfg := @extschema@.call('config');
	maxlag := CASE WHEN cfg->>'maximum_lag_on_failover' ~ '^\d+$' THEN (cfg->>'maximum_lag_on_failover')::bigint
				   ELSE pg_size_bytes(cfg->>'maximum_lag_on_failover') END;

	IF coalesce(st->>'leader', '') = '' THEN
		severity := 'CRITICAL'; node := NULL; problem := 'no Raft leader: the cluster has no quorum, failover is impossible';
		found_problem := true; RETURN NEXT;
	END IF;
	IF (st->>'paused')::boolean THEN
		severity := 'WARNING'; node := NULL; problem := 'cluster is paused: automatic failover is disabled';
		found_problem := true; RETURN NEXT;
	END IF;

	FOR n IN SELECT * FROM jsonb_array_elements(st->'nodes') LOOP
		members := members + 1;
		node := n->>'node';
		IF n->>'role' = 'primary' AND n->>'state' <> 'running' THEN
			severity := 'CRITICAL'; problem := 'primary is ' || (n->>'state');
			found_problem := true; RETURN NEXT;
		ELSIF n->>'role' = 'replica' AND n->>'state' <> 'streaming' THEN
			severity := CASE WHEN (n->>'sync')::boolean THEN 'CRITICAL' ELSE 'WARNING' END;
			problem := 'replica is ' || (n->>'state') || coalesce(' (' || nullif(n->>'agent_status', '') || ')', '');
			found_problem := true; RETURN NEXT;
		ELSIF n->>'role' = 'witness' AND n->>'state' <> 'witness' THEN
			severity := 'WARNING'; problem := 'witness is ' || (n->>'state');
			found_problem := true; RETURN NEXT;
		END IF;
		IF n->>'role' = 'replica' THEN
			IF (n->>'sync')::boolean THEN
				marked_replicas := marked_replicas + 1;
			END IF;
			IF n->>'state' = 'streaming' THEN
				streaming := streaming + 1;
			END IF;
			IF n->>'sync_state' IN ('sync', 'quorum') THEN
				sync_node := n->>'node';
				sync_marked := (n->>'sync')::boolean;
			END IF;
		END IF;
		IF n->>'role' = 'primary' AND coalesce((n->'archive'->>'failing')::boolean, false) THEN
			severity := 'CRITICAL';
			problem := 'WAL archiving is failing (' || coalesce(n->'archive'->>'failed_count', '?') ||
					   ' failures): pg_wal keeps growing until it succeeds; check archive_command and the archive directory';
			found_problem := true; RETURN NEXT;
		END IF;
		IF n->>'role' = 'primary' AND n->>'fence_state' = 'inactive' THEN
			severity := 'WARNING';
			problem := 'fencing is not active: add worlber_ha to shared_preload_libraries and restart PostgreSQL';
			found_problem := true; RETURN NEXT;
		ELSIF n->>'role' = 'primary' AND n->>'fence_state' = 'gate_closed' THEN
			severity := 'WARNING';
			problem := 'fencing waits for every node to run the current agent (rolling upgrade in progress)';
			found_problem := true; RETURN NEXT;
		END IF;
		IF n->>'role' = 'replica' AND (n->>'lag_bytes')::bigint > maxlag THEN
			severity := 'WARNING';
			problem := 'replica lags ' || pg_size_pretty((n->>'lag_bytes')::bigint) || ', more than maximum_lag_on_failover';
			found_problem := true; RETURN NEXT;
		END IF;
	END LOOP;

	IF streaming > 0 AND sync_node IS NULL THEN
		severity := 'CRITICAL'; node := NULL;
		problem := 'no synchronous standby: commits are acknowledged by the primary alone and can be lost on failover';
		found_problem := true; RETURN NEXT;
	ELSIF sync_node IS NOT NULL AND NOT sync_marked THEN
		severity := 'WARNING'; node := sync_node;
		problem := CASE WHEN marked_replicas = 0
			THEN 'synchronous standby is a fallback: no replica is marked sync (the marked node may be the primary); see worlber_ha.set_sync()'
			ELSE 'synchronous standby is a fallback: the replicas marked sync are not streaming' END;
		found_problem := true; RETURN NEXT;
	END IF;
	IF members < 3 THEN
		severity := 'WARNING'; node := NULL;
		problem := members || ' member(s): at least 3 agents (or 2 + witness) are needed to survive a node failure';
		found_problem := true; RETURN NEXT;
	END IF;
	IF NOT found_problem THEN
		severity := 'OK'; node := NULL; problem := 'cluster healthy';
		RETURN NEXT;
	END IF;
END
$$;

-- join_token: issue a one-time token so a new node can join with a single
-- command (worlber-ha-agent init --join <token>) instead of copying the
-- cluster secret by hand.  Only the nonce hash is stored in the cluster
-- state; the token is single-use and expires.
CREATE FUNCTION join_token(ttl interval DEFAULT '1 hour', sync boolean DEFAULT false)
RETURNS text
LANGUAGE sql VOLATILE
AS $$
	SELECT (@extschema@.call('join_token',
		jsonb_build_object('ttl_seconds', extract(epoch FROM ttl)::int, 'sync', sync)))->>'token';
$$;

REVOKE ALL ON FUNCTION join_token(interval, boolean) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION join_token(interval, boolean) TO worlber_ha_admin;

-- cluster_status: add fencing and archiving state per node.
DROP VIEW cluster_status;
CREATE VIEW cluster_status AS
	SELECT n.node, n.host, n.port, n.role, n.state, n.sync, nullif(n.sync_state, '') AS sync_state,
		   n.timeline, n.lsn::pg_lsn AS lsn, n.replay_lsn::pg_lsn AS replay_lsn,
		   n.lag_bytes, n.raft_role,
		   CASE WHEN n.last_seen_ms IS NOT NULL
				THEN make_interval(secs => n.last_seen_ms / 1000.0) END AS last_seen,
		   nullif(n.agent_status, '') AS agent_status,
		   nullif(n.fence_state, '') AS fence_state,
		   round(n.lease_age_s::numeric, 1) AS lease_age_s,
		   (n.archive->>'failing')::boolean AS archive_failing,
		   CASE WHEN n.archive->>'last_archived_s' IS NOT NULL
				THEN make_interval(secs => (n.archive->>'last_archived_s')::float8) END AS last_archived
	FROM jsonb_to_recordset(@extschema@.status()->'nodes')
		 AS n(node text, host text, port int, role text, state text, sync boolean,
			  sync_state text, timeline int, lsn text, replay_lsn text, lag_bytes bigint,
			  raft_role text, last_seen_ms bigint, agent_status text,
			  fence_state text, lease_age_s float8, archive jsonb);
GRANT SELECT ON cluster_status TO worlber_ha_admin, pg_monitor;

--
-- Live proof: drill() and the proof view.
--
-- worlber_ha.drill() runs a planned switchover while measuring it.  Just
-- before, the primary writes a row into worlber_ha_internal.canary; after,
-- the drill checks that row on the new primary (no acknowledged write lost).
-- The receipt is a drill_completed / drill_failed event with JSON detail.
--
CREATE SCHEMA worlber_ha_internal;
CREATE TABLE worlber_ha_internal.canary (
	id bigserial PRIMARY KEY,
	written_at timestamptz NOT NULL DEFAULT now()
);
REVOKE ALL ON SCHEMA worlber_ha_internal FROM PUBLIC;

CREATE FUNCTION drill(kind text DEFAULT 'switchover')
RETURNS jsonb
LANGUAGE sql VOLATILE
AS $$
	SELECT @extschema@.call('drill', jsonb_build_object('kind', kind), 900);
$$;
REVOKE ALL ON FUNCTION drill(text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION drill(text) TO worlber_ha_admin;

CREATE VIEW proof AS
	WITH d AS (
		SELECT seq, event_time, kind, detail::jsonb AS j
		FROM @extschema@.events
		WHERE kind IN ('drill_completed', 'drill_failed')
	), last AS (
		SELECT * FROM d ORDER BY seq DESC LIMIT 1
	), last_fail AS (
		SELECT max(seq) AS seq FROM d WHERE kind = 'drill_failed'
	), af AS (
		SELECT event_time, detail FROM @extschema@.events
		WHERE kind = 'automatic_failover' ORDER BY seq DESC LIMIT 1
	)
	SELECT
		(SELECT max(event_time) FROM d WHERE kind = 'drill_completed') AS last_proven_at,
		last.kind = 'drill_completed' AS last_drill_passed,
		(last.j->>'rto_ms')::int AS last_drill_rto_ms,
		last.j->>'old_primary' AS last_drill_from,
		last.j->>'new_primary' AS last_drill_to,
		(last.j->>'canary_present')::boolean AS last_drill_canary_present,
		last.j->>'error' AS last_drill_error,
		(SELECT count(*) FROM d WHERE kind = 'drill_completed'
			AND seq > coalesce((SELECT seq FROM last_fail), 0)) AS drill_pass_streak,
		af.event_time AS last_automatic_failover_at,
		af.detail AS last_automatic_failover
	FROM (SELECT 1) one
	LEFT JOIN last ON true
	LEFT JOIN af ON true;
GRANT SELECT ON proof TO worlber_ha_admin, pg_monitor;
