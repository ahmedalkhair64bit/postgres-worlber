/* contrib/worlber_ha/worlber_ha--1.0--1.1.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION worlber_ha UPDATE TO '1.1'" to load this file. \quit

--
-- Replication progress of every standby, seen from the primary
-- (like sys.dm_hadr_database_replica_states).  Works on any node.
--
CREATE FUNCTION replication_raw()
RETURNS jsonb
LANGUAGE sql VOLATILE SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$ SELECT @extschema@.call('replication'); $$;

CREATE VIEW replication AS
	SELECT r.node, r.client_addr, r.state, r.sync_state,
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

--
-- Redo (WAL replay) state of the standby you are connected to.
--
CREATE VIEW redo AS
	SELECT pg_is_in_recovery() AS is_standby,
		   r.status AS receiver_status,
		   r.sender_host AS primary_host,
		   pg_last_wal_receive_lsn() AS received_lsn,
		   pg_last_wal_replay_lsn() AS replayed_lsn,
		   pg_size_pretty(pg_wal_lsn_diff(pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn())) AS redo_queue,
		   pg_wal_lsn_diff(pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn()) AS redo_queue_bytes,
		   pg_last_xact_replay_timestamp() AS last_replayed_commit,
		   CASE WHEN pg_is_in_recovery() THEN now() - pg_last_xact_replay_timestamp() END AS replay_delay,
		   CASE WHEN pg_is_in_recovery() THEN pg_get_wal_replay_pause_state() END AS replay_pause_state,
		   r.last_msg_receipt_time AS last_message_from_primary
	FROM (SELECT 1) dummy
	LEFT JOIN pg_stat_wal_receiver r ON true;

--
-- Sessions that are blocked on this node: commits waiting for a synchronous
-- replica, lock waits, and WAL replay (redo) blocked by queries on a standby.
--
CREATE VIEW blocking AS
	SELECT a.pid, a.backend_type, a.usename, a.datname, a.application_name, a.client_addr,
		   CASE
			   WHEN a.wait_event = 'SyncRep' THEN 'commit waiting for synchronous replica'
			   WHEN a.backend_type = 'startup' AND a.wait_event = 'RecoveryPause' THEN 'redo paused (pg_wal_replay_pause)'
			   WHEN a.backend_type = 'startup' THEN 'redo blocked by queries on this standby'
			   WHEN a.wait_event_type = 'Lock' THEN 'lock wait (' || a.wait_event || ')'
			   ELSE a.wait_event_type || ': ' || a.wait_event
		   END AS reason,
		   pg_blocking_pids(a.pid) AS blocked_by,
		   now() - coalesce(a.xact_start, a.query_start, a.backend_start) AS waiting_for,
		   a.wait_event_type, a.wait_event,
		   left(a.query, 300) AS query
	FROM pg_stat_activity a
	WHERE a.pid <> pg_backend_pid()
	  AND (a.wait_event = 'SyncRep'
		   OR (a.backend_type = 'startup'
			   AND (a.wait_event_type IN ('Lock', 'Buffer')
					OR a.wait_event IN ('RecoveryConflictSnapshot', 'RecoveryConflictTablespace', 'RecoveryPause')))
		   OR cardinality(pg_blocking_pids(a.pid)) > 0);

--
-- Agent log of any node (decisions, seeding, rewind, fencing...).
--
CREATE FUNCTION agent_log(node text DEFAULT NULL, lines int DEFAULT 100, min_level text DEFAULT 'info')
RETURNS TABLE (log_time timestamptz, level text, message text)
LANGUAGE sql VOLATILE SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
	SELECT (e->>'time')::timestamptz, e->>'level', e->>'message'
	FROM jsonb_array_elements(@extschema@.call('log',
			jsonb_build_object('node', coalesce(node, ''), 'lines', lines, 'level', min_level))) AS e;
$$;

--
-- One-line verdict per problem, like the AG dashboard.  Returns a single
-- 'OK' row when the cluster is healthy.
--
CREATE FUNCTION health_check()
RETURNS TABLE (severity text, node text, problem text)
LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
	st jsonb;
	n jsonb;
	cfg jsonb;
	members int := 0;
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
		IF n->>'role' = 'replica' AND (n->>'sync')::boolean
		   AND n->>'state' = 'streaming' AND coalesce(n->>'sync_state', '') NOT IN ('sync', 'quorum') THEN
			severity := 'WARNING'; problem := 'configured as synchronous but sync_state is ' || coalesce(n->>'sync_state', 'unknown');
			found_problem := true; RETURN NEXT;
		END IF;
		IF n->>'role' = 'replica' AND (n->>'lag_bytes')::bigint > maxlag THEN
			severity := 'WARNING';
			problem := 'replica lags ' || pg_size_pretty((n->>'lag_bytes')::bigint) || ', more than maximum_lag_on_failover';
			found_problem := true; RETURN NEXT;
		END IF;
	END LOOP;

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

REVOKE ALL ON FUNCTION replication_raw(), agent_log(text, int, text), health_check() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION replication_raw(), agent_log(text, int, text), health_check() TO worlber_ha_admin, pg_monitor;
REVOKE ALL ON replication, redo, blocking FROM PUBLIC;
GRANT SELECT ON replication, redo, blocking TO worlber_ha_admin, pg_monitor;
