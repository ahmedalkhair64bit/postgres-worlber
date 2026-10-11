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
