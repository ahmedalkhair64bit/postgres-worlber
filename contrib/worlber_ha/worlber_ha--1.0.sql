/* contrib/worlber_ha/worlber_ha--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION worlber_ha" to load this file. \quit

-- Role allowed to manage the cluster without being superuser.
DO $$
BEGIN
	IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'worlber_ha_admin') THEN
		CREATE ROLE worlber_ha_admin NOLOGIN;
	END IF;
END
$$;

--
-- Low-level transport to the local worlber-ha-agent.
--
CREATE FUNCTION agent_request(op text, args jsonb DEFAULT '{}', timeout_s int DEFAULT NULL)
RETURNS jsonb
AS 'MODULE_PATHNAME', 'worlber_ha_agent_request'
LANGUAGE C VOLATILE PARALLEL UNSAFE;

CREATE FUNCTION agent_socket_path()
RETURNS text
AS 'MODULE_PATHNAME', 'worlber_ha_agent_socket_path'
LANGUAGE C STABLE;

-- Call the agent, raise its error, emit its warnings, return its result.
CREATE FUNCTION call(op text, args jsonb DEFAULT '{}', timeout_s int DEFAULT NULL)
RETURNS jsonb
LANGUAGE plpgsql VOLATILE
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
	r jsonb;
	w text;
BEGIN
	r := @extschema@.agent_request(op, args, timeout_s);
	IF NOT coalesce((r->>'ok')::boolean, false) THEN
		IF r->>'hint' IS NOT NULL THEN
			RAISE EXCEPTION 'worlber_ha: %', r->>'error' USING HINT = r->>'hint';
		END IF;
		RAISE EXCEPTION 'worlber_ha: %', coalesce(r->>'error', 'unknown agent error');
	END IF;
	FOR w IN SELECT jsonb_array_elements_text(coalesce(r->'warnings', '[]'::jsonb)) LOOP
		RAISE WARNING 'worlber_ha: %', w;
	END LOOP;
	RETURN r->'result';
END
$$;

--
-- Cluster management (like CREATE AVAILABILITY GROUP / ALTER AVAILABILITY GROUP)
--

-- Turn this primary into a one-node HA cluster.
CREATE FUNCTION create_cluster(name text,
							   synchronous_mode text DEFAULT 'available',
							   failover_timeout interval DEFAULT '30 seconds')
RETURNS jsonb
LANGUAGE sql VOLATILE
AS $$
	SELECT @extschema@.call('create_cluster',
		jsonb_build_object('name', name,
						   'synchronous_mode', synchronous_mode,
						   'failover_timeout', extract(epoch FROM failover_timeout)::bigint || 's'));
$$;

-- Add a node running worlber-ha-agent.  The agent seeds the replica with
-- pg_basebackup (like AG automatic seeding) and starts streaming.
CREATE FUNCTION add_node(host text,
						 agent_port int DEFAULT 7400,
						 sync boolean DEFAULT false,
						 wait boolean DEFAULT true,
						 timeout_s int DEFAULT 900)
RETURNS jsonb
LANGUAGE sql VOLATILE
AS $$
	SELECT @extschema@.call('add_node',
		jsonb_build_object('host', host, 'agent_port', agent_port, 'sync', sync,
						   'wait', wait, 'timeout', timeout_s),
		timeout_s + 60);
$$;

-- A witness is an agent started with --witness: it votes, holds no data.
CREATE FUNCTION add_witness(host text, agent_port int DEFAULT 7400)
RETURNS jsonb
LANGUAGE sql VOLATILE
AS $$
	SELECT @extschema@.call('add_node',
		jsonb_build_object('host', host, 'agent_port', agent_port, 'wait', false));
$$;

CREATE FUNCTION remove_node(node text)
RETURNS jsonb
LANGUAGE sql VOLATILE
AS $$ SELECT @extschema@.call('remove_node', jsonb_build_object('name', node)); $$;

-- Planned role change without data loss.  NULL target = best standby.
CREATE FUNCTION switchover(target text DEFAULT NULL)
RETURNS jsonb
LANGUAGE sql VOLATILE
AS $$
	SELECT @extschema@.call('switchover', jsonb_build_object('target', coalesce(target, '')), 900);
$$;

-- Manual failover.  force => true promotes even if the primary looks
-- healthy or the standby is lagging (FORCE_FAILOVER_ALLOW_DATA_LOSS).
CREATE FUNCTION failover(target text DEFAULT NULL, force boolean DEFAULT false)
RETURNS jsonb
LANGUAGE sql VOLATILE
AS $$
	SELECT @extschema@.call('failover', jsonb_build_object('target', coalesce(target, ''), 'force', force));
$$;

-- Synchronous (true) or asynchronous (false) commit for one node.
CREATE FUNCTION set_sync(node text, sync boolean)
RETURNS jsonb
LANGUAGE sql VOLATILE
AS $$ SELECT @extschema@.call('set_sync', jsonb_build_object('name', node, 'sync', sync)); $$;

-- Options: failover_timeout, maximum_lag_on_failover, synchronous_mode.
CREATE FUNCTION set_config(option text, value text)
RETURNS jsonb
LANGUAGE sql VOLATILE
AS $$ SELECT @extschema@.call('set_config', jsonb_build_object('key', option, 'value', value)); $$;

-- Maintenance mode: no automatic failover, agents leave PostgreSQL alone.
CREATE FUNCTION pause()
RETURNS jsonb
LANGUAGE sql VOLATILE
AS $$ SELECT @extschema@.call('set_config', '{"key": "paused", "value": "true"}'); $$;

CREATE FUNCTION resume()
RETURNS jsonb
LANGUAGE sql VOLATILE
AS $$ SELECT @extschema@.call('set_config', '{"key": "paused", "value": "false"}'); $$;

--
-- Monitoring (like sys.dm_hadr_availability_replica_states)
--
-- Read-only calls are SECURITY DEFINER so pg_monitor can use the views
-- without being able to run management operations.
CREATE FUNCTION status()
RETURNS jsonb
LANGUAGE sql VOLATILE SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$ SELECT @extschema@.call('status'); $$;

CREATE FUNCTION config()
RETURNS jsonb
LANGUAGE sql VOLATILE SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$ SELECT @extschema@.call('config'); $$;

CREATE FUNCTION event_log()
RETURNS jsonb
LANGUAGE sql VOLATILE SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$ SELECT @extschema@.call('events'); $$;

CREATE VIEW cluster_status AS
	SELECT n.node, n.host, n.port, n.role, n.state, n.sync, nullif(n.sync_state, '') AS sync_state,
		   n.timeline, n.lsn::pg_lsn AS lsn, n.replay_lsn::pg_lsn AS replay_lsn,
		   n.lag_bytes, n.raft_role,
		   CASE WHEN n.last_seen_ms IS NOT NULL
				THEN make_interval(secs => n.last_seen_ms / 1000.0) END AS last_seen,
		   nullif(n.agent_status, '') AS agent_status
	FROM jsonb_to_recordset(@extschema@.status()->'nodes')
		 AS n(node text, host text, port int, role text, state text, sync boolean,
			  sync_state text, timeline int, lsn text, replay_lsn text, lag_bytes bigint,
			  raft_role text, last_seen_ms bigint, agent_status text);

CREATE VIEW cluster_config AS
	SELECT key AS option, value
	FROM jsonb_each_text(@extschema@.config())
	ORDER BY 1;

CREATE VIEW events AS
	SELECT x.seq, to_timestamp((x.e->>'time')::bigint) AS event_time, x.e->>'kind' AS kind,
		   nullif(x.e->>'node', '') AS node, (x.e->>'epoch')::bigint AS epoch,
		   nullif(x.e->>'detail', '') AS detail
	FROM jsonb_array_elements(@extschema@.event_log()) WITH ORDINALITY AS x(e, seq)
	ORDER BY x.seq DESC;

-- Multi-host libpq URI that always reaches the primary (the "listener").
CREATE FUNCTION connection_string(dbname text DEFAULT current_database(),
								  read_only boolean DEFAULT false)
RETURNS text
LANGUAGE sql VOLATILE
AS $$
	SELECT 'postgresql://' ||
		   string_agg(CASE WHEN n->>'host' LIKE '/%'
						   THEN '%2F' || replace(substr(n->>'host', 2), '/', '%2F')
						   ELSE n->>'host' END || ':' || (n->>'port'), ',' ORDER BY n->>'node') ||
		   '/' || dbname ||
		   CASE WHEN read_only THEN '?target_session_attrs=prefer-standby'
				ELSE '?target_session_attrs=read-write' END
	FROM jsonb_array_elements(@extschema@.status()->'nodes') AS n
	WHERE n->>'role' <> 'witness';
$$;

--
-- Privileges: only superusers and worlber_ha_admin may manage the cluster;
-- pg_monitor may look at it.
--
REVOKE ALL ON FUNCTION agent_request(text, jsonb, int), call(text, jsonb, int),
	create_cluster(text, text, interval), add_node(text, int, boolean, boolean, int),
	add_witness(text, int), remove_node(text), switchover(text), failover(text, boolean),
	set_sync(text, boolean), set_config(text, text), pause(), resume(), status(),
	config(), event_log(), connection_string(text, boolean) FROM PUBLIC;

GRANT EXECUTE ON FUNCTION agent_request(text, jsonb, int), call(text, jsonb, int),
	create_cluster(text, text, interval), add_node(text, int, boolean, boolean, int),
	add_witness(text, int), remove_node(text), switchover(text), failover(text, boolean),
	set_sync(text, boolean), set_config(text, text), pause(), resume(), status(),
	config(), event_log(), connection_string(text, boolean) TO worlber_ha_admin;
GRANT EXECUTE ON FUNCTION status(), config(), event_log(), connection_string(text, boolean) TO pg_monitor;

REVOKE ALL ON cluster_status, cluster_config, events FROM PUBLIC;
GRANT SELECT ON cluster_status, cluster_config, events TO worlber_ha_admin, pg_monitor;
