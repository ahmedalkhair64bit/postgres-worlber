--
-- worlber_ha without a running agent: objects, privileges and error paths.
-- The full cluster lifecycle is covered by the TAP test t/001_cluster.pl.
--
CREATE EXTENSION worlber_ha;

-- objects created by the extension
SELECT p.proname
FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
WHERE n.nspname = 'worlber_ha'
ORDER BY 1;

SELECT c.relname, c.relkind
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'worlber_ha'
ORDER BY 1;

-- management is restricted, monitoring is open to pg_monitor
SELECT has_function_privilege('public', 'worlber_ha.switchover(text)', 'EXECUTE') AS public_switchover,
       has_function_privilege('worlber_ha_admin', 'worlber_ha.switchover(text)', 'EXECUTE') AS admin_switchover,
       has_function_privilege('pg_monitor', 'worlber_ha.status()', 'EXECUTE') AS monitor_status,
       has_function_privilege('pg_monitor', 'worlber_ha.failover(text, boolean)', 'EXECUTE') AS monitor_failover,
       has_table_privilege('pg_monitor', 'worlber_ha.cluster_status', 'SELECT') AS monitor_view;

-- no agent listening
SET worlber_ha.agent_socket = '/nonexistent/worlber_ha.sock';
SELECT worlber_ha.agent_socket_path();
SELECT * FROM worlber_ha.cluster_status;
SELECT worlber_ha.create_cluster('prod');

-- argument validation happens before contacting the agent
SELECT worlber_ha.agent_request('bad-op');
SELECT worlber_ha.agent_request(NULL);

SET worlber_ha.agent_timeout = 0;
RESET worlber_ha.agent_timeout;
RESET worlber_ha.agent_socket;

DROP EXTENSION worlber_ha;
DROP ROLE worlber_ha_admin;
