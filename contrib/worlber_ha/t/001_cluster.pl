# Copyright (c) 2026, Worlber
#
# End-to-end test of worlber_ha: build a 3-node cluster from psql, switch
# over, lose the primary (automatic failover), and rejoin the old primary.
#
# Requires the agent binary: set WORLBER_HA_AGENT=/path/to/worlber-ha-agent
# (built with "make -C contrib/worlber_ha agent").

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep);

my $agent_bin = $ENV{WORLBER_HA_AGENT};
if (!defined $agent_bin || !-x $agent_bin)
{
	plan skip_all => 'WORLBER_HA_AGENT is not set to the worlber-ha-agent binary';
}

my $token = 'tap-test-secret';
my $bindir = (split /\n/, `pg_config --bindir`)[0];

# Only node1 gets initdb; the agent seeds the others with pg_basebackup.
my @nodes = map { PostgreSQL::Test::Cluster->new("node$_") } 1 .. 3;
$nodes[0]->init(allows_streaming => 1);
$nodes[0]->start;

my %agents;    # node name => pid
my %ports;

foreach my $node (@nodes)
{
	$ports{ $node->name } = {
		api => PostgreSQL::Test::Cluster::get_free_port(),
		raft => PostgreSQL::Test::Cluster::get_free_port(),
	};
}

sub start_agent
{
	my ($node) = @_;
	my $name = $node->name;
	my $log = "$PostgreSQL::Test::Utils::log_path/agent_$name.log";
	my $pid = fork();
	die "fork failed: $!" unless defined $pid;
	if ($pid == 0)
	{
		open STDOUT, '>>', $log or die $!;
		open STDERR, '>&', \*STDOUT or die $!;
		exec($agent_bin,
			'--name' => $name,
			'--token' => $token,
			'--data-dir' => $node->basedir . '/agent',
			'--pgdata' => $node->data_dir,
			'--pgbin' => $bindir,
			'--pg-socket-dir' => $node->host,
			'--pg-port' => $node->port,
			'--pg-user' => scalar(getpwuid($<)),
			'--pg-advertise-host' => $node->host,
			'--pg-log' => $node->logfile,
			'--api-addr' => "127.0.0.1:$ports{$name}{api}",
			'--raft-addr' => "127.0.0.1:$ports{$name}{raft}",
			'--log-level' => 'debug') or die "exec: $!";
	}
	$agents{$name} = $pid;
	# wait for the extension socket
	my $sock = $node->host . '/.s.WORLBER_HA.' . $node->port;
	for (1 .. 100)
	{
		last if -S $sock;
		usleep(100_000);
	}
	ok(-S $sock, "agent for $name is listening");
}

sub kill_agent
{
	my ($name, $sig) = @_;
	return unless $agents{$name};
	kill $sig // 'TERM', $agents{$name};
	waitpid($agents{$name}, 0);
	delete $agents{$name};
}

# Poll a query on any of the given nodes until it returns the expected value.
sub poll_cluster
{
	my ($query, $expected, $on, $msg) = @_;
	my $deadline = time() + $PostgreSQL::Test::Utils::timeout_default;
	my $last = '';
	while (time() < $deadline)
	{
		foreach my $node (@$on)
		{
			my ($ret, $out, $err) = $node->psql('postgres', $query);
			$last = "$out$err";
			if ($ret == 0 && $out eq $expected)
			{
				pass($msg);
				return 1;
			}
		}
		usleep(500_000);
	}
	fail($msg);
	diag("last result: $last");
	return 0;
}

start_agent($_) foreach @nodes;

my ($n1, $n2, $n3) = @nodes;

# --- Build the cluster from psql -----------------------------------------
$n1->safe_psql('postgres', 'CREATE EXTENSION worlber_ha');
my $res = $n1->safe_psql('postgres',
	"SELECT worlber_ha.create_cluster('prod', failover_timeout => '7 seconds')->>'primary'");
is($res, 'node1', 'create_cluster makes node1 the primary');

my ($ret, $out, $err) = $n1->psql('postgres', "SELECT worlber_ha.create_cluster('again')");
like($err, qr/already belongs to cluster "prod"/, 'create_cluster twice is rejected');

$res = $n1->safe_psql('postgres',
	"SELECT worlber_ha.add_node('127.0.0.1', agent_port => $ports{node2}{api}, sync => true, timeout_s => 120)->>'state'");
is($res, 'streaming', 'node2 added as synchronous replica');
$res = $n1->safe_psql('postgres',
	"SELECT worlber_ha.add_node('127.0.0.1', agent_port => $ports{node3}{api}, timeout_s => 120)->>'state'");
is($res, 'streaming', 'node3 added as asynchronous replica');

poll_cluster(
	"SELECT string_agg(node || ':' || role || ':' || state || ':' || coalesce(sync_state, '-'), ' ' ORDER BY node) FROM worlber_ha.cluster_status",
	'node1:primary:running:- node2:replica:streaming:sync node3:replica:streaming:potential',
	[$n1], 'cluster status shows one primary and two streaming replicas');

is($n1->safe_psql('postgres', 'SHOW synchronous_standby_names'),
	'FIRST 1 ("node2", "node3")', 'synchronous_standby_names managed by the agent');

$n1->safe_psql('postgres', 'CREATE TABLE t (id int); INSERT INTO t SELECT generate_series(1, 100)');
poll_cluster('SELECT count(*) FROM t', '100', [$n2], 'data replicated to node2');
poll_cluster('SELECT count(*) FROM t', '100', [$n3], 'data replicated to node3');

# The extension also works on a replica: requests go to the Raft leader.
like($n3->safe_psql('postgres', 'SELECT worlber_ha.connection_string()'),
	qr/target_session_attrs=read-write$/, 'connection_string works on a replica');

# --- Planned switchover ----------------------------------------------------
$res = $n3->safe_psql('postgres', "SELECT worlber_ha.switchover('node2')->>'new_primary'");
is($res, 'node2', 'switchover to node2 requested from node3');

poll_cluster(
	"SELECT string_agg(node || ':' || role || ':' || state, ' ' ORDER BY node) FROM worlber_ha.cluster_status",
	'node1:replica:streaming node2:primary:running node3:replica:streaming',
	[$n2, $n3], 'after switchover node2 is primary and node1 rejoined');

$n2->safe_psql('postgres', 'INSERT INTO t SELECT generate_series(101, 200)');
poll_cluster('SELECT count(*) FROM t', '200', [$n1], 'old primary node1 follows node2');
poll_cluster('SELECT count(*) FROM t', '200', [$n3], 'node3 follows node2');

# --- Automatic failover: lose node2 (agent and server) ---------------------
kill_agent('node2', 'KILL');
# node2 was started by its agent, so Cluster.pm does not track its pid
system_or_bail('pg_ctl', '-D', $n2->data_dir, '-m', 'immediate', 'stop');

poll_cluster(
	"SELECT count(*) FROM worlber_ha.cluster_status WHERE role = 'primary' AND state = 'running' AND node <> 'node2'",
	'1', [$n1, $n3], 'automatic failover promoted a surviving node');

my $new_primary;
foreach my $node ($n1, $n3)
{
	my ($r, $o) = $node->psql('postgres', 'SELECT pg_is_in_recovery()');
	$new_primary = $node if $r == 0 && $o eq 'f';
}
ok(defined $new_primary, 'found the new primary');
my $other = $new_primary == $n1 ? $n3 : $n1;
is($new_primary->safe_psql('postgres', 'SELECT count(*) FROM t'), '200',
	'no committed data lost');

$new_primary->safe_psql('postgres', 'INSERT INTO t SELECT generate_series(201, 300)');
poll_cluster('SELECT count(*) FROM t', '300', [$other], 'remaining standby follows the new primary');

# --- The old primary comes back and rejoins as a replica ------------------
start_agent($n2);
poll_cluster(
	"SELECT role || ':' || state FROM worlber_ha.cluster_status WHERE node = 'node2'",
	'replica:streaming', [$new_primary], 'node2 rejoined as a streaming replica');
poll_cluster('SELECT count(*) FROM t', '300', [$n2], 'rejoined node2 has all data');

# All three nodes hold exactly the same data.
is($new_primary->safe_psql('postgres',
		"SELECT string_agg(node || ':' || matches_primary, ',' ORDER BY node) FROM worlber_ha.verify_table('t')"),
	'node1:true,node2:true,node3:true', 'verify_table: identical data on every node');
like($new_primary->safe_psql('postgres',
		"SELECT detail FROM worlber_ha.events WHERE kind = 'automatic_failover'"),
	qr/estimated data loss: (none|up to \d+ bytes)/, 'failover event reports the data loss estimate');

my $events = $new_primary->safe_psql('postgres',
	"SELECT string_agg(kind, ',' ORDER BY seq) FROM worlber_ha.events WHERE kind IN ('cluster_created', 'switchover', 'automatic_failover')");
is($events, 'cluster_created,switchover,automatic_failover', 'event history recorded');

# --- Maintenance mode -------------------------------------------------------
$new_primary->safe_psql('postgres', 'SELECT worlber_ha.pause()');
is($new_primary->safe_psql('postgres', "SELECT value FROM worlber_ha.cluster_config WHERE option = 'paused'"),
	'true', 'cluster paused');
$new_primary->safe_psql('postgres', 'SELECT worlber_ha.resume()');

($ret, $out, $err) = $new_primary->psql('postgres', "SELECT worlber_ha.set_config('failover_timeout', '1s')");
like($err, qr/failover_timeout must be/, 'invalid option rejected');

# --- Fencing / archive visibility (D13) -------------------------------------
# The first agent adds worlber_ha to shared_preload_libraries and replicas
# inherit it through pg_basebackup, so a promoted node runs the fence worker
# ("armed").  Either way cluster_status and health_check must agree.
my $fence = '';
foreach (1 .. 60)
{
	$fence = $new_primary->safe_psql('postgres',
		"SELECT coalesce(fence_state, '') FROM worlber_ha.cluster_status WHERE role = 'primary'");
	last if $fence eq 'armed' || $fence eq 'inactive';
	sleep 1;
}
like($fence, qr/^(armed|inactive)$/, "cluster_status reports the primary's fence state ($fence)");
my $problems = $new_primary->safe_psql('postgres', "SELECT string_agg(problem, ' | ') FROM worlber_ha.health_check()");
if ($fence eq 'inactive')
{
	like($problems, qr/fencing is not active/, 'health_check warns that fencing is not active');
}
else
{
	unlike($problems, qr/fencing is not active/, 'health_check does not warn while fencing is armed');
}

# --- Join token (W10) ------------------------------------------------------
# A fresh cluster has the fencing gate open, so join_token works.
my $jt = $new_primary->safe_psql('postgres', "SELECT worlber_ha.join_token('10 minutes', false)");
like($jt, qr/^wjt1\./, 'join_token returns a wjt1 token');
# A non-admin role cannot mint one.
$new_primary->safe_psql('postgres', 'CREATE ROLE joiner LOGIN');
($ret, $out, $err) = $new_primary->psql('postgres', "SET ROLE joiner; SELECT worlber_ha.join_token()");
like($err, qr/permission denied/, 'non-admin cannot call join_token');

# --- Cleanup ---------------------------------------------------------------
kill_agent($_) foreach keys %agents;
foreach my $node (@nodes)
{
	system_log('pg_ctl', '-D', $node->data_dir, '-m', 'immediate', 'stop')
	  if -f $node->data_dir . '/postmaster.pid';
}

done_testing();
