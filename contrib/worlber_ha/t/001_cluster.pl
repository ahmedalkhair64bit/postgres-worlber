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
use HTTP::Tiny;
use MIME::Base64 qw(decode_base64);

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

# A test that dies skips the cleanup at the end: never leave agents behind,
# or they restart PostgreSQL in the next run's data directories.
END
{
	my $exit = $?;
	kill 'KILL', values %agents;
	$? = $exit;
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

# --- Join hardening (D16) ----------------------------------------------------
# The token is read from a file or stdin, so it stays out of shell history.
my $jt_file = "$PostgreSQL::Test::Utils::tmp_check/join.token";
open my $jfh, '>', $jt_file or die $!;
print $jfh "$jt\n";
close $jfh;
my ($dry_out, $dry_err) = run_command([ $agent_bin, 'init', '--join-file', $jt_file, '--name', 'pg9', '--dry-run' ]);
like($dry_out, qr/cluster: prod/, 'init --join-file reads the token (dry run)');
my $dry_stdin = `printf '%s' '$jt' | '$agent_bin' init --join-file - --name pg9 --dry-run`;
like($dry_stdin, qr/cluster: prod/, 'init --join-file - reads the token from stdin');

# Redeem it by hand against /peer/join (no cluster token: the nonce is the auth).
my $jt_json = $jt =~ s/^wjt1\.//r;
$jt_json =~ tr{-_}{+/};
$jt_json .= '=' x ((4 - length($jt_json) % 4) % 4);
my ($nonce) = decode_base64($jt_json) =~ /"nonce":"([0-9a-f]{64})"/;
ok(defined $nonce, 'the join token carries a nonce');
my $join_url = "http://127.0.0.1:$ports{$new_primary->name}{api}/peer/join";
my $http = HTTP::Tiny->new(timeout => 30);
sub join_post
{
	my ($n, $name) = @_;
	my $body = sprintf('{"nonce":"%s","name":"%s","api_addr":"127.0.0.1:1","raft_addr":"127.0.0.1:2"}', $n, $name);
	return $http->post($join_url, { content => $body, headers => { 'Content-Type' => 'application/json' } });
}
my $r = join_post($nonce, 'pg9');
like($r->{content}, qr/"ok":true.*"token":"\Q$token\E"/, 'first use of the join token returns the cluster secret');
$r = join_post($nonce, 'pg9');
like($r->{content}, qr/"ok":true.*"token":"\Q$token\E"/, 'a retry from the same host gets the same reply (retry-safe)');
$r = join_post($nonce, 'pg8');
like($r->{content}, qr/already used by another host/, 'another host cannot reuse a consumed token');
$r = join_post('0' x 64, 'pg8');
like($r->{content}, qr/unknown or already-pruned/, 'an unknown nonce is rejected');
my $short = $new_primary->safe_psql('postgres', "SELECT worlber_ha.join_token('1 second', false)");
my $short_json = $short =~ s/^wjt1\.//r;
$short_json =~ tr{-_}{+/};
$short_json .= '=' x ((4 - length($short_json) % 4) % 4);
my ($short_nonce) = decode_base64($short_json) =~ /"nonce":"([0-9a-f]{64})"/;
sleep 3;
$r = join_post($short_nonce, 'pg8');
like($r->{content}, qr/expired|unknown or already-pruned/, 'an expired join token is rejected');
# That was the 5th attempt from 127.0.0.1 this minute: the 6th gets 429.
$r = join_post($nonce, 'pg9');
is($r->{status}, 429, 'the 6th join attempt in a minute is rate-limited');
poll_cluster("SELECT count(*) >= 4 FROM worlber_ha.events WHERE kind = 'join_rejected'",
	't', [$new_primary], 'rejected joins are recorded as join_rejected events');
like($new_primary->safe_psql('postgres', "SELECT string_agg(detail, ' | ') FROM worlber_ha.events WHERE kind = 'join_rejected'"),
	qr/rate limit from 127\.0\.0\.1/, 'join_rejected records the source IP');

# --- Live proof: drill() (W5) -----------------------------------------------
# Run from a replica: the switchover stops the current primary, which would
# cut a session connected to it.
my $replica_for_drill;
foreach my $node (@nodes)
{
	my ($r, $o) = $node->psql('postgres', 'SELECT pg_is_in_recovery()');
	if ($r == 0 && $o eq 't') { $replica_for_drill = $node; last; }
}
my ($dret, $dout, $derr) = $replica_for_drill->psql('postgres', 'SELECT worlber_ha.drill()');
is($dret, 0, "drill() completes ($derr)");
poll_cluster("SELECT last_drill_passed::text || ':' || last_drill_canary_present::text || ':' || (last_drill_rto_ms > 0)::text FROM worlber_ha.proof",
	'true:true:true', [$replica_for_drill], 'proof shows a passing drill, the canary row on the new primary and a measured RTO');
diag('drill RTO ms: ' . $replica_for_drill->safe_psql('postgres', 'SELECT last_drill_rto_ms FROM worlber_ha.proof'));

# The extension may live in a database other than postgres: the canary row
# must be written and checked in that database.
my ($primary_now, $replica_now);
foreach my $node (@nodes)
{
	my ($r, $o) = $node->psql('postgres', 'SELECT pg_is_in_recovery()');
	next if $r != 0;
	if ($o eq 'f') { $primary_now = $node; } else { $replica_now //= $node; }
}
$primary_now->safe_psql('postgres', 'CREATE DATABASE app');
$primary_now->safe_psql('app', 'CREATE EXTENSION worlber_ha');
$replica_now->poll_query_until('app', "SELECT count(*) = 1 FROM pg_extension WHERE extname = 'worlber_ha'")
  or die 'the app database did not reach the replica';
# Both standbys streaming again after the first drill (a switchover while a
# standby is still rejoining is a separate case, see TODOS.md).
$primary_now->poll_query_until('postgres', "SELECT count(*) = 2 FROM pg_stat_replication WHERE state = 'streaming'")
  or die 'standbys not streaming before the second drill';
($dret, $dout, $derr) = $replica_now->psql('app', 'SELECT worlber_ha.drill()');
is($dret, 0, "drill() completes from a database other than postgres ($derr)");
poll_cluster("SELECT last_drill_passed::text || ':' || last_drill_canary_present::text || ':' || drill_pass_streak FROM worlber_ha.proof",
	'true:true:2', [$replica_now], 'the drill from the app database passes');
$replica_now->poll_query_until('app', 'SELECT count(*) = 1 FROM worlber_ha_internal.canary')
  or die 'no canary row in app';
pass('the canary row is written in the database the drill ran in');

# --- WAL archive set on a running cluster (W6) -----------------------------
# A standby that is already running must get restore_command (reloaded at
# once) and archive_mode (applied at its next restart), so it archives if it
# is promoted later.
# The second drill demoted $primary_now: wait until it streams again, then
# use the current roles.
poll_cluster("SELECT count(*) FILTER (WHERE role = 'primary') || ':' || count(*) FILTER (WHERE state = 'streaming') FROM worlber_ha.cluster_status",
	'1:2', \@nodes, 'one primary and two streaming standbys after the drills');
my ($arch_primary, $arch_standby);
foreach my $node (@nodes)
{
	my ($r, $o) = $node->psql('postgres', 'SELECT pg_is_in_recovery()');
	next if $r != 0;
	if ($o eq 'f') { $arch_primary = $node; } else { $arch_standby //= $node; }
}
my $archive_dir = PostgreSQL::Test::Utils::tempdir();
($ret, $out, $err) = $arch_primary->psql('postgres', "SELECT worlber_ha.set_config('wal_archive_dir', '$archive_dir')");
is($ret, 0, "wal_archive_dir set on a running cluster ($err)");
$arch_standby->poll_query_until('postgres', "SELECT current_setting('restore_command') LIKE '%$archive_dir%'")
  or die 'restore_command not set on the running standby';
pass('a running standby gets restore_command');
$arch_standby->poll_query_until('postgres', "SELECT count(*) = 1 FROM pg_file_settings WHERE name = 'archive_mode' AND setting = 'on' AND applied IS NOT TRUE")
  or die 'archive_mode not set on the running standby';
pass('a running standby gets archive_mode for its next restart');
sleep(10);    # several reconcile passes
is($arch_primary->safe_psql('postgres', "SELECT count(*) <= 3 FROM worlber_ha.events WHERE kind = 'archive_restart_needed'"),
	't', 'archive_restart_needed is reported once per node, not on every pass');

# --- Cleanup ---------------------------------------------------------------
kill_agent($_) foreach keys %agents;
foreach my $node (@nodes)
{
	system_log('pg_ctl', '-D', $node->data_dir, '-m', 'immediate', 'stop')
	  if -f $node->data_dir . '/postmaster.pid';
}

done_testing();
