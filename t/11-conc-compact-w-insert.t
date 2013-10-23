#! /usr/bin/env perl

# should set queue-use-concurrent-compaction=1 and queue-concurrent-compaction-interval to a small value

use strict;
use warnings;

use Parallel::ForkManager;
use DBI;

use Test::More;

sub dbi_connect {
    my $dbh = DBI->connect(
        $ENV{DBI} || 'dbi:mysql:database=test;host=localhost',
        $ENV{DBI_USER} || 'root',
        $ENV{DBI_PASSWORD} || '',
    ) or die 'connection failed:' . $DBI::errstr;
    $dbh;
}

my $dbh = dbi_connect();

ok(
    $dbh->do('drop table if exists q4m_t'),
    'drop table',
);

ok(
    $dbh->do('create table q4m_t (t text not null) engine=queue default charset=
utf8'),
    'create table',
);

my $TOTAL_ROWS = 65536 * 4;
my $kilodata = '0123456789abcdef' x 64;
my $rows_per_insert = 16;
my $insert_sql = 'insert into q4m_t values ' . join(
    ',',
    map {
	"('" . substr($kilodata, 0, int(rand() * 1024)) . "')"
    } 1..$rows_per_insert
);

undef $dbh;

# fork 20 consumers (that leaves 2100 records in queue)
my @pids;
for (my $i = 0; $i < 20; $i++) {
    my $pid = fork;
    die "fork failed:$!"
        unless defined $pid;
    if ($pid != 0) {
        push @pids, $pid;
        next;
    }
    # child
    $dbh = dbi_connect();
    my $c = 0;
    my $do_exit;
    local $SIG{TERM} = sub {
	$do_exit = 1;
    };
 LAST_LOOP:
    while (1) {
        while (1) {
	    last LAST_LOOP if $do_exit;
            my $r = $dbh->selectrow_arrayref('SELECT COUNT(*) FROM q4m_t')
                or die $dbh->errstr;
	    print STDERR "have: $r->[0] rows\n"
		if $ENV{DEBUG};
            last if $r->[0] > 10000;
            sleep 1;
        }
        for (my $i = 0; $i < 100; $i++) {
            my $r = $dbh->selectrow_arrayref("SELECT queue_wait('q4m_t',1)")
                or die $dbh->errstr;
            die "the queue is empty (logic flaw"
		unless $r->[0];
            $c++;
        }
	$dbh->do("SELECT queue_end()") or die $dbh->errstr;
    }
    open my $fh, '>', "$$.tmp"
        or die "failed to create file:$$.tmp:$!";
    print $fh $c;
    close $fh;
    exit 0;
}

# insert
$dbh = dbi_connect();
for (my $i = 0; $i < $TOTAL_ROWS / $rows_per_insert; $i++) { # total 1M rows
    ok($dbh->do($insert_sql), "insert $i");
}

kill 'TERM', $_
    for @pids;

# count the rows consumed
my $consumed = 0;
while (@pids) {
    my $pid = shift @pids;
    while (waitpid($pid, 0) == -1) {}
    open my $fh, '<', "$pid.tmp"
        or die "failed to open file:$pid.tmp:$!";
    $consumed += <$fh>;
    close $fh;
    unlink "$pid.tmp";
}

my $left = $dbh->selectrow_arrayref('SELECT COUNT(*) FROM q4m_t')->[0];

is($left + $consumed, $TOTAL_ROWS, 'no missing rows');

done_testing;
