#! /usr/bin/perl

use strict;
use warnings;

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
    $dbh->do('create table q4m_t (t text not null) engine=queue default charset=utf8'),
    'create table',
);

my $kilodata = '0123456789abcdef' x 64;
my $qmegasql = 'insert into q4m_t values ' . join(
    ',',
    map { "('$kilodata')" } 1..256
);

# insert 100MB of data and then delete 74%
my $startnum = 1024 * 100;
for (my $i = 0; $i < $startnum / 256; $i++) {
    ok(
        $dbh->do($qmegasql),
        "insert $i",
    );
}
ok(
    $dbh->do('delete from q4m_t limit ' . int($startnum * 74 / 100)),
    "delete 74%",
);
my $total = $dbh->selectrow_arrayref(
    'select count(*) from q4m_t',
)->[0];

# fork 20 childs that does select
$dbh->disconnect;
undef $dbh;
my @pids;
for (my $i = 0; $i < 20; $i++) {
    my $pid = fork;
    die "fork failed:$!"
        unless defined $pid;
    if ($pid) {
        push @pids, $pid;
        next;
    }
    # child
    $dbh = dbi_connect();
    my $c = 0;
    while (1) {
        my $r = $dbh->selectrow_arrayref("SELECT queue_wait('q4m_t',1)")
            or die $dbh->errstr;
        last unless $r->[0];
        $c++;
    }
    open my $fh, '>', "$$.tmp"
        or die "failed to create file:$$.tmp:$!";
    print $fh $c;
    close $fh;
    exit(0);
}

# loop until count(*) becomes < 10
$dbh = dbi_connect();
while (1) {
    my $cnt = $dbh->selectrow_arrayref('select count(*) from q4m_t')->[0];
    ok(1, "left rows: $cnt");
    last if $cnt < 10;
    $dbh->do("insert into q4m_t values ('$kilodata')")
        or die $dbh->errstr;
    $total++;
}

# wait until all children dies, and calc sum of consumed rows
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
my $left = $dbh->selectrow_arrayref('select count(*) from q4m_t')->[0];

is($left + $consumed, $total, '# of rows consumed vs. total');

done_testing();
