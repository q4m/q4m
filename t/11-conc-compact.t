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

# insert 500MB of data
for (my $i = 0; $i < 4 * 500; $i++) {
    ok(
        $dbh->do($qmegasql),
        "insert $i",
    );
}

# fork 4 childs that does select
$dbh->disconnect;
undef $dbh;
for (my $i = 0; $i < 4; $i++) {
    my $pid = fork;
    die "fork failed:$!"
        unless defined $pid;
    next unless $pid;
    # child
    $dbh = dbi_connect();
    while (1) {
        my $r = $dbh->selectrow_arrayref("SELECT queue_wait('q4m_t',1)")
            or die $dbh->errstr;
        exit(0) unless $r->[0];
    }
}

# loop until count(*) becomes < 10
$dbh = dbi_connect();
while (1) {
    my $cnt = $dbh->selectrow_arrayref('select count(*) from q4m_t')->[0];
    ok(1, "left rows: $cnt");
    last if $cnt < 10;
    $dbh->do("insert into q4m_t values ('$kilodata')")
        or die $dbh->errstr;
}

ok(1, 'done');

done_testing();
