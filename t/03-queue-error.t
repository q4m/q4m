#! /usr/bin/perl

use Test::More tests => 15;

use strict;
use warnings;

use DBI;

sub dbi_connect {
    DBI->connect(
        $ENV{DBI} || 'dbi:mysql:database=test;host=localhost',
        $ENV{DBI_USER} || 'root',
        $ENV{DBI_PASSWORD} || '',
    ) or die 'connection failed:';
}

my $dbh = dbi_connect();
my $dbh2 = dbi_connect();

ok($dbh->do('drop table if exists q4m_t'));
ok($dbh->do('create table q4m_t (v int not null) engine=queue'));

ok($dbh->do("insert into q4m_t (v) values (1),(2)"));
is_deeply(
    [ $dbh2->selectrow_array("select queue_wait('q4m_t')") ],
    [ 1 ],
);
is_deeply(
    $dbh->selectall_arrayref("select * from q4m_t"),
    [ [ 2 ] ],
);
is_deeply(
    $dbh2->selectall_arrayref("select * from q4m_t"),
    [ [ 1 ] ],
);
ok($dbh2->do("select queue_abort()"));
is_deeply(
    $dbh->selectall_arrayref("select * from q4m_t"),
    [ [ 1 ], [ 2 ] ],
);
is_deeply(
    $dbh2->selectall_arrayref("select * from q4m_t"),
    [ [ 1 ], [ 2 ] ],
);

ok($dbh2->do("select queue_wait('q4m_t')"));
is_deeply(
    $dbh2->selectall_arrayref("select * from q4m_t"),
    [ [ 1 ] ],
);
ok($dbh2->do("select queue_end()"));
is_deeply(
    $dbh->selectall_arrayref("select * from q4m_t"),
    [ [ 2 ] ],
);
is_deeply(
    $dbh2->selectall_arrayref("select * from q4m_t"),
    [ [ 2 ] ],
);

$dbh->disconnect;
$dbh2->disconnect;

my $pid = fork;
if ($pid == 0) {
    $dbh = dbi_connect();
    $dbh->do("select queue_wait('q4m_t')");
    sleep 10;
    die "shouldn't reach here\n";
}
sleep 3;
kill 9, $pid;
sleep 2;
$dbh = dbi_connect();
is_deeply(
    $dbh->selectall_arrayref("select * from q4m_t"),
    [ [ 2 ] ],
);
