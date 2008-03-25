#! /usr/bin/perl

use strict;
use warnings;

use Data::Compare;
use DBI;

use Test::More tests => 17;

sub dbi_connect {
    DBI->connect(
        $ENV{DBI} || 'dbi:mysql:database=test;host=localhost',
        $ENV{DBI_USER} || 'root',
        $ENV{DBI_PASSWORD} || '',
    ) or die 'connection failed:';
}

my $dbh = dbi_connect();

ok($dbh->do('drop table if exists q4m_t'));
ok($dbh->do('create table q4m_t (v int not null) engine=queue'));

ok($dbh->do("insert into q4m_t (v) values (1)"));
is_deeply(
    $dbh->selectall_arrayref("select queue_wait('q4m_t:v!=1', 1)"),
    [ [ 0 ] ],
);
is_deeply(
    $dbh->selectall_arrayref("select queue_wait('q4m_t:v=1', 1)"),
    [ [ 1 ] ],
);
is_deeply(
    $dbh->selectall_arrayref('select * from q4m_t'),
    [ [ 1 ] ],
);
ok($dbh->do('select queue_end()'));

$dbh->disconnect();

unless (fork) {
    sleep 2;
    $dbh = dbi_connect();
    $dbh->do('insert into q4m_t values (3)');
    exit 0;
}
$dbh = dbi_connect();
is_deeply(
    $dbh->selectall_arrayref("select queue_wait('q4m_t:v=3')"),
    [ [ 1 ] ],
);
is_deeply(
    $dbh->selectall_arrayref('select * from q4m_t'),
    [ [ 3 ] ],
);
ok($dbh->do('select queue_end()'));
is_deeply(
    $dbh->selectall_arrayref('select * from q4m_t'),
    [],
);
$dbh->disconnect;

unless (fork) {
    $dbh = dbi_connect();
    exit 1
        unless Compare(
            $dbh->selectall_arrayref("select queue_wait('q4m_t:v=4')"),
            [ [ 1 ] ],
        );
    exit 0;
}
sleep 2;
$dbh = dbi_connect;
ok($dbh->do('insert into q4m_t values (4)'));
wait;
ok($? == 0);
is_deeply(
    $dbh->selectall_arrayref('select * from q4m_t'),
    [ [ 4 ] ],
);
is_deeply(
    $dbh->selectall_arrayref("select queue_wait('q4m_t:v=4')"),
    [ [ 1 ] ],
);
is_deeply(
    $dbh->selectall_arrayref('select * from q4m_t'),
    [ [ 4 ] ],
);
ok($dbh->do('select queue_end()'));
