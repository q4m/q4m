#! /usr/bin/perl

use Test::More tests => 14;

use strict;
use warnings;

use DBI;

my $dbh = DBI->connect(
    $ENV{DBI} || 'dbi:mysql:database=test;host=localhost',
    $ENV{DBI_USER} || 'root',
    $ENV{DBI_PASSWORD} || '',
) or die 'connection failed:';

ok($dbh->do('drop table if exists q4m_t'));
ok($dbh->do('create table q4m_t (v int not null) engine=queue'));

ok($dbh->do("select queue_wait('q4m_t', 1)"));
is_deeply(
    $dbh->selectall_arrayref("select * from q4m_t"),
    [],
);
ok($dbh->do("insert into q4m_t (v) values (1)"));
is_deeply(
    $dbh->selectall_arrayref("select * from q4m_t"),
    [],
);
ok($dbh->do("select queue_end()"));
is_deeply(
    $dbh->selectall_arrayref("select * from q4m_t"),
    [ [ 1 ] ],
);
ok($dbh->do("select queue_wait('q4m_t', 1)"));
is_deeply(
    $dbh->selectall_arrayref("select * from q4m_t"),
    [ [ 1 ] ],
);
ok($dbh->do("select queue_wait('q4m_t', 1)"));
is_deeply(
    $dbh->selectall_arrayref("select * from q4m_t"),
    [],
);
ok($dbh->do("select queue_end()"));
is_deeply(
    $dbh->selectall_arrayref("select * from q4m_t"),
    [],
);
