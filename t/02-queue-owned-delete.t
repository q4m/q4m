#! /usr/bin/perl

use strict;
use warnings;

use DBI;

use Test::More tests => 15;

my $dbh = DBI->connect(
    $ENV{DBI} || 'dbi:mysql:database=test;host=localhost',
    $ENV{DBI_USER} || 'root',
    $ENV{DBI_PASSWORD} || '',
) or die 'connection failed:';

ok($dbh->do('drop table if exists q4m_t'));
ok($dbh->do('create table q4m_t (v int not null) engine=queue'));

ok($dbh->do('insert into q4m_t values (1),(2),(3)'));

ok($dbh->do("select queue_wait('q4m_t')"));
is_deeply(
    $dbh->selectall_arrayref('select * from q4m_t'),
    [ [ 1 ] ],
);
ok($dbh->do('delete from q4m_t'));
is_deeply(
    $dbh->selectall_arrayref('select * from q4m_t'),
    [],
);
ok($dbh->do('select queue_abort()'));

is_deeply(
    $dbh->selectall_arrayref('select * from q4m_t'),
    [ [ 2 ], [ 3 ] ],
);

ok($dbh->do("select queue_wait('q4m_t')"));
is_deeply(
    $dbh->selectall_arrayref('select * from q4m_t'),
    [ [ 2 ] ],
);
ok($dbh->do('delete from q4m_t'));
is_deeply(
    $dbh->selectall_arrayref('select * from q4m_t'),
    [],
);
ok($dbh->do('select queue_end()'));

is_deeply(
    $dbh->selectall_arrayref('select * from q4m_t'),
    [ [ 3 ] ],
);
