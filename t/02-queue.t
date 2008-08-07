#! /usr/bin/perl

use strict;
use warnings;

use DBI;

my $TEST_ROWS;
BEGIN {
    $TEST_ROWS = $ENV{TEST_ROWS} || 1024;
};

use Test::More tests => $TEST_ROWS * 3 + 5;

my $dbh = DBI->connect(
    $ENV{DBI} || 'dbi:mysql:database=test;host=localhost',
    $ENV{DBI_USER} || 'root',
    $ENV{DBI_PASSWORD} || '',
) or die 'connection failed:';

# queue_wait should return null for non-existent table
is_deeply(
    $dbh->selectall_arrayref('select queue_wait("nonexistent_table",1)'),
    [ [ undef ] ],
);

ok($dbh->do('drop table if exists q4m_t'));
ok($dbh->do('create table q4m_t (v int not null) engine=queue'));

for (my $i = 0; $i < $TEST_ROWS; $i++) {
    ok($dbh->do("insert into q4m_t (v) values ($i)"));
    ok($dbh->do("select queue_wait('test.q4m_t')"));
    is_deeply(
        $dbh->selectall_arrayref('select * from q4m_t'),
        [ [ $i ] ],
    );
}

ok($dbh->do('select queue_end()'));
is_deeply(
    $dbh->selectall_arrayref('select * from q4m_t'),
    [],
);
