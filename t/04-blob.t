#! /usr/bin/perl

use strict;
use warnings;

use DBI;

my $TEST_ROWS;
BEGIN {
    $TEST_ROWS = $ENV{TEST_ROWS} || 1024;
};

use Test::More tests => $TEST_ROWS * 2 + 2;

my $dbh = DBI->connect(
    $ENV{DBI} || 'dbi:mysql:database=test;host=localhost',
    $ENV{DBI_USER} || 'root',
    $ENV{DBI_PASSWORD} || '',
) or die 'connection failed:';

ok($dbh->do('drop table if exists q4m_t'));
ok($dbh->do('create table q4m_t (s mediumtext) engine=queue'));

my @rows;

for (my $i = 0; $i < $TEST_ROWS; $i++) {
    my $row = '0123456789abcdef' x $i;
    ok($dbh->do("insert into q4m_t (s) values ('$row')"));
    push @rows, [ $row ];
    is_deeply(
        $dbh->selectall_arrayref('select * from q4m_t'),
        \@rows,
    );
}
