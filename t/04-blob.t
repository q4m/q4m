#! /usr/bin/env perl

use strict;
use warnings;

use DBI;

my $TEST_ROWS;
BEGIN {
    $TEST_ROWS = $ENV{TEST_ROWS} || 1024;
};

use Test::More tests => int(($TEST_ROWS + 19) / 20) * 4 + 2;

my $dbh = DBI->connect(
    $ENV{DBI} || 'dbi:mysql:database=test;host=localhost',
    $ENV{DBI_USER} || 'root',
    $ENV{DBI_PASSWORD} || '',
) or die 'connection failed:';

ok($dbh->do('drop table if exists q4m_t'));
ok($dbh->do('create table q4m_t (s mediumtext) engine=queue'));

my @rows;

for (my $i = 0; $i < $TEST_ROWS; $i += 20) {
    my $row = '0123456789abcdef0123456789abcdef0123456789abcdef' x $i;
    ok($dbh->do("insert into q4m_t (s) values "
                    . join(',', map { "('$row')" } (0..19))));
    push @rows, map { [ $row ] } (0..19);
    is_deeply(
        $dbh->selectall_arrayref('select * from q4m_t'),
        \@rows,
    );
}

while (@rows >= 20) {
    ok($dbh->do("delete from q4m_t limit 20"));
    splice @rows, 0, 20;
    is_deeply(
        $dbh->selectall_arrayref('select * from q4m_t'),
        \@rows,
    );
}
