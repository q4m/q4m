#! /usr/bin/perl

use strict;
use warnings;

use DBI;

my $TEST_ROWS;
BEGIN {
    $TEST_ROWS = $ENV{TEST_ROWS} || 1024;
};

use Test::More;

BEGIN {
    unless ($ENV{BIG_TESTS}) {
        plan skip_all => 'set BIG_TESTS=1 to run theese tests';
    } else {
        plan tests => $TEST_ROWS + 3;
    }
};

my $dbh = DBI->connect(
    $ENV{DBI} || 'dbi:mysql:database=test;host=localhost',
    $ENV{DBI_USER} || 'root',
    $ENV{DBI_PASSWORD} || '',
) or die 'connection failed:';

ok($dbh->do('drop table if exists q4m_t'));
ok($dbh->do('create table q4m_t (s mediumtext) engine=queue'));

my $row = '0123456789abcdef' x 64 x 1024; # 1MB

for (my $i = 0; $i < $TEST_ROWS; $i++) {
    ok($dbh->do("insert into q4m_t (s) values ('$row')"));
}

is_deeply(
    $dbh->selectall_arrayref("select count(*) from q4m_t where s!='$row'"),
    [ [ 0 ] ],
);
