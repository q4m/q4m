#! /usr/bin/perl

use strict;
use warnings;

use DBI;

use Test::More tests => 5;

my $dbh = DBI->connect(
    $ENV{DBI} || 'dbi:mysql:database=test;host=localhost',
    $ENV{DBI_USER} || 'root',
    $ENV{DBI_PASSWORD} || '',
) or die 'connection failed:';

ok($dbh->do('drop table if exists q4m_t'));
ok($dbh->do('create table q4m_t (v int not null,w int not null) engine=queue'));

ok($dbh->do('insert into q4m_t (v,w) values (1,3),(2,2),(2,1),(3,0)'));
ok($dbh->do('delete from q4m_t where v=2 order by w limit 1'));
is_deeply(
    $dbh->selectall_arrayref('select * from q4m_t'),
    [ [ 1, 3 ], [ 2, 2 ], [ 3, 0 ], ],
);

