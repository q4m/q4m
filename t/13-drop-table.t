#! /usr/bin/env perl

use strict;
use warnings;

use DBI;
use Test::More tests => 10;

sub dbi_connect {
    DBI->connect(
        $ENV{DBI} || 'dbi:mysql:database=test;host=localhost',
        $ENV{DBI_USER} || 'root',
        $ENV{DBI_PASSWORD} || '',
    ) or die 'connection failed:';
}

my $dbh = dbi_connect();

ok $dbh->do('drop table if exists q4m_t');

# try dropping the table while owning a row
ok $dbh->do('create table q4m_t (v int not null) engine=queue');
my $owner = dbi_connect();
ok $dbh->do("insert into q4m_t values (7),(70)");
is_deeply(
    $owner->selectall_arrayref(q{select queue_wait('q4m_t')}),
    [ [ 1 ] ],
);
is_deeply(
    $owner->selectall_arrayref(q{select * from q4m_t}),
    [ [ 7 ] ],
);
is_deeply(
    $dbh->selectall_arrayref(q{select * from q4m_t}),
    [ [ 70 ] ],
);
ok $dbh->do('drop table q4m_t');
is_deeply(
    $dbh->selectall_arrayref(q{show tables like 'q4m_t'}),
    [],
);
is($dbh->do(q{select * from q4m_t}), undef);
is($owner->do(q{select * from q4m_t}), undef);
