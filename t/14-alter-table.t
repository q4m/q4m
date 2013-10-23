#! /usr/bin/env perl

use strict;
use warnings;

use DBI;
use Test::More tests => 14;

sub dbi_connect {
    DBI->connect(
        $ENV{DBI} || 'dbi:mysql:database=test;host=localhost',
        $ENV{DBI_USER} || 'root',
        $ENV{DBI_PASSWORD} || '',
    ) or die 'connection failed:';
}

my $dbh = dbi_connect();

ok $dbh->do('drop table if exists q4m_t');

# try altering the table while owning a row
ok $dbh->do('create table q4m_t (v1 int not null, v2 int not null) engine=queue');
my $owner = dbi_connect();
ok $dbh->do("insert into q4m_t values (1,1), (10,10)");
is_deeply(
    $owner->selectall_arrayref(q{select queue_wait('q4m_t')}),
    [ [ 1 ] ],
);
is_deeply(
    $owner->selectall_arrayref(q{select * from q4m_t}),
    [ [ 1, 1 ] ],
);
is_deeply(
    $dbh->selectall_arrayref(q{select * from q4m_t}),
    [ [ 10, 10 ] ],
);
ok $dbh->do('alter table q4m_t add v3 int');
ok $dbh->do("insert into q4m_t values (20,20,20)");
is_deeply(
    $dbh->selectall_arrayref(q{select * from q4m_t}),
    [ [ 10, 10, undef ], [ 20, 20, 20 ] ],
);
is_deeply(
    $owner->selectall_arrayref(q{select * from q4m_t}),
    [ [ 1, 1, undef ] ],
);
ok $dbh->do('alter table q4m_t drop v1');
ok $owner->do("insert into q4m_t values (30,30)");  # how about insert by owner?
is_deeply(
    $dbh->selectall_arrayref(q{select * from q4m_t}),
    [ [ 10, undef ], [ 20, 20 ], [ 30,30 ] ],
);
is_deeply(
    $owner->selectall_arrayref(q{select * from q4m_t}),
    [ [ 1, undef ] ],
);
