#! /usr/bin/perl

use strict;
use warnings;

use DBI;
use Test::More tests => 15;

sub dbi_connect {
    DBI->connect(
        $ENV{DBI} || 'dbi:mysql:database=test;host=localhost',
        $ENV{DBI_USER} || 'root',
        $ENV{DBI_PASSWORD} || '',
    ) or die 'connection failed:';
}

my $dbh = dbi_connect();

ok $dbh->do('drop table if exists q4m_t');

# try deleting the table contents while a row is begin owned
ok $dbh->do('create table q4m_t (v1 int not null) engine=queue');
my $owner = dbi_connect();
ok $dbh->do("insert into q4m_t values (2), (20)");
is_deeply(
    $owner->selectall_arrayref(q{select queue_wait('q4m_t')}),
    [ [ 1 ] ],
);
is_deeply(
    $owner->selectall_arrayref(q{select * from q4m_t}),
    [ [ 2 ] ],
);
is_deeply(
    $dbh->selectall_arrayref(q{select * from q4m_t}),
    [ [ 20 ] ],
);
ok $dbh->do('delete from q4m_t');
is_deeply(
    $dbh->selectall_arrayref(q{select * from q4m_t}),
    [ ],
);
is_deeply(
    $owner->selectall_arrayref(q{select * from q4m_t}),
    [ [ 2 ] ],
);
ok $dbh->do("insert into q4m_t values (30)");
is_deeply(
    $owner->selectall_arrayref(q{select * from q4m_t}),
    [ [ 2 ] ],
);
is_deeply(
    $dbh->selectall_arrayref(q{select * from q4m_t}),
    [ [ 30 ] ],
);
ok $owner->do('delete from q4m_t');
is_deeply(
    $owner->selectall_arrayref(q{select * from q4m_t}),
    [  ],
);
is_deeply(
    $dbh->selectall_arrayref(q{select * from q4m_t}),
    [ [ 30 ] ],
);
