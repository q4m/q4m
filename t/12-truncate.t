#! /usr/bin/env perl

use strict;
use warnings;

use DBI;
use Test::More tests => 72;

sub dbi_connect {
    DBI->connect(
        $ENV{DBI} || 'dbi:mysql:database=test;host=localhost',
        $ENV{DBI_USER} || 'root',
        $ENV{DBI_PASSWORD} || '',
    ) or die 'connection failed:';
}

my $dbh = dbi_connect();

ok $dbh->do('drop table if exists q4m_t');
ok $dbh->do('create table q4m_t (v int not null) engine=queue');

# first, with no owner connection
ok $dbh->do('insert into q4m_t values (3)');
is_deeply $dbh->selectall_arrayref('select * from q4m_t'), [ [ 3 ] ];
ok $dbh->do('truncate q4m_t');
is_deeply $dbh->selectall_arrayref('select * from q4m_t'), [];

# try truncating the table while owning a row
for my $n (11..16) {
    my $owner = dbi_connect();
    ok $dbh->do('delete from q4m_t');
    ok $dbh->do("insert into q4m_t values ($n),($n*10)");
    is_deeply(
        $owner->selectall_arrayref(q{select queue_wait('q4m_t')}),
        [ [ 1 ] ],
    );
    is_deeply(
        $owner->selectall_arrayref(q{select * from q4m_t}),
        [ [ $n ] ],
    );
    is_deeply(
        $dbh->selectall_arrayref(q{select * from q4m_t}),
        [ [ $n * 10 ] ],
    );
    ok $dbh->do('truncate q4m_t');
    ok $dbh->do("insert into q4m_t values ($n*20)");
    is_deeply(
        $owner->selectall_arrayref(q{select * from q4m_t}),
        [],
    );
    ok $owner->do($n %2 ? q{select queue_end()} : q{select queue_abort()});
    is_deeply(
        $owner->selectall_arrayref(q{select * from q4m_t}),
        [ [ $n * 20 ] ],
    );
    is_deeply(
        $dbh->selectall_arrayref(q{select * from q4m_t}),
        [ [ $n * 20 ] ],
    );
}
