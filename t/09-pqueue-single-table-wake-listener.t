#! /usr/bin/env perl

use strict;
use warnings;
use DBI;

use Test::More tests => 11;

sub dbi_connect {
    DBI->connect(
        $ENV{DBI} || 'dbi:mysql:database=test;host=localhost',
        $ENV{DBI_USER} || 'root',
        $ENV{DBI_PASSWORD} || '',
    ) or die 'connection failed:';
}

my $dbh = dbi_connect();

ok(
    $dbh->do('drop table if exists q4m_t'),
    'drop table',
);
ok(
    $dbh->do('create table q4m_t (id int not null,prio tinyint not null) engine=queue default charset=utf8'),
    'create table',
);

$dbh->disconnect();

unless (fork) {
    sleep 2;
    $dbh = dbi_connect();
    $dbh->do('insert into q4m_t values (1,1),(2,0),(3,1)');
    exit 0;
}

$dbh = dbi_connect();

is_deeply(
    $dbh->selectall_arrayref(
        q{select queue_wait('q4m_t:prio=0','q4m_t:prio=1',1)},
    ),
    [ [ 0 ] ],
    'timeout',
);

is_deeply(
    $dbh->selectall_arrayref(
        q{select queue_wait('q4m_t:prio=0','q4m_t:prio=1',10)},
    ),
    [ [ 1 ] ],
);
is_deeply(
    $dbh->selectall_arrayref(
        q{select * from q4m_t},
    ),
    [ [ 2, 0 ] ],
);
is_deeply(
    $dbh->selectall_arrayref(
        q{select queue_wait('q4m_t:prio=0','q4m_t:prio=1',10)},
    ),
    [ [ 2 ] ],
);
is_deeply(
    $dbh->selectall_arrayref(
        q{select * from q4m_t},
    ),
    [ [ 1, 1 ] ],
);
is_deeply(
    $dbh->selectall_arrayref(
        q{select queue_wait('q4m_t:prio=0','q4m_t:prio=1',10)},
    ),
    [ [ 2 ] ],
);
is_deeply(
    $dbh->selectall_arrayref(
        q{select * from q4m_t},
    ),
    [ [ 3, 1 ] ],
);

is_deeply(
    $dbh->selectall_arrayref(
        q{select queue_end()},
    ),
    [ [ 1 ] ],
);

is_deeply(
    $dbh->selectall_arrayref(
        q{select count(*) from q4m_t},
    ),
    [ [ 0 ] ],
);
