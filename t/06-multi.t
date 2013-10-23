#! /usr/bin/env perl

use strict;
use warnings;

use DBI;

use Test::More tests => 29;

sub dbi_connect {
    DBI->connect(
        $ENV{DBI} || 'dbi:mysql:database=test;host=localhost',
        $ENV{DBI_USER} || 'root',
        $ENV{DBI_PASSWORD} || '',
    ) or die 'connection failed:';
}

my $dbh = dbi_connect();
ok($dbh->do('drop table if exists q4m_t'));
ok($dbh->do('create table q4m_t (v int not null) engine=queue'));
ok($dbh->do('drop table if exists q4m_t2'));
ok($dbh->do('create table q4m_t2 (v int not null) engine=queue'));

is_deeply(
    $dbh->selectall_arrayref("select queue_wait('q4m_t','q4m_t2',1)"),
    [ [ 0 ] ],
);
ok($dbh->do("insert into q4m_t (v) values (1)"));
is_deeply(
    $dbh->selectall_arrayref("select queue_wait('q4m_t','q4m_t2',5)"),
    [ [ 1 ] ],
);
ok($dbh->do("insert into q4m_t values (2)"));
ok($dbh->do("insert into q4m_t2 values (3),(4)"));
is_deeply(
    $dbh->selectall_arrayref("select * from q4m_t"),
    [ [ 1 ] ],
);
is_deeply(
    $dbh->selectall_arrayref("select * from q4m_t2"),
    [],
);

is_deeply(
    $dbh->selectall_arrayref("select queue_wait('q4m_t','q4m_t2',5)"),
    [ [ 1 ] ],
);
is_deeply(
    $dbh->selectall_arrayref("select * from q4m_t"),
    [ [ 2 ] ],
);
is_deeply(
    $dbh->selectall_arrayref("select * from q4m_t2"),
    [],
);

is_deeply(
    $dbh->selectall_arrayref("select queue_wait('q4m_t','q4m_t2',5)"),
    [ [ 2 ] ],
);
is_deeply(
    $dbh->selectall_arrayref("select * from q4m_t"),
    [],
);
is_deeply(
    $dbh->selectall_arrayref("select * from q4m_t2"),
    [ [ 3 ] ],
);

is_deeply(
    $dbh->selectall_arrayref("select queue_wait('q4m_t','q4m_t2',5)"),
    [ [ 2 ] ],
);
is_deeply(
    $dbh->selectall_arrayref("select * from q4m_t"),
    [],
);
is_deeply(
    $dbh->selectall_arrayref("select * from q4m_t2"),
    [ [ 4 ] ],
);

is_deeply(
    $dbh->selectall_arrayref("select queue_wait('q4m_t','q4m_t2',1)"),
    [ [ 0 ] ],
);

# test waiting
$dbh->disconnect;
unless (fork) {
    sleep 5;
    $dbh = dbi_connect();
    $dbh->do("insert into q4m_t values (2)");
    exit;
}
$dbh = dbi_connect();
is_deeply(
    $dbh->selectall_arrayref("select queue_wait('q4m_t','q4m_t2',10)"),
    [ [ 1 ] ],
);
is_deeply(
    $dbh->selectall_arrayref("select * from q4m_t"),
    [ [ 2 ] ],
);
is_deeply(
    $dbh->selectall_arrayref("select * from q4m_t2"),
    [],
);
ok($dbh->do("select queue_end()"));

$dbh->disconnect;
unless (fork) {
    sleep 5;
    $dbh = dbi_connect();
    $dbh->do("insert into q4m_t2 values (2)");
    exit;
}
$dbh = dbi_connect();
is_deeply(
    $dbh->selectall_arrayref("select queue_wait('q4m_t','q4m_t2',10)"),
    [ [ 2 ] ],
);
is_deeply(
    $dbh->selectall_arrayref("select * from q4m_t"),
    [],
);
is_deeply(
    $dbh->selectall_arrayref("select * from q4m_t2"),
    [ [ 2 ] ],
);
ok($dbh->do("select queue_end()"));
