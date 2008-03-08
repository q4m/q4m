#! /usr/bin/perl

use strict;
use warnings;

use DBI;

use Test::More tests => 25;

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

my $dbh2 = dbi_connect();

# prepare test data
ok($dbh->do('insert into q4m_t values (1),(2),(3),(4),(5)'));

# transmit
my $r = $dbh->selectall_arrayref('select binary queue_dread("q4m_t")');
is(ref $r, 'ARRAY');
is(ref $r->[0], 'ARRAY');
is(substr($r->[0]->[0], 8), "\xff\1\0\0\0");
is_deeply(
    $dbh->selectall_arrayref('select queue_dwrite("q4m_t2", 0, binary ' . $dbh->quote($r->[0]->[0]) . ')'),
    [ [ 1 ] ],
);
is_deeply(
    $dbh2->selectall_arrayref('select queue_wait("q4m_t2", 1)'),
    [ [ 1 ] ],
);
is_deeply(
    $dbh2->selectall_arrayref('select * from q4m_t2'),
    [ [ 1 ] ],
);

# dup check
is_deeply(
    $dbh->selectall_arrayref('select queue_dwrite("q4m_t2", 0, binary ' . $dbh->quote($r->[0]->[0]) . ')'),
    [ [ 0 ] ],
);
is_deeply(
    $dbh2->selectall_arrayref('select queue_wait("q4m_t2", 1)'),
    [ [ 0 ] ],
);
is_deeply(
    $dbh2->selectall_arrayref('select * from q4m_t2'),
    [],
);

# transmit once more
$r = $dbh->selectall_arrayref('select binary queue_dread("q4m_t")');
is(ref $r, 'ARRAY');
is(ref $r->[0], 'ARRAY');
is(substr($r->[0]->[0], 8), "\xff\2\0\0\0");
is_deeply(
    $dbh->selectall_arrayref('select queue_dwrite("q4m_t2", 0, binary ' . $dbh->quote($r->[0]->[0]) . ')'),
    [ [ 1 ] ],
);
is_deeply(
    $dbh2->selectall_arrayref('select queue_wait("q4m_t2", 1)'),
    [ [ 1 ] ],
);
is_deeply(
    $dbh2->selectall_arrayref('select * from q4m_t2'),
    [ [ 2 ] ],
);

# check if we can re-insert the same value using a different source_id
is_deeply(
    $dbh->selectall_arrayref('select queue_dwrite("q4m_t2", 1, binary ' . $dbh->quote($r->[0]->[0]) . ')'),
    [ [ 1 ] ],
);
is_deeply(
    $dbh2->selectall_arrayref('select queue_wait("q4m_t2", 1)'),
    [ [ 1 ] ],
);
is_deeply(
    $dbh2->selectall_arrayref('select * from q4m_t2'),
    [ [ 2 ] ],
);

# sanity check of source_id
is_deeply(
    $dbh->selectall_arrayref('select queue_dwrite("q4m_t2", 64, binary ' . $dbh->quote($r->[0]->[0]) . ')'),
    [ [ undef ] ],
);
is_deeply(
    $dbh->selectall_arrayref('select queue_dwrite("q4m_t2", -1, binary ' . $dbh->quote($r->[0]->[0]) . ')'),
    [ [ undef ] ],
);
