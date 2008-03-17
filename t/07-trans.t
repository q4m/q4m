#! /usr/bin/perl

use strict;
use warnings;

use DBI;

use Test::More tests => 32;

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

# calling queue_rowid while NOT in owner mode should return an error
{
    local $dbh->{PrintError} = undef;
    local $dbh->{PrintWarn} = undef;
    my $r = $dbh->selectall_arrayref('select queue_rowid()');
    isnt(ref $r, 'ARRAY');
}

# transmit first row
is_deeply(
    $dbh->selectall_arrayref('select queue_wait("q4m_t")'),
    [ [ 1 ] ],
);
my $r = $dbh->selectall_arrayref('select queue_rowid()');
is(ref $r, 'ARRAY');
is(ref $r->[0], 'ARRAY');
my $row_id = $r->[0][0];
$r = $dbh->selectall_arrayref('select * from q4m_t');
is_deeply(
    $dbh->selectall_arrayref("select queue_set_srcid(0,'a',$row_id)"),
    [ [ 1 ] ],
);
ok($dbh->do("insert into q4m_t2 values ($r->[0][0])"));
is_deeply(
    $dbh2->selectall_arrayref("select * from q4m_t2"),
    [ [ 1 ] ],
);

# test if duplicates are ignored
is_deeply(
    $dbh->selectall_arrayref("select queue_set_srcid(0,'a',$row_id)"),
    [ [ 1 ] ],
);
ok($dbh->do("insert into q4m_t2 values ($r->[0][0])"));
is_deeply(
    $dbh2->selectall_arrayref("select * from q4m_t2"),
    [ [ 1 ] ],
);

# transmit next row
is_deeply(
    $dbh->selectall_arrayref('select queue_wait("q4m_t")'),
    [ [ 1 ] ],
);
$r = $dbh->selectall_arrayref('select queue_rowid()');
is(ref $r, 'ARRAY');
is(ref $r->[0], 'ARRAY');
$row_id = $r->[0][0];
$r = $dbh->selectall_arrayref('select * from q4m_t');
is_deeply(
    $dbh->selectall_arrayref("select queue_set_srcid(0,'a',$row_id)"),
    [ [ 1 ] ],
);
ok($dbh->do("insert into q4m_t2 values ($r->[0][0])"));
is_deeply(
    $dbh2->selectall_arrayref("select * from q4m_t2"),
    [ [ 1 ], [ 2 ] ],
);

# check if we can re-insert the same value using a different source_id
is_deeply(
    $dbh->selectall_arrayref("select queue_set_srcid(1,'a',$row_id)"),
    [ [ 1 ] ],
);
ok($dbh->do("insert into q4m_t2 values ($r->[0][0])"));
is_deeply(
    $dbh2->selectall_arrayref("select * from q4m_t2"),
    [ [ 1 ], [ 2 ], [ 2 ] ],
);

# check if we can re-insert the same value by using "w" mode
is_deeply(
    $dbh->selectall_arrayref("select queue_set_srcid(1,'w',$row_id)"),
    [ [ 1 ] ],
);
ok($dbh->do("insert into q4m_t2 values ($r->[0][0])"));
is_deeply(
    $dbh2->selectall_arrayref("select * from q4m_t2"),
    [ [ 1 ], [ 2 ], [ 2 ], [ 2 ] ],
);
is_deeply(
    $dbh->selectall_arrayref("select queue_set_srcid(1,'a',$row_id)"),
    [ [ 1 ] ],
);
ok($dbh->do("insert into q4m_t2 values ($r->[0][0])"));
is_deeply(
    $dbh2->selectall_arrayref("select * from q4m_t2"),
    [ [ 1 ], [ 2 ], [ 2 ], [ 2 ] ],
);

# sanity check of source_id
is_deeply(
    $dbh->selectall_arrayref("select queue_set_srcid(-1,'a',$row_id)"),
    [ [ undef ] ],
);
is_deeply(
    $dbh->selectall_arrayref("select queue_set_srcid(64,'a',$row_id)"),
    [ [ undef ] ],
);
