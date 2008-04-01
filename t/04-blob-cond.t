#! /usr/bin/perl

use strict;
use warnings;

use Data::Compare;
use DBI;

use Test::More tests => 22011;

sub dbi_connect {
    DBI->connect(
        $ENV{DBI} || 'dbi:mysql:database=test;host=localhost',
        $ENV{DBI_USER} || 'root',
        $ENV{DBI_PASSWORD} || '',
    ) or die 'connection failed:';
}

# large data with compaction
my $dbh = dbi_connect;
my $dbh2 = dbi_connect;

ok($dbh->do('drop table if exists q4m_t'));
# locate int field after a variable length field
ok($dbh->do('create table q4m_t (data mediumblob,id int not null) engine=queue'));
my $data16k = '0123456789abcdef' x 1024;

ok($dbh->do('insert into q4m_t (id,data) values (?,?)', {}, -1, $data16k));
is_deeply(
    $dbh->selectall_arrayref('select queue_wait("q4m_t",1)'),
    [ [ 1 ] ],
);
is_deeply(
    $dbh->selectall_arrayref('select queue_rowid()'),
    [ [ 1 ] ],
);
is_deeply(
    $dbh->selectall_arrayref('select queue_abort()'),
    [ [ 1 ] ],
);

for (my $i = 0; $i < 2000; $i++) {
    ok($dbh->do(
        'insert into q4m_t (id,data) values (?,?)',
        {},
        $i,
        substr($data16k, $i),
    ));
    is_deeply(
        $dbh->selectall_arrayref('select id,data from q4m_t'),
        [ [ -1, $data16k ], [ $i, substr($data16k, $i) ] ],
    );
    is_deeply(
        $dbh->selectall_arrayref("select queue_wait('q4m_t:id>=0',1)"),
        [ [ 1 ] ],
    );
    is_deeply(
        $dbh->selectall_arrayref('select id,data from q4m_t'),
        [ [ $i, substr($data16k, $i) ] ],
    );
    is_deeply(
        $dbh->selectall_arrayref('select queue_rowid()'),
        [ [ $i + 2 ] ],
    );
    is_deeply(
        $dbh->selectall_arrayref("select queue_end()"),
        [ [ 1 ] ],
    );
}
is_deeply(
    $dbh->selectall_arrayref("select queue_wait('q4m_t:id>=0',1)"),
    [ [ 0 ] ],
);
is_deeply(
    $dbh->selectall_arrayref("select queue_wait('q4m_t:id>=-1',1)"),
    [ [ 1 ] ],
);
is_deeply(
    $dbh->selectall_arrayref('select id,data from q4m_t'),
    [ [ -1, $data16k ] ],
);
is_deeply(
    $dbh->selectall_arrayref('select queue_rowid()'),
    [ [ 1 ] ],
);
is_deeply(
    $dbh->selectall_arrayref("select queue_end()"),
    [ [ 1 ] ],
);
for (my $i = 2000; $i < 4000; $i++) {
    ok($dbh->do(
        'insert into q4m_t (id,data) values (?,?)',
        {},
        $i,
        substr($data16k, $i),
    ));
    is_deeply(
        $dbh->selectall_arrayref("select queue_wait('q4m_t:id>=0',1)"),
        [ [ 1 ] ],
    );
    is_deeply(
        $dbh->selectall_arrayref('select id,data from q4m_t'),
        [ [ $i, substr($data16k, $i) ] ],
    );
    is_deeply(
        $dbh->selectall_arrayref('select queue_rowid()'),
        [ [ $i + 2 ] ],
    );
    is_deeply(
        $dbh->selectall_arrayref("select queue_end()"),
        [ [ 1 ] ],
    );
}
