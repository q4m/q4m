#! /usr/bin/perl

use strict;
use warnings;

use DBI;
use List::Util qw/min/;

my $TEST_ROWS;
BEGIN {
    $TEST_ROWS = $ENV{TEST_ROWS} || 20480;
};

use Test::More tests => int(($TEST_ROWS + 99) / 100) + $TEST_ROWS * 2 + 3;

my $dbh = DBI->connect(
    $ENV{DBI} || 'dbi:mysql:database=test;host=localhost',
    $ENV{DBI_USER} || 'root',
    $ENV{DBI_PASSWORD} || '',
) or die 'connection failed:';

ok(
    $dbh->do('drop table if exists q4m_t'),
    'drop table',
);
ok(
    $dbh->do('create table q4m_t (id int not null,prio tinyint not null,body longtext not null) engine=queue default charset=utf8'),
    'create table',
);

my $body = '0123456789abcdef' x 255;
my $id = 0;

sub do_insert {
    if ($id < $TEST_ROWS) {
        ok(
            $dbh->do(
                'insert into q4m_t (id,prio,body) values '
                    . join(
                        ',',
                        map {
                            "($_," . ($_ % 10 < 9 ? '0' : '1') . ",'$body')"
                        } $id .. min($id + 99, $TEST_ROWS - 1),
                    ),
            ),
            'insert',
        );
        $id += 100;
    }
}

my $zero_cnt = $TEST_ROWS - int($TEST_ROWS / 10);
my $id_expected = 0;
for (my $i = 0; $i < $zero_cnt; $i++) {
    if ($i % 90 == 0) {
        do_insert();
    }
    is_deeply(
        $dbh->selectall_arrayref(
            "select queue_wait('q4m_t:prio=0','q4m_t:prio=1',1)",
        ),
        [ [ 1 ] ],
        'queue_wait (prio=0)',
    );
    is_deeply(
        $dbh->selectall_arrayref('select id,prio from q4m_t'),
        [ [ $id_expected, 0 ] ],
        'queue select (prio=0)',
    );
    if (++$id_expected % 10 == 9) {
        $id_expected++;
    }
}
$id_expected = 9;
for (my $i = $zero_cnt; $i < $TEST_ROWS; $i++) {
    is_deeply(
        $dbh->selectall_arrayref(
            "select queue_wait('q4m_t:prio=0','q4m_t:prio=1',1)",
        ),
        [ [ 2 ] ],
        'queue_wait (prio=1)',
    );
    is_deeply(
        $dbh->selectall_arrayref('select id,prio from q4m_t'),
        [ [ $id_expected, 1 ] ],
        'queue select (prio=1)',
    );
    $id_expected += 10;
}
is_deeply(
    $dbh->selectall_arrayref('select queue_end()'),
    [ [ 1 ] ],
    'queue_end',
);

