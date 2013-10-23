#! /usr/bin/env perl

use strict;
use warnings;

use DBI;
use POSIX qw/:sys_wait_h/;

use Test::More tests => 253;

sub dbi_uri {
    my $uri = $ENV{DBI} || 'dbi:mysql:database=test;host=localhost';
    $uri .= ';user=' . ($ENV{DBI_USER} || 'root');
    $uri .= ';password=' . ($ENV{DBI_PASSWORD} || '');
}

sub dbi_connect {
    DBI->connect(
        $ENV{DBI} || 'dbi:mysql:database=test;host=localhost',
        $ENV{DBI_USER} || 'root',
        $ENV{DBI_PASSWORD} || '',
    ) or die 'connection failed:';
}

my $src = dbi_connect();
my $dst = dbi_connect();

ok($src->do('drop table if exists q4m_t'));
ok($src->do('create table q4m_t (v int not null,s mediumtext not null) engine=queue'));
ok($src->do('drop table if exists q4m_t2'));
ok($src->do('create table q4m_t2 (s mediumtext not null,v int not null) engine=queue'));

# spawn q4m-forward
my $dbi_uri = $ENV{DBI} || 'dbi:mysql:database=test;host=localhost';
$dbi_uri .= ';user=' . ($ENV{DBI_USER} || 'root');
$dbi_uri .= ';password=' . ($ENV{DBI_PASSWORD} || '');
my $fwd_pid = fork();
die "fork failed" unless defined $fwd_pid;
unless ($fwd_pid) {
    exec(
        'support-files/q4m-forward',
        "$dbi_uri;table=q4m_t",
        "$dbi_uri;table=q4m_t2",
    );
    die "failed to spawn q4m-forward\n";
}
sleep 1;
is(waitpid($fwd_pid, WNOHANG), 0);

# simple transfer tests
is_deeply(
    $dst->selectall_arrayref('select queue_wait("q4m_t2",1)'),
    [ [ 0 ] ],
);
ok($src->do('insert into q4m_t values (10,"foobar")'));
is_deeply(
    $dst->selectall_arrayref('select queue_wait("q4m_t2",10)'),
    [ [ 1 ] ],
);
is_deeply(
    $dst->selectall_arrayref('select * from q4m_t2'),
    [ [ 'foobar', 10 ] ],
);
is_deeply(
    $src->selectall_arrayref('select * from q4m_t'),
    [],
);
is_deeply(
    $dst->selectall_arrayref('select queue_end()'),
    [ [ 1 ] ],
);
is_deeply(
    $src->selectall_arrayref('select * from q4m_t'),
    [],
);
is_deeply(
    $dst->selectall_arrayref('select * from q4m_t2'),
    [],
);

# compaction test
my $s = '0123456789abcdef' x 128 x 255;

for (my $i = 0; $i < 80; $i++) {
    ok($src->do('insert into q4m_t values (?,?)', {}, $i, $s));
    is_deeply(
        $dst->selectall_arrayref('select queue_wait("q4m_t2",10)'),
        [ [ 1 ] ],
    );
    is_deeply(
        $dst->selectall_arrayref('select * from q4m_t2'),
        [ [ $s, $i ] ],
    );
}

kill 15, $fwd_pid;
waitpid($fwd_pid, 0);


