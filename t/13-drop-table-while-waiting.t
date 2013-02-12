#! /usr/bin/perl

use strict;
use warnings;

use DBI;
use Test::More tests => 6;

sub dbi_connect {
    DBI->connect(
        $ENV{DBI} || 'dbi:mysql:database=test;host=localhost',
        $ENV{DBI_USER} || 'root',
        $ENV{DBI_PASSWORD} || '',
    ) or die 'connection failed:';
}

# try dropping the table while the owner waiting for a record
if (my $pid = fork) {
    # parent process = subscriber
    my $dbh = dbi_connect();
    ok $dbh->do('drop table if exists q4m_t');
    note("parent: start create table");
    ok $dbh->do('create table q4m_t (v int not null) engine=queue');
    my $owner_pid = $pid;
    note("parent: waiting for child process to start queue_wait()");
    sleep 2;  # waiting for child process to start queue_wait()
    ok $dbh->do('drop table q4m_t');
    is_deeply(
        $dbh->selectall_arrayref(q{show tables like '%q4m_t%'}),
        [],
    );
    is($dbh->do(q{select * from q4m_t}), undef);
    note("parent: waiting for child process to finish");
    waitpid($owner_pid, 0);
    ok(($?>>8) == 0);
    done_testing;
} else {
    # child process = owner
    note("child: waiting for parent process to create table");
    sleep 1;  # waiting for parent process to create table
    my $owner = dbi_connect();
    note("child: start queue_wait");
    my $res = $owner->selectall_arrayref(q{select queue_wait('q4m_t', 5)});
    $res->[0][0] == 0 or die 'Fail at l.' . __LINE__;
    note("child: q4m_t should be removed by parent process after 5 sec wait");
    $res = $owner->selectall_arrayref(q{show tables like '%q4m_t%'}),
    @$res == 0 or die 'Fail at l.' . __LINE__;
    $owner->do("select queue_end()");
}
