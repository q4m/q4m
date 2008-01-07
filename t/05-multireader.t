#! /usr/bin/perl

use strict;
use warnings;

use Test::More tests => 3;

use DBI;
use List::MoreUtils qw/uniq/;

my $NUM_CHILDREN = 32;
my $NUM_MESSAGES = $NUM_CHILDREN * 100;
my $BLOCK_SIZE = 100;

sub dbi_connect {
    DBI->connect(
        $ENV{DBI} || 'dbi:mysql:database=test;host=localhost',
        $ENV{DBI_USER} || 'root',
        $ENV{DBI_PASSWORD} || '',
    ) or die 'connection failed:';
}

# create table
my $dbh = dbi_connect();
$dbh->do('drop table if exists q4m_t')
    or die $dbh->errstr;
$dbh->do('create table q4m_t (v int not null) engine=queue')
    or die $dbh->errstr;
$dbh->disconnect;

# fork subscribers
my @children;
for (my $i = 0; $i < $NUM_CHILDREN; $i++) {
    if (my $pid = fork) {
        push @children, $pid;
    } else {
        open my $logfh, '>', "$$.log" or die $!;
        $dbh = dbi_connect();
        for (my $j = 0; $j < $NUM_MESSAGES / $NUM_CHILDREN; $j++) {
            $dbh->do("select queue_wait('test.q4m_t')")
                or die $dbh->errstr;
            my $a = $dbh->selectall_arrayref("select * from q4m_t")
                or die $dbh->errstr;
            print $logfh "$a->[0]->[0]\n";
        }
        $dbh->do("select queue_end()");
        exit 0;
    }
}

# start adding messages
$dbh = dbi_connect();
for (my $i = 0; $i < $NUM_MESSAGES; $i += $BLOCK_SIZE) {
    $dbh->do(
        'insert into q4m_t (v) values '
            . join(',', map { '(' . ($i + $_) . ')' } (1..$BLOCK_SIZE)))
        or die $dbh->errstr;
}

# wait until all subscribers stop
for (my $i = 0; $i < $NUM_CHILDREN; $i++) {
    until (waitpid(-1, 0) > 0) {
    }
}

# check all logs
my @recvs;
foreach my $pid (@children) {
    open my $logfh, '<', "$pid.log" or die $!;
    while (my $l = <$logfh>) {
        chomp $l;
        push @recvs, $l;
    }
}
@recvs = sort { $a <=> $b } uniq @recvs;

is(scalar @recvs, $NUM_MESSAGES);
is($recvs[0], 1);
is($recvs[-1], $NUM_MESSAGES);
