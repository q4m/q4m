#! /usr/bin/perl

use strict;
use warnings;

use Time::HiRes qw/time/;
use Test::More tests => 4;

use DBI;
use List::MoreUtils qw/uniq/;

my $NUM_CHILDREN = $ENV{CONCURRENCY} || 32;
my $NUM_MESSAGES = $NUM_CHILDREN * 200;
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
            my @w = $dbh->selectrow_array("select queue_wait('test.q4m_t')")
                or die $dbh->errstr;
	    next unless $w[0];
            my $a = $dbh->selectall_arrayref("select * from q4m_t")
                or die $dbh->errstr;
            print $logfh "$a->[0]->[0]\n";
        }
        $dbh->do("select queue_end()");
        exit 0;
    }
}

my $start = time;

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

my $elapsed = time - $start;

# check all logs
my @recvs;
foreach my $pid (@children) {
    open my $logfh, '<', "$pid.log" or die $!;
    while (my $l = <$logfh>) {
        chomp $l;
        push @recvs, $l;
    }
    close $logfh;
    unlink "$pid.log";
}
@recvs = sort { $a <=> $b } uniq @recvs;

is(scalar @recvs, $NUM_MESSAGES);
is($recvs[0], 1);
is($recvs[-1], $NUM_MESSAGES);
is($dbh->selectrow_array('select count(*) from q4m_t'), 0);

print STDERR "\n\nMultireader benchmark result:\n";
printf STDERR "    Number of messages: %d\n", $NUM_MESSAGES;
printf STDERR "    Number of readers:  %d\n", $NUM_CHILDREN;
printf STDERR "    Elapsed:            %.3f seconds\n", $elapsed;
printf STDERR "    Throughput:         %.3f mess./sec.\n",
    $NUM_MESSAGES / $elapsed;
print STDERR "\n";
