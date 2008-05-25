#! /usr/bin/perl

use strict;
use warnings;

use Time::HiRes qw/time/;
use Test::More tests => 4;

use DBI;
use List::MoreUtils qw/uniq/;

my $NUM_CHILDREN = $ENV{CONCURRENCY} || 32;
my $NUM_MESSAGES = $ENV{MESSAGES} || ($NUM_CHILDREN * 200);
my $BLOCK_SIZE = 100;

$ENV{DBI} ||= 'dbi:mysql:test;host=localhost';
$ENV{DBI_USER} ||= 'root';
$ENV{DBI_PASSWORD} ||= '';

sub dbi_connect {
    DBI->connect($ENV{DBI}, $ENV{DBI_USER}, $ENV{DBI_PASSWORD})
        or die 'connection failed:';
}

# create tables (q4m_t2 used to notify consumers to start operation)
my $dbh = dbi_connect();
$dbh->do('drop table if exists q4m_t')
    or die $dbh->errstr;
$dbh->do('drop table if exists q4m_t2')
    or die $dbh->errstr;
$dbh->do(
    'create table q4m_t (v int not null'
        . ($ENV{VAR_LENGTH} ? ',s longtext not null' : '')
            . ') engine=queue',
) or die $dbh->errstr;
$dbh->do(
    'create table q4m_t2 (v int not null) engine=queue',
) or die $dbh->errstr;
$dbh->disconnect;

# parse DBI string to be passed to C version of reader
$ENV{MYSQL_USER} = $ENV{DBI_USER};
$ENV{MYSQL_PASSWORD} = $ENV{DBI_PASSWORD};
$ENV{DBI} =~ /^dbi:mysql:(.*)$/i;
my @params = split ';', $1;
unless ($params[0] =~ /=/) {
    $ENV{MYSQL_DB} = shift @params;
}
foreach my $p (@params) {
    if ($p =~ /=/) {
        my ($n, $v) = ($`, $');
        $n = "MYSQL_$n"
            unless $n =~ /^mysql_/i;
        $ENV{uc $n} = $v;
    }
}

# fork subscribers
my @children;
for (my $i = 0; $i < $NUM_CHILDREN; $i++) {
    if (my $pid = fork) {
        push @children, $pid;
    } else {
        my $loop = $NUM_MESSAGES / $NUM_CHILDREN;
        open STDOUT, '>', "$$.log" or die $!;
        # use C version if exists
        if ($ENV{USE_C_CLIENT}) {
            exec('t/05-multireader-read', $loop)
        }
        # use perl version
        $dbh = dbi_connect();
        while (1) {
            my @w = $dbh->selectrow_array("select queue_wait('q4m_t2')")
                or die $dbh->errstr;
            last if $w[0];
        }
        for (my $j = 0; $j < $loop; $j++) {
            while (1) {
                my @w = $dbh->selectrow_array("select queue_wait('q4m_t')")
                    or die $dbh->errstr;
                last if $w[0];
                print STDERR "queue_wait timeout\n";
            }
            my $a = $dbh->selectall_arrayref("select * from q4m_t")
                or die $dbh->errstr;
            print "$a->[0]->[0]\n";
        }
        $dbh->do("select queue_end()");
        exit 0;
    }
}

sub blob_str {
    return '' unless $ENV{VAR_LENGTH};
    q(,') . ('z' x $ENV{VAR_LENGTH}) . q(');
}

# start adding messages
$dbh = dbi_connect();
my $sth = $dbh->prepare(
    'insert into q4m_t values '
        . join(',',
               map {
                   '(?' . ($ENV{VAR_LENGTH} ? ',?' : '') . ')'
               } (1..$BLOCK_SIZE),
           ),
) or die $dbh->errstr;
for (my $i = 0; $i < $NUM_MESSAGES; $i += $BLOCK_SIZE) {
    $sth->execute(
        $ENV{VAR_LENGTH}
            ? map { ($i + $_, 'z' x $ENV{VAR_LENGTH}) } (1..$BLOCK_SIZE)
                : map { $i + $_ } (1..$BLOCK_SIZE),
    ) or die $dbh->errstr;
}

my $start = time;

# notify all clients to start
$dbh->do(
    'insert into q4m_t2 values '
        . join (',', map { '(1)' } (1..$NUM_CHILDREN)),
) or die $dbh->errstr;

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
    unlink "$pid.log"
        unless $ENV{Q4M_TEST_PRESERVE_LOG};
}
@recvs = sort { $a <=> $b } uniq @recvs;

is(scalar @recvs, $NUM_MESSAGES, 'check number of messages');
is($recvs[0], 1, 'min value of received message');
is($recvs[-1], $NUM_MESSAGES, 'max value of received message');
is($dbh->selectrow_array('select count(*) from q4m_t'), 0, 'should have no rows in table');

print STDERR "\n\nMultireader benchmark result:\n";
printf STDERR "    Number of messages: %d\n", $NUM_MESSAGES;
printf STDERR "    Number of readers:  %d\n", $NUM_CHILDREN;
printf STDERR "    Elapsed:            %.3f seconds\n", $elapsed;
printf STDERR "    Throughput:         %.3f mess./sec.\n",
    $NUM_MESSAGES / $elapsed;
print STDERR "\n";
