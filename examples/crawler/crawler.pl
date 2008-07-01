#! /usr/bin/perl

use strict;
use warnings;

use BSD::Resource;
use DBI;
use Filter::SQL;
use Getopt::Long;
use LWP::UserAgent;
use Parallel::Prefork;

# setup
my $dbi = 'dbi:mysql:test;user=root;mysql_enable_utf8=1';
my $reqs_per_child = 100;
my $retry_interval = 5;
my $max_retries = 5;
my $num_workers = 10;
my $benchmark_mode = undef;
my $ua = LWP::UserAgent->new;
my $_dbh;

GetOptions(
    'dbi:s'            => \$dbi,
    'reqs-per-child:i' => \$reqs_per_child,
    'retry-interval:i' => \$retry_interval,
    'max-retries:i'    => \$max_retries,
    'num-workers:i'    => \$num_workers,
    'benchmark'        => \$benchmark_mode,
);

# test if we can connect to db (as well as preloading before fork)
connect_db();
Filter::SQL->dbh->disconnect;

my $pm = Parallel::Prefork->new({
    max_workers  => $num_workers,
    trap_signals => {
        TERM => 'TERM',
        USR1 => undef,
    },
});

# start first $max_retries processes for rescheduling failed requests
my @resched_pids;
for (my $i = 1; $i < $max_retries; $i++) {
    my $pid = fork;
    die 'fork error' unless defined $pid;
    unless ($pid) {
        # in child process
        connect_db();
        run_rescheduler($i);
        exit 0;
    }
    push @resched_pids, $pid;
}

# main loop
until ($pm->signal_received) {
    $pm->start and next;
    # in child process
    connect_db();
    run();
    $pm->finish;
}

# kill all reschedulers, and wait for children
foreach my $p (@resched_pids) {
    kill 'TERM', $p;
    while ($p != waitpid($p, 0)) {
    }
}
$pm->wait_all_children;


# actual work
sub run {
    # set priority
    setpriority(PRIO_PROCESS, 0, 10);
    # loop while we have uris in queue
    my $loop_cnt = 0;
    while ($loop_cnt < $reqs_per_child) {
        # load a url to fetch
        my ($id, $fail_cnt) =
            SELECT ROW id,fail_cnt FROM crawler_queue
                WHERE queue_wait('crawler_queue');
            or next;
        my ($url) = SELECT ROW url FROM url WHERE id=$id;
            or next;
        # fetch url
        my $response = $ua->get($url);
        printf "%-5d %s %d %s\n", $$, scalar localtime, $response->code, $url;
        if ($response->is_success) {
            # success, store the result
            UPDATE url SET title={$response->title || undef} WHERE id=$id;;
        } elsif ($fail_cnt < $max_retries) {
            # schedule for retry
            INSERT INTO crawler_reschedule_queue (id,fail_cnt,at)
                VALUES ($id,{$fail_cnt+1},{time});;
        }
        $loop_cnt++;
    }
    SELECT queue_end();;
    0;
}

# move entry in resheduler_queue to queue
sub run_rescheduler {
    my ($fail_cnt) = @_;
    my $interval = $retry_interval * 2 ** $fail_cnt;
    
    while (1) {
        my ($id, $fail_cnt, $at) =
            SELECT ROW id,fail_cnt,at FROM crawler_reschedule_queue
                WHERE
                    queue_wait("crawler_reschedule_queue:fail_cnt=$fail_cnt");
            or next;
        while (my $sleep_secs = $at + $interval - time > 0) {
            sleep $sleep_secs;
        }
        INSERT INTO crawler_queue (id,fail_cnt) VALUES ($id,$fail_cnt);;
    }
}

# connect to db and set handle in Filter::SQL
sub connect_db {
    Filter::SQL->dbh(DBI->connect($dbi))
            or die DBI->errstr;
}
