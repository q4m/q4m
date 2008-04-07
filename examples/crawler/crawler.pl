#! /usr/bin/perl

use strict;
use warnings;

use BSD::Resource;
use DBI;
use Getopt::Long;
use LWP::UserAgent;
use Parallel::Prefork;

# setup
my $dbi = 'dbi:mysql:test;user=root;mysql_enable_utf8=1';
my $reqs_per_child = 100;
my $retry_interval = 1;
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

sub dbh () {
    return $_dbh if $_dbh;
    $_dbh = DBI->connect($dbi)
         or die DBI->errstr;
    $_dbh;
}

sub dbh_disconnect {
    $_dbh->disconnect if $_dbh;
    $_dbh = undef;
}

# test if we can connect to db (as well as preloading before fork)
dbh();
dbh_disconnect();

my $pm = Parallel::Prefork->new({
    max_workers  => $num_workers,
    trap_signals => {
        TERM => 'TERM',
        USR1 => undef,
    },
});
until ($pm->signal_received) {
    $pm->start and next;
    # in child process
    run();
    $pm->finish;
}
$pm->wait_all_children;

# actual work
sub run {
    # set priority
    setpriority(PRIO_PROCESS, 0, 10);
    # loop while we have uris in queue
    for (my $c = 0; $c < $reqs_per_child; $c++) {
        # load a url to fetch
        my ($id, $url, $fail_cnt) = load_url();
        # fetch url
        my $response = $ua->get($url);
        printf "%-5d %s %d %s\n", $$, scalar localtime, $response->code, $url;
        if ($response->is_success) {
            # success, store the result
            dbh->do(
                'update url set title=? where id=?',
                {},
                $response->title || undef, $id,
            ) or die dbh->errstr;
        } elsif ($fail_cnt < $max_retries) {
            # schedule for retry
            dbh->do(
                'insert into crawler_queue (id,fail_cnt,failed_at) values (?,?,?)',
                {},
                $id, $fail_cnt + 1, int(time / 60),
            ) or die dbh->errstr;
        }
    }
    dbh->selectrow_array("select queue_end()")
        or die dbh->errstr;
    0;
}

# loads a url to fetch
sub load_url {
    while (1) {
        my @w = dbh->selectrow_array(sprintf(
            "select queue_wait('crawler_queue:failed_at+%d*pow(2,fail_cnt)-1<=%d',%d)",
            $retry_interval,
            int(time / 60),
            $benchmark_mode ? 0 : 60,
        )) or die dbh->errstr;
        unless ($w[0]) {
            if ($benchmark_mode) {
                kill 'USR1', $pm->manager_pid;
                exit 0;
            }
            next;
        }
        my $rows = dbh->selectall_arrayref(
            'select url.id,url.url,fail_cnt from url natural join crawler_queue',
        ) or die dbh->errstr;
        next unless @$rows;
        return @{$rows->[0]};
    }
}
