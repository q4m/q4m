#! /usr/bin/perl

use strict;
use warnings;

use BSD::Resource;
use DBI;
use Getopt::Long;
use LWP::UserAgent;
use POSIX qw(:sys_wait_h);

my $dbi = 'dbi:mysql:test;user=root;mysql_enable_utf8=1';
my $workers = 32;
my $reqs_per_child = 100;
my $max_retries = 5;
my $retry_interval = 15; # in minutes
my $help;

GetOptions(
    'dbi=s'            => \$dbi,
    'workers=i'        => \$workers,
    'reqs-per-child=i' => \$reqs_per_child,
    'max-reties=i'     => \$max_retries,
    'retry-interval=i' => \$retry_interval,
    'help'             => \$help,
) or exit 99;
if ($help) {
    print <<"EOT"
Usage: $0 [options]
Options: --dbi=dbi_uri          DBI-style URI of MySQL database
         --workers=num_workers  number of workers to spawn (or 0 not to spawn)
         --reqs-per-child=num   number of requests to be handled by a worker
                                process before it shuts down
         --max-retries=num      maximum number of retries to fetch a URL upon
                                failures
         --retry-interval=min   interval between retries in minutes
EOT
;
    exit 0;
}

$reqs_per_child = 0 unless $workers;

my $ua = LWP::UserAgent->new;

my $_dbh;

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

# load given url and store the result
sub load_url {
    my ($id, $url) = @_;
    # request url, return if failed
    my $r = $ua->get($url);
    print $r->code, ' ', $url, "\n";
    return if $r->code >= 500;
    # save title
    dbh->do(
        'update url set title=? where id=?',
        {},
        $r->title,
        $id,
    ) or die dbh->errsr;
    # return
    1;
}

# main routine of worker processes
sub run {
    # set priority
    setpriority(PRIO_PROCESS, 0, 10);
    # main loop
    my $reqs_handled = 0;
    while (! $reqs_per_child || $reqs_handled < $reqs_per_child) {
        # poll for a url
        dbh->selectall_arrayref(
            'select queue_wait(?,1)',
            {},
            sprintf(
                'crawler_queue:failed_at+fail_cnt*%d<=%d',
                $retry_interval,
                int(time / 60),
            ),
        ) or die dbh->errstr;
        # obtain url (returns 1 row at max., or 0 if timeout)
        my $rows = dbh->selectall_arrayref(
            'select url.id,url.url,fail_cnt from url natural join crawler_queue',
            { Slice => {} },
        ) or die dbh->errstr;
        # load url, resubmit the request into queue
        foreach my $row (@$rows) {
            if (! load_url($row->{id}, $row->{url})
                    and $row->{fail_cnt} < $max_retries) {
                dbh->do(
                    'insert into crawler_queue (id,fail_cnt,failed_at) values (?,?,?)',
                    {},
                    $row->{id},
                    $row->{fail_cnt} + 1,
                    int(time / 60),
                ) or die dbh->errstr;
            }
            $reqs_handled++;
        }
    }
    dbh->selectrow_array("select queue_end()")
        or die dbh->errstr;
    0;
}

exit run()
    if $workers == 0;

# fork workers
my %pids;
while (1) {
    dbh_disconnect;
    while (scalar(keys %pids) < $workers) {
        my $pid = fork;
        die "fork failed:" unless defined $pid;
        exit run()
            unless $pid;
        $pids{$pid} = 1;
    }
    my $exit_pid = wait;
    while ($exit_pid > 0) {
        delete $pids{$exit_pid};
        $exit_pid = waitpid(-1, WNOHANG);
    }
}
