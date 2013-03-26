use strict;
use warnings;

use DBI;
use Test::More;
use Test::mysqld;

BEGIN {
    unless ($ENV{MYSQL_DIR}) {
        plan skip_all => 'set MYSQL_DIR to run these tests';
    } else {
        plan tests => 30;
    }
    plan skip_all => 'Test::mysqld >= 0.17 is required to run these tests'
        unless $Test::mysqld::VERSION >= 0.17;
};

sub get_path_of {
    my ($name, @subdirs) = @_;
    for my $subdir (@subdirs) {
        my $path = "$ENV{MYSQL_DIR}/$subdir/$name";
        return $path
            if -x $path;
    }
    die "could not find $name under $ENV{MYSQL_DIR}";
}

my $mysqld = Test::mysqld->new(
    my_cnf => {
        'skip-networking' => '',
    },
    mysql_install_db => get_path_of(qw(mysql_install_db bin scripts)),
    mysqld           => get_path_of(qw(mysqld libexec bin)),
    copy_data_from   => 't/16-upgrade',
) or die $Test::mysqld::errstr;

my $dbh = DBI->connect($mysqld->dsn(dbname => 'test'))
    or die $DBI::errstr;
$dbh->do(q(INSTALL PLUGIN queue SONAME 'libqueue_engine.so'))
    or die $dbh->errstr;

my @tables = qw(closed096 closed097 closed098 crashed096 crashed097 crashed098);

# check that all data can be read
for my $tbl (@tables) {
    is_deeply(
        $dbh->selectall_arrayref(
            "select id from $tbl",
        ),
        [ [ 456 ] ],
        $tbl,
    );
}

# insert 789 to each table and them remove 456
for my $tbl (@tables) {
    ok $dbh->do("insert into $tbl values (789)"), "insert into $tbl";
    ok $dbh->do("delete from $tbl where id=456"), "delete from $tbl";
    is_deeply(
        $dbh->selectall_arrayref(
            "select id from $tbl",
        ),
        [ [ 789 ] ],
    );
}

# restart
$mysqld->stop;
$mysqld->start;
$dbh = DBI->connect($mysqld->dsn(dbname => 'test'))
    or die $DBI::errstr;

# check that all data can be read
for my $tbl (@tables) {
    is_deeply(
        $dbh->selectall_arrayref(
            "select id from $tbl",
        ),
        [ [ 789 ] ],
    );
}

# diag $mysqld->read_log;
