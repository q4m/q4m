#! /usr/bin/perl -w

my @TARGETS;
foreach my $t (@ARGV) {
    $t =~ /^mysql-([0-9\.-]+(?:-rc|)-.*?)-(with.*)$/
        or die "failed to parse: $t\n";
    push @TARGETS, {
        dist_suffix => $2,
        mysql_bin   => "$t-bin",
        mysql_src   => "$t-src",
        ver_arch    => $1,
    };
}
my $HOME = $ENV{MBUILD_ROOT} || `pwd`;
chomp $HOME;
my $Q4M_ROOT = $ENV{Q4M_ROOT} || '../q4m';
my $EXTRA_FLAGS = $ENV{MBUILD_EXTRA_FLAGS} || '';

sub task {
    my $cmd = shift;
    print "$cmd\n";
    system("$cmd");
    if ($? == 0) {
        # ok
    } elsif ($? == -1) {
        die "failed to execute ($!): $cmd\n";
    } elsif ($? & 127) {
        die 'child died with signal ', ($? & 127), "\n";
    } else {
        exit ($? >> 8);
    }
}

sub read_makefile_params {
    my %params;
    open my $fh, '<', "$Q4M_ROOT/Makefile"
        or die "failed to open $Q4M_ROOT/Makefile:";
    while (my $line = <$fh>) {
        chomp $line;
        if ($line =~ /(\S+)\s*=\s*(.*)\s*$/) {
            $params{$1} = $2;
        }
    }
    close $fh;
    \%params;
}

sub stop_mysqld {
    my $args = shift;
    if (-e "$HOME/$args->{mysql_bin}/tmp/mysql.sock") {
        task("kill -TERM `cat $HOME/$args->{mysql_bin}/tmp/mysqld.pid`");
        while (-e "$HOME/$args->{mysql_bin}/tmp/mysql.sock") {
            sleep 1;
        }
    }
}

sub build_and_test {
    my $args = shift;
    my $mf_params = read_makefile_params();
    my $bindistdir = "mysql-$args->{ver_arch}-$args->{dist_suffix}-q4m-$mf_params->{VERSION}";
    task("cd $Q4M_ROOT && ./configure --prefix=$HOME/$args->{mysql_bin} --with-mysql=$HOME/$args->{mysql_src} $EXTRA_FLAGS"); # disable for now BINARY_DIST_DIR=$bindistdir");
    task("cd $Q4M_ROOT && make -e clean") unless $ENV{MBUILD_NOCLEAN};
    task("cd $Q4M_ROOT && make -e all install");
    task("cd $HOME/$args->{mysql_bin} && bin/mysqld_safe --defaults-file=etc/my.cnf &");
    while (! -e "$HOME/$args->{mysql_bin}/tmp/mysql.sock") {
        sleep 1;
    }
    system("cd $Q4M_ROOT && $HOME/$args->{mysql_bin}/bin/mysql -u root -f mysql -S $HOME/$args->{mysql_bin}/tmp/mysql.sock < support-files/install.sql"); # ignore error
    task("cd $Q4M_ROOT && DBI='dbi:mysql:test;mysql_socket=$HOME/$args->{mysql_bin}/tmp/mysql.sock' make -e test") unless $ENV{MBUILD_NOTEST};
    stop_mysqld($args);
    task("cd $Q4M_ROOT && make -e binary-dist");
    task("mv $Q4M_ROOT/q4m-$mf_params->{VERSION}-$mf_params->{BINARY_ARCH}.tar.gz $HOME/$bindistdir.tar.gz");
}

# main
foreach my $target (@TARGETS) {
    stop_mysqld($target);
}
foreach my $target (@TARGETS) {
    build_and_test($target);
}
