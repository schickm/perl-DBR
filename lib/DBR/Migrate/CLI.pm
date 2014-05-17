package DBR::Migrate::CLI;

use strict;
use warnings;
use Getopt::Long qw( GetOptionsFromArray );
use List::Util qw( max );

my %commands;
my %opt;
my @args;

sub load {
}

$commands{load} = {
    func  => \&load,
    short => 'Loads migrations from a directory into DB',
    opts  => [
        ['migration-dir=s' => 'Overrides migration directory setting in conf file'],
        ['common-prefix' => 'Roll back to the common prefix before applying any new migrations'],
        ['non-atomic'    => 'Stop as soon as possible on apply errors'],
    ],
    long  => <<LD ,
This command will set the state of your database to the state described by a
set of migration files in a directory.  Migrations currently active but not
found in the directory will be rolled back.
LD
};

sub instantiate {
}

$commands{instantiate} = {
    func  => \&instantiate,
    short => 'Creates a schema instance',
    opts  => [
        ['schema=s' => 'Handle of schema to use'],
        ['class=s' => '"class" token for new instance'],
        ['tag=s' => '"tag" token for new instance'],
        ['module=s' => 'Database module (Mysql, SQLite, Pg)'],
        ['dbfile=s' => 'Database file name (SQLite)'],
        ['host=s' => 'Database connection host'],
        ['username=s' => 'Database username'],
        ['password=s' => 'Database password'],
        ['dbname=s' => 'Database schema name'],
    ],
    long  => <<LD ,
Given connection information for an empty database you have just created, this
command will attach said database to DBR and populate it with tables
corresponding to a named schema.  It will be kept up to date with future schema
changes.
LD
};

my @global_opt = (
    ['config-file|f=s', 'Path to DBR config file'],
    ['help|?', 'Displays this message'],
);

sub columns {
    my (@cols) = @_;
    for my $c (@cols) {
        my $l = max 0, map { length } @$c;
        map { $_ .= " " x ($l - length($_)) } @$c;
    }
    my $out = '';
    while (@{$cols[0]}) {
        $out .= join("   ", map { shift @$_ } @cols) . "\n";
    }
    $out;
}

sub usage {
    my ($err, $subcmd) = @_;

    my $b = '';

    if ($subcmd) {
        my $i = $commands{$subcmd};
        $b .= "usage: $0 $subcmd [options]" . ($i->{args} ? " $i->{args}" : "") . "\n\noptions:\n";

        my (@left, @right);

        for my $oi (@global_opt, @{$i->{opts}||[]}) {
            my ($opt, $desc) = @$oi;
            my $arg = $opt =~ /=/;
            $opt =~ s/[=:!+].*//;
            my $short = $opt =~ s/\|(.)// ? $1 : '';

            push @left, ($short ? "  -$short " : "     ") . "--$opt" . ($arg ? "=X" : "");
            push @right, $desc;
        }

        $b .= columns(\@left, \@right);
        $b .= "\n$i->{long}";
    }
    else {
        $b .= "usage: $0 [subcommand] [options]\n\nsubcommands:\n";
        my (@left, @right);
        for (sort keys %commands) {
            push @left, "  $_";
            push @right, $commands{$_}{short};
        }
        $b .= columns(\@left, \@right);
    }

    print { $err ? \*STDERR : \*STDOUT } $b;
    exit($err);
}

sub run {
    my (@tmp_args, %tmp_opts);
    Getopt::Long::Configure('gnu_getopt');

    @tmp_args = @ARGV;
    GetOptionsFromArray(\@tmp_args, \%tmp_opts, map { $_->[0] } @global_opt, map { @{$_->{opts}||[]} } values %commands) or usage(1,'');

    my $subcommand = shift(@tmp_args);
    if (!defined($subcommand)) {
        usage(!$tmp_opts{'help'}, '');
    }

    my $i = $commands{$subcommand} or usage(1,'');
    usage(0, $subcommand) if $tmp_opts{'help'};

    @args = @ARGV;
    GetOptionsFromArray(\@args, \%opt, map { $_->[0] } @global_opt, @{$i->{opts}||[]}) or usage(1,$subcommand);

    $i->{func}->();
}

1;
