#!/usr/bin/perl

use strict;
use warnings;

use t::lib::Test;
use Test::More;
use Try::Tiny;

my $dbr = setup_schema_ok('empty', exceptions => 1);

my $inst_1 = $dbr->get_instance('empty');
my $inst_2 = DBR::Config::Instance::Anon->new( map( ($_ => $inst_1->$_), qw' username host password dbname dbfile module prefix ' ) );
$inst_2->{prefix} .= 'pfx__';

my @battery = (

    {
        section => 'basic validation',
    },
    {
        title => 'empty',
        cmd => { },
        invalid => 'instance',
    },
    {
        title => 'instance only',
        cmd => { instance => $inst_1 },
        invalid => 'from_name or to_name',
    },
    {
        title => 'instance + from_name',
        cmd => { instance => $inst_1, from_name => 'bob' },
        valid => 1,
    },
    {
        title => 'instance + to_name',
        cmd => { instance => $inst_1, to_name => 'bob' },
        valid => 1,
    },
    {
        title => 'instance + from_name + to_name',
        cmd => { instance => $inst_1, from_name => 'bob', to_name => 'bob' },
        valid => 1,
    },
    {
        title => 'extraneous field',
        cmd => { instance => $inst_1, from_name => 'bob', to_name => 'bob', flurgh => 1 },
        invalid => 'flurgh',
    },

    {
        section => 'validation with columnspecs',
    },
    {
        title => 'instance + from_name + to_name + columnspec(from,to)',
        cmd => { instance => $inst_1, from_name => 'bob', to_name => 'bob', columns => [ { from_name => 'x', from_type => '', to_name => 'y', to_name => '' } ] },
        valid => 1,
    },
    {
        title => 'instance + from_name + to_name + columnspec(from)',
        cmd => { instance => $inst_1, from_name => 'bob', to_name => 'bob', columns => [ { from_name => 'x', from_type => '' } ] },
        valid => 1,
    },
    {
        title => 'instance + from_name + to_name + columnspec(to)',
        cmd => { instance => $inst_1, from_name => 'bob', to_name => 'bob', columns => [ { to_name => 'x', to_type => '' } ] },
        valid => 1,
    },
    {
        title => 'instance + from_name + to_name + columnspec(empty)',
        cmd => { instance => $inst_1, from_name => 'bob', to_name => 'bob', columns => [ { } ] },
        invalid => 'to or from',
    },
    {
        title => 'instance + from_name + to_name + columnspec(to/type missing)',
        cmd => { instance => $inst_1, from_name => 'bob', to_name => 'bob', columns => [ { to_name => 'x' } ] },
        invalid => 'type',
    },
    {
        title => 'instance + from_name + to_name + columnspec(from/type missing)',
        cmd => { instance => $inst_1, from_name => 'bob', to_name => 'bob', columns => [ { from_name => 'x' } ] },
        invalid => 'type',
    },
    {
        title => 'instance + from_name + to_name + columnspec(to/name missing)',
        cmd => { instance => $inst_1, from_name => 'bob', to_name => 'bob', columns => [ { to_type => 'x' } ] },
        invalid => 'name',
    },
    {
        title => 'instance + from_name + to_name + columnspec(from/type missing)',
        cmd => { instance => $inst_1, from_name => 'bob', to_name => 'bob', columns => [ { from_type => 'x' } ] },
        invalid => 'name',
    },
    {
        title => 'instance + from_name + to_name + columnspec(extraneous)',
        cmd => { instance => $inst_1, from_name => 'bob', to_name => 'bob', columns => [ { to_name => 'x', to_type => '', blargh => 1 } ] },
        invalid => 'blargh',
    },
    {
        title => 'instance + from_name + olumnspec(from)',
        cmd => { instance => $inst_1, from_name => 'bob', columns => [ { from_name => 'x', from_type => '' } ] },
        valid => 1,
    },
    {
        title => 'instance + to_name + olumnspec(to)',
        cmd => { instance => $inst_1, to_name => 'bob', columns => [ { to_name => 'x', to_type => '' } ] },
        valid => 1,
    },
    {
        title => 'instance + to_name + olumnspec(to)',
        cmd => { instance => $inst_1, to_name => 'bob', columns => [ { from_name => 'x', from_type => '' } ] },
        invalid => 'drop',
    },
    {
        title => 'instance + to_name + olumnspec(to)',
        cmd => { instance => $inst_1, to_name => 'bob', columns => [ { to_name => 'x', to_type => '' } ] },
        invalid => 'drop',
    },
    {
        title => 'instance + from_name + olumnspec(from,dup)',
        cmd => { instance => $inst_1, from_name => 'bob', columns => [ { from_name => 'x', from_type => '1' }, { from_name => 'x', from_type => '2' } ] },
        invalid => 'duplicate',
    },
    {
        title => 'instance + from_name + olumnspec(from,non-dup)',
        cmd => { instance => $inst_1, from_name => 'bob', columns => [ { from_name => 'x', from_type => '1' }, { from_name => 'y', from_type => '2' } ] },
        valid => 1,
    },
    {
        title => 'instance + to_name + olumnspec(to,dup)',
        cmd => { instance => $inst_1, to_name => 'bob', columns => [ { to_name => 'x', to_type => '1' }, { to_name => 'x', to_type => '2' } ] },
        invalid => 'duplicate',
    },
    {
        title => 'instance + to_name + olumnspec(to,non-dup)',
        cmd => { instance => $inst_1, to_name => 'bob', columns => [ { to_name => 'x', to_type => '1' }, { to_name => 'y', to_type => '2' } ] },
        valid => 1,
    },

    {
        section => 'validation with indexes (TODO)',
    },

    {
        section => 'validation with foreign keys (TODO)',
    },

);

for my $test (@battery) {
    if ($test->{section}) {
        diag $test->{section};
        next;
    }

    my ($op, $err);

    try {
        $op = DBR::Migration::Operations::Alter->new(%{$test->{cmd}});
    } catch {
        $err = $_; chomp $err;
    };

    if ($test->{invalid}) {
        like $err, qr/$test->{invalid}/, "$test->{title} - invalid [$test->{invalid}]";
        next;
    }

    ok $op, "$test->{title} - validates" or diag $err;

    if (!$op) {
        # TODO generate skips
        next;
    }
}

done_testing;
