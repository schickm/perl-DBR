#!/usr/bin/perl

use strict;
use warnings;
no warnings 'uninitialized';

$| = 1;

use lib './lib';
use t::lib::Test;
use Test::More tests => 12;

my $dbr = setup_schema_ok('indextest', version => 2);

my $dbh = $dbr->connect('dbrconf');
ok($dbh, 'dbr connect');

is($dbh->indexes->all->count, 9, 'found all indexes and no extras');

my $ix_abc_a = $dbh->indexes->where( 'field.name' => 'a', refinement_of_id => undef, prefix_length => undef, is_unique => 0 )->next;
ok ($ix_abc_a, 'found index on abc(a)');
my $ix_abc_b = $dbh->indexes->where( 'field.name' => 'b', refinement_of_id => undef, prefix_length => undef, is_unique => 1 )->next;
ok ($ix_abc_b, 'found index on abc(b)');

my $My = $dbr->get_instance('indextest')->module eq 'Mysql';
my $ix_def_d   = $dbh->indexes->where( 'field.name' => 'd', refinement_of_id => undef, prefix_length => undef, is_unique => 0 )->next;
ok $ix_def_d, 'found prefix index on def(d)';
my $ix_def_g   = $dbh->indexes->where( 'field.name' => 'g', refinement_of_id => undef, prefix_length => undef, is_unique => 0 )->next;
ok $ix_def_g, 'found prefix index on def(g)';
my $ix_def_de  = $dbh->indexes->where( 'field.name' => 'e', refinement_of_id => $ix_def_d->id, prefix_length => undef, is_unique => 1 )->next;
ok $ix_def_de, 'found unique index on def(de)';
my $ix_def_gf  = $dbh->indexes->where( 'field.name' => 'f', refinement_of_id => $ix_def_g->id, prefix_length => $My ? 8 : undef, is_unique => 0 )->next;
ok $ix_def_gf, 'found index on def(g,f(undef))';
my $ix_def_gh  = $dbh->indexes->where( 'field.name' => 'h', refinement_of_id => $ix_def_g->id, prefix_length => undef, is_unique => 0 )->next;
ok $ix_def_gh, 'found index on def(g,h(undef))';
my $ix_def_gd  = $dbh->indexes->where( 'field.name' => 'd', refinement_of_id => $ix_def_g->id, prefix_length => undef, is_unique => 0 )->next;
ok $ix_def_gd, 'found prefix index on def(gd)';
my $ix_def_gde = $dbh->indexes->where( 'field.name' => 'e', refinement_of_id => $ix_def_gd->id, prefix_length => undef, is_unique => 0 )->next;
ok $ix_def_gd, 'found index on def(gde)';
