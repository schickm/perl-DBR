#!/usr/bin/perl

use strict;
use warnings;
no warnings 'uninitialized';

$| = 1;

use lib './lib';
use t::lib::Test;
use Test::More tests => 4;

# As always, it's important that the sample database is not tampered with, otherwise our tests will fail
my $dbr = setup_schema_ok('music');

my $dbh = $dbr->connect('dbrconf');
ok($dbh, 'dbr connect');

is($dbh->enum->where( handle => 'earbleed' )->next->name, 'My Ears are Bleeding', 'can fetch v2 on dbrconf');

is($dbh->dbr_fields->where( name => 'rating' )->next->table->name, 'album', 'relationships work on dbrconf');
