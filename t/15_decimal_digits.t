#!/usr/bin/perl

use strict;
use warnings;
no warnings 'uninitialized';

$| = 1;

use lib './lib';
use t::lib::Test;
use Test::More tests => 4;

my $dbr = setup_schema_ok('indextest', version => 2);

my $fld = $dbr->connect('dbrconf')->dbr_fields->where( name => 'dec' )->next;
ok $fld, 'field with digits found in scan';
is $fld->max_value, 5, 'field length correct';
is $fld->decimal_digits, 2, 'field digits correct';
