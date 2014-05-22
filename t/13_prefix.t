#!/usr/bin/perl

use strict;

$| = 1;

use lib './lib';
use t::lib::Test;
use Test::More tests => 6;
use DBR::Config::Scope;
use Test::Exception;

my $dbr = setup_schema_ok( 'sorttest', version => 2 );

my $dbinfo = $dbr->connect('dbrconf')->select( -table => 'dbr_instances', -fields => 'instance_id schema_id class dbname username password host dbfile module handle readonly tag' )->[0];

ok($dbinfo, 'fetch DB config info');

my $r = $dbr->connect('dbrconf')->insert( -table => 'dbr_instances', -fields => { schema_id => ['d',$dbinfo->{schema_id}], map(($_, $dbinfo->{$_}), qw'class dbfile module'), handle => 'newsch', prefix => 'pfx__' } );
ok($r, 'save DB config info');

my $dbri = $dbr->get_instance( 'newsch' );
my $dbrh = $dbri->connect;
ok($dbri, 'get handle to new instance');
is $dbri->prefix, 'pfx__', 'prefix readable';

$dbri->connect('conn')->do('CREATE TABLE pfx__abc ( id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, a VARCHAR(255) NOT NULL, b VARCHAR(255) NOT NULL );');
$dbri->connect('conn')->do(q{INSERT INTO pfx__abc VALUES (1,'a', 'b');});


is ($dbrh->abc->all->count, 1, 'can use new table');

1;
