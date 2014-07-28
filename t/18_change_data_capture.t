#!/usr/bin/perl

use strict;
use warnings;
no warnings 'uninitialized';

$| = 1;

use lib './lib';
use t::lib::Test;
use Test::More tests => 41;
use Test::Exception;
use Test::Deep;

# As always, it's important that the sample database is not tampered with, otherwise our tests will fail
my $dbr = setup_schema_ok('cdc');
$dbr->{session}->{use_exceptions} = 1;

my $dbh = $dbr->connect('cdc');
ok($dbh, 'dbr connect');

## UID storage

$dbh->_session->user_id(42);
is($dbh->_session->user_id, 42, 'DBR session can store user IDs');

## Table identification (user code is not supposed to touch this)

throws_ok { $dbh->cdc_badthing->{table}->cdc_type } qr/unrecognized CDC table/;
throws_ok { $dbh->tbl_cdcbadcol->{table}->cdc_type } qr/non-logged.*has cdc_ columns/;
lives_and { cmp_deeply($dbh->tbl_normal->{table}->cdc_type, {}) } 'normal table with no funnybusiness';
throws_ok { $dbh->badver->{table}->cdc_type } qr/bad cdc_row_version field/;
throws_ok { $dbh->unknowncdc->{table}->cdc_type } qr/unknown cdc_ field/;
throws_ok { $dbh->badstart->{table}->cdc_type } qr/bad cdc_start_time field/;
throws_ok { $dbh->badend->{table}->cdc_type } qr/bad cdc_end_time field/;
throws_ok { $dbh->badlogrowver1->{table}->cdc_type } qr/cdc_row_version in log but not table/;
throws_ok { $dbh->badlogrowver2->{table}->cdc_type } qr/cdc_row_version mismatched type between log and table/;
throws_ok { $dbh->unknowncdc2->{table}->cdc_type } qr/unknown cdc_ field in log/;
throws_ok { $dbh->spurious1->{table}->cdc_type } qr/foo in log but not table/;
throws_ok { $dbh->spurious2->{table}->cdc_type } qr/foo mismatched types between log and table/;
throws_ok { $dbh->missing_stime->{table}->cdc_type } qr/log must contain cdc_start_time/;
throws_ok { $dbh->missing_suser->{table}->cdc_type } qr/log must contain cdc_start_user/;
throws_ok { $dbh->missing_ver->{table}->cdc_type } qr/log must contain cdc_row_version if table does/;
throws_ok { $dbh->missing_etime1->{table}->cdc_type } qr/log must contain cdc_end_time if cdc_end_user or cdc_row_version/;
throws_ok { $dbh->missing_etime2->{table}->cdc_type } qr/log must contain cdc_end_time if cdc_end_user or cdc_row_version/;
throws_ok { $dbh->missing_other->{table}->cdc_type } qr/log must contain all fields from table, missing bar/;
throws_ok { $dbh->spurious_etime->{table}->cdc_type } qr/log may only contain cdc_end_time if cdc_end_user or cdc_row_version/;
lives_and { cmp_deeply($dbh->good_c->{table}->cdc_type, {logged => 1, log_table => ignore(), has_version => '', update_ok => '', delete_ok => ''}) } 'good table with log, no update/delete';
lives_and { cmp_deeply($dbh->good_cu->{table}->cdc_type, {logged => 1, log_table => ignore(), has_version => 1, update_ok => 1, delete_ok => ''}) } 'good table with log, update';
lives_and { cmp_deeply($dbh->good_cd->{table}->cdc_type, {logged => 1, log_table => ignore(), has_version => '', update_ok => '', delete_ok => 1}) } 'good table with log, delete';
lives_and { cmp_deeply($dbh->good_cud->{table}->cdc_type, {logged => 1, log_table => ignore(), has_version => 1, update_ok => 1, delete_ok => 1}) } 'good table with log, update+delete';

## Log record capture

my @shipments;
$dbh->_session->cdc_log_shipping_sub( sub { push @shipments, [@_] } );

my %good_c_comm = (user_id => 42, itag => '', ihandle => 'cdc', table => 'good_c');
my %mfcud_comm = (user_id => 42, itag => '', ihandle => 'cdc', table => 'multifield_cud');

@shipments = ();
$dbh->good_c->insert( foo => undef );
$dbh->good_c->insert( foo => 'ABCDEF' );
cmp_deeply(\@shipments, [
    [ { %good_c_comm, over => 0, old => undef, new => { id => ignore(), foo => undef } } ],
    [ { %good_c_comm, over => 0, old => undef, new => { id => ignore(), foo => 'ABCDEF' } } ],
], 'basic insert change records; 2 transactions');

@shipments = ();
$dbh->begin;
my $id_1 = $dbh->multifield_cud->insert( foo => 3, bar => 4 );
my $id_2 = $dbh->multifield_cud->insert( foo => 3, enm => 'ccc' );
cmp_deeply(\@shipments, [], 'no logging until post-commit');
$dbh->commit;
cmp_deeply(\@shipments, [
    [
        { %mfcud_comm, over => 0, old => undef, new => { id => ignore(), cdc_row_version => 1, foo => 3, bar => 4, enm => 2 } },
        { %mfcud_comm, over => 0, old => undef, new => { id => ignore(), cdc_row_version => 1, foo => 3, bar => undef, enm => 3 } },
    ]
], 'insert change records grouped within a transaction, values defaulted to undef, translators applied');

@shipments = ();
$dbh->begin;
$dbh->multifield_cud->insert( foo => 3, bar => 4 );
$dbh->multifield_cud->insert( foo => 3, enm => 'ccc' );
$dbh->rollback;
cmp_deeply(\@shipments, [], 'no logging after rollback');

my $r_1 = $dbh->multifield_cud->get( $id_1 );
my $r_2 = $dbh->multifield_cud->get( $id_1 );
$r_1->foo; $r_2->foo;

@shipments = ();
$r_1->set( foo => 5 );
cmp_deeply(\@shipments, [[ { %mfcud_comm, over => 1,
    old => { id => $id_1, cdc_row_version => 1, foo => 3, bar => 4, enm => 2 },
    new => { id => $id_1, cdc_row_version => 2, foo => 5, bar => 4, enm => 2 },
}]], 'update capture');

@shipments = ();
$r_2->set( foo => 6 );
cmp_deeply(\@shipments, [[ { %mfcud_comm, over => 2,
    old => { id => $id_1, cdc_row_version => 2, foo => 5, bar => 4, enm => 2 },
    new => { id => $id_1, cdc_row_version => 3, foo => 6, bar => 4, enm => 2 },
}]], 'update capture not fooled by concurrent update');

@shipments = ();
$r_2->enm( 'aaa' );
cmp_deeply(\@shipments, [[ { %mfcud_comm, over => 3,
    old => { id => $id_1, cdc_row_version => 3, foo => 6, bar => 4, enm => 2 },
    new => { id => $id_1, cdc_row_version => 4, foo => 6, bar => 4, enm => 1 },
}]], 'update works for simple set, translators');

@shipments = ();
$r_2->delete;
cmp_deeply(\@shipments, [[ { %mfcud_comm, over => 4,
    old => { id => $id_1, cdc_row_version => 4, foo => 6, bar => 4, enm => 1 },
    new => undef,
}]], 'delete capture');

@shipments = ();
$r_1->foo(2);
$r_1->delete;
cmp_deeply(\@shipments, [], 'concurrent delete, no log records');

throws_ok {
    my $dbh2 = $dbh->{instance}->connect;
    $dbh2->begin;
    my $r_2 = $dbh2->multifield_cud->get($id_2);
    $r_2->set( foo => $r_2->foo+1 ) for 1 .. 257;
} qr/cdc_row_version/;

## Integrity checking

throws_ok { $dbh->cdc_log_multifield_cud->insert() } qr/invalid insert into CDC log/;
throws_ok { $dbh->multifield_cud->insert( cdc_row_version => 1 ) } qr/system field/;
throws_ok { $dbh->multifield_cud->get($id_2)->set( cdc_row_version => 2 ) } qr/readonly/;
throws_ok { $dbh->good_c->get(1)->set( foo => 'BAR' ) } qr/this table is not logged for updates/;
throws_ok { $dbh->good_c->get(1)->delete } qr/this table is not logged for deletes/;
