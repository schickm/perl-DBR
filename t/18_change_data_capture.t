#!/usr/bin/perl

use strict;
use warnings;
no warnings 'uninitialized';

$| = 1;

use lib './lib';
use t::lib::Test;
use Test::More tests => 212;
use Test::Exception;
use Test::Deep;

# As always, it's important that the sample database is not tampered with, otherwise our tests will fail
my $dbr = setup_schema_ok('cdc');
my $sess = $dbr->_session;
$sess->{use_exceptions} = 1;

my $dbh = $dbr->connect('cdc');
ok($dbh, 'dbr connect');

## UID storage

$sess->user_id(42);
is($sess->user_id, 42, 'DBR session can store user IDs');

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
$sess->cdc_log_shipping_sub( sub { push @shipments, [@_] } );

sub ccmp { return ( user_id => 42, itag => '', ihandle => 'cdc', table => shift(), time => ignore(), old => undef, new => undef ) }
sub cset { return ( user_id => 42, itag => '', ihandle => 'cdc', table => shift(), time => time(), old => undef, new => undef ) }

@shipments = ();
$dbh->good_c->insert( foo => undef );
$dbh->good_c->insert( foo => 'ABCDEF' );
cmp_deeply(\@shipments, [
    [ { ccmp('good_c'), new => { id => ignore(), foo => undef } } ],
    [ { ccmp('good_c'), new => { id => ignore(), foo => 'ABCDEF' } } ],
], 'basic insert change records; 2 transactions');

@shipments = ();
$dbh->begin;
my $id_1 = $dbh->multifield_cud->insert( foo => 3, bar => 4 );
my $id_2 = $dbh->multifield_cud->insert( foo => 3, enm => 'ccc' );
cmp_deeply(\@shipments, [], 'no logging until post-commit');
$dbh->commit;
cmp_deeply(\@shipments, [
    [
        { ccmp('multifield_cud'), new => { id => ignore(), cdc_row_version => 1, foo => 3, bar => 4, enm => 2 } },
        { ccmp('multifield_cud'), new => { id => ignore(), cdc_row_version => 1, foo => 3, bar => undef, enm => 3 } },
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
cmp_deeply(\@shipments, [[ { ccmp('multifield_cud'),
    old => { id => $id_1, cdc_row_version => 1, foo => 3, bar => 4, enm => 2 },
    new => { id => $id_1, cdc_row_version => 2, foo => 5, bar => 4, enm => 2 },
}]], 'update capture');

@shipments = ();
$r_2->set( foo => 6 );
cmp_deeply(\@shipments, [[ { ccmp('multifield_cud'),
    old => { id => $id_1, cdc_row_version => 2, foo => 5, bar => 4, enm => 2 },
    new => { id => $id_1, cdc_row_version => 3, foo => 6, bar => 4, enm => 2 },
}]], 'update capture not fooled by concurrent update');

@shipments = ();
$r_2->enm( 'aaa' );
cmp_deeply(\@shipments, [[ { ccmp('multifield_cud'),
    old => { id => $id_1, cdc_row_version => 3, foo => 6, bar => 4, enm => 2 },
    new => { id => $id_1, cdc_row_version => 4, foo => 6, bar => 4, enm => 1 },
}]], 'update works for simple set, translators');

@shipments = ();
$r_2->delete;
cmp_deeply(\@shipments, [[ { ccmp('multifield_cud'),
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

$dbh->multifield_cud->insert( foo => 9 );
$dbh->multifield_cud->insert( foo => 10 );
$dbh->multifield_cud->insert( foo => 11 );

@shipments = ();
$dbh->multifield_cud->where( foo => [9,10,11] )->set(bar => 7);
cmp_deeply(\@shipments, [
    [
        { ccmp('multifield_cud'),
            old => { id => ignore(), cdc_row_version => 1, foo => 9, bar => undef, enm => 2 },
            new => { id => ignore(), cdc_row_version => 2, foo => 9, bar => 7, enm => 2 },
        },
        { ccmp('multifield_cud'),
            old => { id => ignore(), cdc_row_version => 1, foo => 10, bar => undef, enm => 2 },
            new => { id => ignore(), cdc_row_version => 2, foo => 10, bar => 7, enm => 2 },
        },
        { ccmp('multifield_cud'),
            old => { id => ignore(), cdc_row_version => 1, foo => 11, bar => undef, enm => 2 },
            new => { id => ignore(), cdc_row_version => 2, foo => 11, bar => 7, enm => 2 },
        },
    ]
], '"bulk" update converted into non-bulk for CDC');

@shipments = ();
$dbh->multifield_cud->where( foo => [9,10,11] )->delete_matched_records;
cmp_deeply(\@shipments, [
    [
        { ccmp('multifield_cud'),
            old => { id => ignore(), cdc_row_version => 2, foo => 9, bar => 7, enm => 2 },
        },
        { ccmp('multifield_cud'),
            old => { id => ignore(), cdc_row_version => 2, foo => 10, bar => 7, enm => 2 },
        },
        { ccmp('multifield_cud'),
            old => { id => ignore(), cdc_row_version => 2, foo => 11, bar => 7, enm => 2 },
        },
    ]
], '"bulk" delete converted into non-bulk for CDC');



## Log record storage

sub record_ok {
    my ($table, $query, $data, $what) = @_;
    my $rs = $dbh->$table->where(@$query);
    local $Test::Builder::Level = $Test::Builder::Level + 1;

    if ($data) {
        is($rs->count, 1, "$what: exists uniquely");
        my $r = $rs->next;

        while (my ($k,$v) = splice @$data, 0, 2) {
            if ($r) {
                $v = 'NULL' unless defined $v;
                is(defined($r->$k) ? $r->$k : 'NULL', $v, "$what: $k=$v");
            } else {
                fail("$what: $k");
            }
        }
    } else {
        is($rs->count, 0, "$what: should not exist");
    }
}

lives_ok { $sess->record_change_data({ cset('good_c' ), new => { id => 5, foo => 'XYZ' }, user_id => 11, time => 12345 }) } 'record change record (c)';
record_ok 'cdc_log_good_c', [ id => 5 ], [ foo => 'XYZ', cdc_start_user => 11, cdc_start_time => 12345 ], 'c/after insert/version 1';
lives_ok { $sess->record_change_data({ cset('good_c' ), new => { id => 5, foo => 'XYZ' }, user_id => 11, time => 12345 }) } 'change recording is idempotent';
record_ok 'cdc_log_good_c', [ id => 5 ], [ foo => 'XYZ', cdc_start_user => 11, cdc_start_time => 12345 ], 'c/after redundant insert/version 1';
lives_ok { $sess->record_change_data({ cset('good_cu'), new => { id => 6, foo => 'XYZ', cdc_row_version => 1 }, user_id => 11, time => 12345 }) } 'record change record (cu)';
record_ok 'cdc_log_good_cu', [ id => 6, cdc_row_version => 1 ], [ foo => 'XYZ', cdc_start_user => 11, cdc_start_time => 12345 ], 'cu/after insert/v1';
lives_ok { $sess->record_change_data({ cset('good_cu'), old => { id => 7, foo => 'XYZ2', cdc_row_version => 1 }, new => { id => 7, foo => 'XYZ3', cdc_row_version => 2 }, user_id => 11, time => 12350 }) } 'record change record (cu, out of order update)';
record_ok 'cdc_log_good_cu', [ id => 7, cdc_row_version => 1 ], [ foo => 'XYZ2', cdc_start_user => undef, cdc_start_time => 2**32-1, cdc_end_time => 12350 ], 'cu/OoO update 1/v1';
record_ok 'cdc_log_good_cu', [ id => 7, cdc_row_version => 2 ], [ foo => 'XYZ3', cdc_start_user => 11, cdc_start_time => 12350, cdc_end_time => 2**32-1 ], 'cu/OoO update 1/v2';
lives_ok { $sess->record_change_data({ cset('good_cu'), new => { id => 7, foo => 'XYZ2', cdc_row_version => 1 }, user_id => 10, time => 12340 }) } 'record change record (cu, out of order insert)';
record_ok 'cdc_log_good_cu', [ id => 7, cdc_row_version => 1 ], [ foo => 'XYZ2', cdc_start_user => 10, cdc_start_time => 12340, cdc_end_time => 12350 ], 'cu/OoO update 2/v1';
record_ok 'cdc_log_good_cu', [ id => 7, cdc_row_version => 2 ], [ foo => 'XYZ3', cdc_start_user => 11, cdc_start_time => 12350, cdc_end_time => 2**32-1 ], 'cu/OoO update 2/v2';
lives_ok { $sess->record_change_data({ cset('good_cd'), new => { id => 8, foo => 'A5' }, user_id => 12, time => 12360 }) } 'record change record (cd, in order insert)';
record_ok 'cdc_log_good_cd', [ id => 8 ], [ foo => 'A5', cdc_start_user => 12, cdc_start_time => 12360, cdc_end_time => 2**32-1, cdc_end_user => undef ], 'cd/insert/v1';
lives_ok { $sess->record_change_data({ cset('good_cd'), old => { id => 8, foo => 'A5' }, user_id => 13, time => 12370 }) } 'record change record (cd, in order delete)';
record_ok 'cdc_log_good_cd', [ id => 8 ], [ foo => 'A5', cdc_start_user => 12, cdc_start_time => 12360, cdc_end_time => 12370, cdc_end_user => 13 ], 'cu/delete/v1';
lives_ok { $sess->record_change_data({ cset('good_cd'), old => { id => 9, foo => 'A6' }, user_id => 14, time => 12390 }) } 'record change record (cd, out of order delete)';
record_ok 'cdc_log_good_cd', [ id => 9 ], [ foo => 'A6', cdc_start_user => undef, cdc_start_time => 2**32-1, cdc_end_time => 12390, cdc_end_user => 14 ], 'cd/OoO delete/v1';
lives_ok { $sess->record_change_data({ cset('good_cd'), new => { id => 9, foo => 'A6' }, user_id => 15, time => 12380 }) } 'record change record (cd, out of order insert)';
record_ok 'cdc_log_good_cd', [ id => 9 ], [ foo => 'A6', cdc_start_user => 15, cdc_start_time => 12380, cdc_end_time => 12390, cdc_end_user => 14 ], 'cd/OoO insert/v1';
lives_ok { $sess->record_change_data({ cset('good_cud'), new => { id => 10, foo => 'A7', cdc_row_version => 1 }, user_id => 16, time => 12390 }) } 'record change record (cud, in order insert)';
record_ok 'cdc_log_good_cud', [ id => 10, cdc_row_version => 1 ], [ foo => 'A7', cdc_start_user => 16, cdc_start_time => 12390, cdc_end_time => 2**32-1, cdc_end_user => undef ], 'cud/insert/v1';
lives_ok { $sess->record_change_data({ cset('good_cud'), old => { id => 10, foo => 'A7', cdc_row_version => 1 }, new => { id => 10, foo => 'A8', cdc_row_version => 2 }, user_id => 17, time => 12400 }) } 'record change record (cud, in order update)';
record_ok 'cdc_log_good_cud', [ id => 10, cdc_row_version => 1 ], [ foo => 'A7', cdc_start_user => 16, cdc_start_time => 12390, cdc_end_time => 12400, cdc_end_user => 17 ], 'cud/update/v1';
record_ok 'cdc_log_good_cud', [ id => 10, cdc_row_version => 2 ], [ foo => 'A8', cdc_start_user => 17, cdc_start_time => 12400, cdc_end_time => 2**32-1, cdc_end_user => undef ], 'cud/update/v2';
lives_ok { $sess->record_change_data({ cset('good_cud'), old => { id => 10, foo => 'A8', cdc_row_version => 2 }, user_id => 18, time => 12410 }) } 'record change record (cud, in order delete)';
record_ok 'cdc_log_good_cud', [ id => 10, cdc_row_version => 2 ], [ foo => 'A8', cdc_start_user => 17, cdc_start_time => 12400, cdc_end_time => 12410, cdc_end_user => 18 ], 'cud/delete/v2';
lives_ok { $sess->record_change_data({ cset('good_cud'), old => { id => 11, foo => 'A9', cdc_row_version => 2 }, user_id => 21, time => 12440 }) } 'record change record (cud, out of order delete)';
record_ok 'cdc_log_good_cud', [ id => 11, cdc_row_version => 2 ], [ foo => 'A9', cdc_start_user => undef, cdc_start_time => 2**32-1, cdc_end_time => 12440, cdc_end_user => 21 ], 'cud/OoO delete/v2';
lives_ok { $sess->record_change_data({ cset('good_cud'), old => { id => 11, foo => 'A8', cdc_row_version => 1 }, new => { id => 11, foo => 'A9', cdc_row_version => 2 }, user_id => 20, time => 12430 }) } 'record change record (cud, out of order update)';
record_ok 'cdc_log_good_cud', [ id => 11, cdc_row_version => 1 ], [ foo => 'A8', cdc_start_user => undef, cdc_start_time => 2**32-1, cdc_end_time => 12430, cdc_end_user => 20 ], 'cud/OoO update/v1';
record_ok 'cdc_log_good_cud', [ id => 11, cdc_row_version => 2 ], [ foo => 'A9', cdc_start_user => 20, cdc_start_time => 12430, cdc_end_time => 12440, cdc_end_user => 21 ], 'cud/OoO update/v2';
lives_ok { $sess->record_change_data({ cset('good_cud'), new => { id => 11, foo => 'A8', cdc_row_version => 1 }, user_id => 19, time => 12420 }) } 'record change record (cud, out of order insert)';
record_ok 'cdc_log_good_cud', [ id => 11, cdc_row_version => 1 ], [ foo => 'A8', cdc_start_user => 19, cdc_start_time => 12420, cdc_end_time => 12430, cdc_end_user => 20 ], 'cud/OoO insert/v1';

# version 2 has a reversed time interval. we will use this later
lives_ok { $sess->record_change_data({ cset('good_cud'), new => { id => 12, foo => 'A9', cdc_row_version => 1 }, user_id => 16, time => 12390 }) } 'record change record (cud, clock skew 1)';
lives_ok { $sess->record_change_data({ cset('good_cud'), old => { id => 12, foo => 'A9', cdc_row_version => 1 }, new => { id => 12, foo => 'A10', cdc_row_version => 2 }, user_id => 17, time => 12410 }) } 'record change record (cud, clock skew 2)';
lives_ok { $sess->record_change_data({ cset('good_cud'), old => { id => 12, foo => 'A10', cdc_row_version => 2 }, new => { id => 12, foo => 'A11', cdc_row_version => 3 }, user_id => 18, time => 12400 }) } 'record change record (cud, clock skew 3)';

$sess->cdc_log_shipping_sub(undef);

$dbh->begin;
my $id = $dbh->good_cd->insert( foo => 'B1' );
is_deeply([$dbh->cdc_log_good_cd->where( id => $id )->dump('foo cdc_start_user')], [], 'sync modality, none before commit');
$dbh->commit;
is_deeply([$dbh->cdc_log_good_cd->where( id => $id )->dump('foo cdc_start_user')], [{ foo => 'B1', cdc_start_user => 42 }], 'sync modality, after commit');

## Integrity checking

throws_ok { $dbh->cdc_log_multifield_cud->insert() } qr/invalid insert into CDC log/;
throws_ok { $dbh->multifield_cud->insert( cdc_row_version => 1 ) } qr/system field/;
throws_ok { $dbh->multifield_cud->get($id_2)->set( cdc_row_version => 2 ) } qr/readonly/;
throws_ok { $dbh->good_c->get(1)->set( foo => 'BAR' ) } qr/this table is not logged for updates/;
throws_ok { $dbh->good_c->get(1)->delete } qr/this table is not logged for deletes/;
throws_ok { $dbh->cdc_log_good_cd->get(9)->delete } qr/modification of CDC logs not permitted/;
throws_ok { $dbh->cdc_log_good_cd->get(9)->set( foo => 'XYZZY' ) } qr/readonly/;
throws_ok { $dbh->cdc_log_good_cd->get(9)->set( cdc_start_time => 19 ) } qr/readonly/;
throws_ok { $dbh->cdc_log_good_cd->get(9)->cdc_start_time( 19 ) } qr/readonly/;

## Query mode

my $r12 = $dbh->multifield_cud->insert( foo => 12 );

$sess->cdc_mock_time(10000);
my $p1 = $dbh->parent->insert(name => 'P1A');
my $p2 = $dbh->parent->insert(name => 'P2A');
my $c1 = $dbh->child->insert(name => 'C1A', parent_id => $p1);
my $c2 = $dbh->child->insert(name => 'C2A', parent_id => $p1);
my $c3 = $dbh->child->insert(name => 'C3A', parent_id => $p2);
my $c4 = $dbh->child->insert(name => 'C4A', parent_id => $p2);

$sess->cdc_mock_time(20000);
$dbh->parent->get($p1)->name('P1B');
$dbh->parent->get($p2)->name('P2B');
$dbh->child->get($c1)->name('C1B');
$dbh->child->get($c2)->name('C2B');
$dbh->child->get($c3)->name('C3B');
$dbh->child->get($c4)->name('C4B');

my $via_child = sub { my @r; $dbh->child->all->each(sub { push @r, $_[0]->parent->name.'/'.$_[0]->name }); join " ", sort @r };
my $via_parent = sub { my @r; $dbh->parent->all->each(sub { my $p = shift; $p->children->each(sub { push @r, $p->name.'/'.$_[0]->name }); }); join " ", sort @r };

{
    local $sess->{query_time_mode} = 1;
    local $sess->{query_selected_time} = time() + 500;
    local $sess->{query_start_time} = 0;
    local $sess->{query_end_time} = 2**32-1;
    local $sess->{time_breakpoint_queue} = {};
    #local $sess->{query_cache} = {};

    throws_ok { $dbh->multifield_cud->insert( foo => 13 ) } qr/modification/;
    throws_ok { $dbh->multifield_cud->get( $r12 )->set(foo => 14) } qr/modification/;
    throws_ok { $dbh->multifield_cud->get( $r12 )->delete } qr/modification/;

    $sess->{query_selected_time} = 12344;
    record_ok 'good_c', [ id => 5 ], undef, 'c: record does not exist before create';
    $sess->{query_selected_time} = 12345;
    record_ok 'good_c', [ id => 5 ], [ foo => 'XYZ' ], 'c: record exists after create';
    $sess->{query_selected_time} = 2**30;
    record_ok 'good_c', [ id => 5 ], [ foo => 'XYZ' ], 'c: record exists in far future';
    $sess->{query_selected_time} = 12359;
    record_ok 'good_cd', [ id => 8 ], undef, 'cd: no record before create';
    $sess->{query_selected_time} = 12360;
    record_ok 'good_cd', [ id => 8 ], [ foo => 'A5' ], 'cd: record after create';
    $sess->{query_selected_time} = 12369;
    record_ok 'good_cd', [ id => 8 ], [ foo => 'A5' ], 'cd: record before delete';
    $sess->{query_selected_time} = 12370;
    record_ok 'good_cd', [ id => 8 ], undef, 'cd: no record after delete';
    $sess->{query_selected_time} = 12405;
    record_ok 'good_cud', [ id => 12 ], [ foo => 'A11' ], 'cud: clock skew does not create dup results';

    $sess->{query_selected_time} = 11000;
    is $via_child->(), 'P1A/C1A P1A/C2A P2A/C3A P2A/C4A', 'many-to-1 fetches v1 ok';
    is $via_parent->(), 'P1A/C1A P1A/C2A P2A/C3A P2A/C4A', '1-to-many fetches v1 ok';
    is $dbh->child->where( name => 'C1A', 'parent.name' => 'P1A' )->count, 1, '"join" v1 1/4';
    is $dbh->child->where( name => 'C1B', 'parent.name' => 'P1A' )->count, 0, '"join" v1 1/4';
    is $dbh->child->where( name => 'C1A', 'parent.name' => 'P1B' )->count, 0, '"join" v1 1/4';
    is $dbh->child->where( name => 'C1A', 'parent.name' => 'P2A' )->count, 0, '"join" v1 1/4';
    is $dbh->parent->where( 'children.name' => 'C1A', name => 'P1A' )->count, 1, '"subquery" v1 1/4';
    is $dbh->parent->where( 'children.name' => 'C1B', name => 'P1A' )->count, 0, '"subquery" v1 1/4';
    is $dbh->parent->where( 'children.name' => 'C1A', name => 'P1B' )->count, 0, '"subquery" v1 1/4';
    is $dbh->parent->where( 'children.name' => 'C1A', name => 'P2A' )->count, 0, '"subquery" v1 1/4';

    $sess->{query_selected_time} = 21000;
    $sess->{time_breakpoint_queue} = {};
    is $via_child->(), 'P1B/C1B P1B/C2B P2B/C3B P2B/C4B', 'many-to-1 fetches v2 ok';
    is $via_parent->(), 'P1B/C1B P1B/C2B P2B/C3B P2B/C4B', '1-to-many fetches v2 ok';
    is $dbh->child->where( name => 'C1B', 'parent.name' => 'P1B' )->count, 1, '"join" v2 1/4';
    is $dbh->child->where( name => 'C1A', 'parent.name' => 'P1B' )->count, 0, '"join" v2 1/4';
    is $dbh->child->where( name => 'C1B', 'parent.name' => 'P1A' )->count, 0, '"join" v2 1/4';
    is $dbh->child->where( name => 'C1B', 'parent.name' => 'P2B' )->count, 0, '"join" v2 1/4';
    is $dbh->parent->where( 'children.name' => 'C1B', name => 'P1B' )->count, 1, '"subquery" v2 1/4';
    is $dbh->parent->where( 'children.name' => 'C1A', name => 'P1B' )->count, 0, '"subquery" v2 1/4';
    is $dbh->parent->where( 'children.name' => 'C1B', name => 'P1A' )->count, 0, '"subquery" v2 1/4';
    is $dbh->parent->where( 'children.name' => 'C1B', name => 'P2B' )->count, 0, '"subquery" v2 1/4';
    is join(' ',sort keys %{$sess->{time_breakpoint_queue}}), join(' ',10000,20000,2**32-1), 'time breakpoint extraction';

    $sess->{query_start_time} = 21000;
    $sess->{query_end_time} = 23000;
    $sess->{query_selected_time} = 22000;
    is $dbh->parent->where( name => 'P2B' )->count, 1, 'version visible despite spanning beginning';
    $sess->{query_start_time} = 11000;
    $sess->{query_end_time} = 13000;
    $sess->{query_selected_time} = 12000;
    is $dbh->parent->where( name => 'P2A' )->count, 1, 'version visible despite spanning end';
}

lives_and {
    $sess->query_point_in_time( 11000, sub {
        is $dbh->child->where( name => 'C1A', 'parent.name' => 'P1A' )->count, 1;
    } );
} 'point in time query';

is_deeply [$sess->query_history( 5000 => 25000, $via_child )],
    [ { start => 5000, end => 10000, value => '' }, { start => 10000, end => 20000, value => 'P1A/C1A P1A/C2A P2A/C3A P2A/C4A' }, { start => 20000, end => 25000, value => 'P1B/C1B P1B/C2B P2B/C3B P2B/C4B' } ],
    'history of complex query 1';
