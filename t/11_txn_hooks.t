#!/usr/bin/perl

use strict;
use warnings;
no warnings 'uninitialized';

$| = 1;

use lib './lib';
use t::lib::Test;
use Test::More tests => 22;

# As always, it's important that the sample database is not tampered with, otherwise our tests will fail
my $dbr = setup_schema_ok('music');

my $dbh = $dbr->connect('music');
ok($dbh, 'dbr connect');

my $count;
my $rv;
my $log;

$log = '';
$dbh->add_rollback_hook(sub { $log .= 'A' });
is($log, '', 'rollback hook outside txn is ignored');

$log = '';
$dbh->add_pre_commit_hook(sub { $log .= 'B' });
is($log, 'B', 'pre-commit hook outside txn is run immediately');

$log = '';
$dbh->add_pre_commit_hook(sub { $log .= 'C' });
is($log, 'C', 'post-commit hook outside txn is run immediately');

$log = '';
$dbh->begin;
$dbh->add_rollback_hook(sub { $log .= 'D' });
$dbh->commit;
is($log, '', 'rollback hook is ignored by commit');

$log = '';
$dbh->begin;
$dbh->add_rollback_hook(sub { $log .= 'E' });
$dbh->rollback;
is($log, 'E', 'rollback hooks are run by rollback');

$log = '';
$dbh->begin;
$dbh->add_rollback_hook(sub { $log .= 'F' });
$dbh->add_rollback_hook(sub { $log .= 'G' });
$dbh->rollback;
is($log, 'GF', '... in reverse order');

$log = '';
$dbh->begin;
$dbh->add_pre_commit_hook(sub { $log .= $dbh->{conn}->b_intrans ? 'H' : 'I' });
$dbh->add_post_commit_hook(sub { $log .= $dbh->{conn}->b_intrans ? 'J' : 'K' });
$dbh->add_pre_commit_hook(sub { $log .= $dbh->{conn}->b_intrans ? 'L' : 'M' });
$dbh->add_post_commit_hook(sub { $log .= $dbh->{conn}->b_intrans ? 'N' : 'O' });
is($log, '', 'commit hooks deferred in transaction');
$dbh->commit;
is($log, 'HLKO', 'run correctly on commit');

$log = '';
$dbh->begin;
$dbh->add_pre_commit_hook(sub { $log .= $dbh->{conn}->b_intrans ? 'H' : 'I' });
$dbh->add_post_commit_hook(sub { $log .= $dbh->{conn}->b_intrans ? 'J' : 'K' });
$dbh->add_pre_commit_hook(sub { $log .= $dbh->{conn}->b_intrans ? 'L' : 'M' });
$dbh->add_post_commit_hook(sub { $log .= $dbh->{conn}->b_intrans ? 'N' : 'O' });
$dbh->rollback;
is($log, '', 'ignored on rollback');

$log = '';
$dbh->add_pre_commit_hook(sub { $log .= shift() }, 'P');
is($log, 'P', 'pre_commit immediate uses args');

$log = '';
$dbh->add_post_commit_hook(sub { $log .= shift() }, 'Q');
is($log, 'Q', 'post_commit immediate uses args');

my $apphook  = sub { $log .= join('','(',@_,')') };
my $apphook2 = sub { $log .= join('','[',@_,']') };
my $high_apphook  = sub { $log .= join('','(',@_,')') };
my $nilhook  = sub { $log .= 'R' };

$log = '';
$dbh->begin;
$dbh->add_pre_commit_hook($nilhook);
$dbh->add_pre_commit_hook($nilhook);
$dbh->commit;
is($log, 'R', 'with same sub, hook run only once');

$log = '';
$dbh->begin;
$dbh->add_pre_commit_hook($apphook, 'S');
$dbh->add_pre_commit_hook($apphook2, 'T');
$dbh->add_pre_commit_hook($apphook, 'U');
$dbh->commit;
is($log, '(SU)[T]', 'order preserving merge 1, pre_commit');

$log = '';
$dbh->begin;
$dbh->add_pre_commit_hook($apphook2, 'S');
$dbh->add_pre_commit_hook($apphook, 'T');
$dbh->add_pre_commit_hook($apphook2, 'U');
$dbh->commit;
is($log, '[SU](T)', 'order preserving merge 2, pre_commit');

$log = '';
$dbh->begin;
$dbh->add_post_commit_hook($apphook, 'S');
$dbh->add_post_commit_hook($apphook2, 'T');
$dbh->add_post_commit_hook($apphook, 'U');
$dbh->commit;
is($log, '(SU)[T]', 'order preserving merge, post_commit');

$log = '';
$dbh->begin;
$dbh->add_rollback_hook($apphook, 'S');
$dbh->add_rollback_hook($apphook2, 'T');
$dbh->add_rollback_hook($apphook, 'U');
$dbh->rollback;
is($log, '[T](US)', 'order preserving merge 1, rollback');

$log = '';
$dbh->add_pre_commit_hook(sub { $dbh->add_pre_commit_hook(sub { $log .= 'V' }) });
is($log, 'V', 'can add pre_commit hook from immediately executed pre_commit');

$log = '';
$dbh->begin;
$dbh->add_pre_commit_hook(sub { $dbh->add_pre_commit_hook(sub { $log .= 'W' }) });
$dbh->add_pre_commit_hook(sub { $log .= 'X' });
$dbh->add_pre_commit_hook(sub { $dbh->add_post_commit_hook(sub { $log .= 'Y' }) });
$dbh->add_pre_commit_hook(sub { $dbh->add_rollback_hook(sub { $log .= 'Z' }) });
$dbh->commit;
is($log, 'XWY', 'can add pre_commit and post_commit hooks at pre_commit time, order is respected');

$log = '';
$dbh->begin;
$dbh->add_pre_commit_hook($apphook, 'a');
$dbh->add_pre_commit_hook(sub { $dbh->add_pre_commit_hook($apphook, 'b') });
$dbh->commit;
is($log, '(a)(b)', 'too late to merge pre-commit');

$log = '';
$dbh->begin;
$dbh->add_pre_commit_hook(sub { $dbh->add_pre_commit_hook($apphook, 'd') });
$dbh->add_pre_commit_hook($apphook, 'c');
$dbh->commit;
is($log, '(cd)', 'not too late to merge pre-commit');

$log = '';
$dbh->begin;
DBR::Misc::Connection->set_hook_priority( $high_apphook, '000' );
$dbh->add_pre_commit_hook($apphook, 'S');
$dbh->add_pre_commit_hook($high_apphook, 'T');
$dbh->commit;
is($log, '(T)(S)', 'hook execution respects priorities');
