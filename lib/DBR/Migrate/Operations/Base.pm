package DBR::Migrate::Operations::Base;

use strict;
use warnings;

# return ($code, $err), where $code = 0/success, 1/rollback, 2/fatal
sub _run { die "must be implemented" }

sub describe { die "must be implemented" }

sub reverse { die "must be implemented" }

1;
