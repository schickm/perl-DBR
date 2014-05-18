package DBR::Migrate::Operations::Logical;

use strict;
use warnings;
use parent 'DBR::Migrate::Operations::Base';

# return ($code, $err), where $code = 0/success, 1/rollback, 2/fatal
sub _run { die "must be implemented" }

sub describe { die "must be implemented" }

sub reverse { die "must be implemented" }

1;
