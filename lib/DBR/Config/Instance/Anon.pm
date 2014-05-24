package DBR::Config::Instance::Anon;

use strict;
use base 'DBR::Config::Instance::Common';

# An instance which cannot be looked up, and is used to connect to instances before the proper instance can be loaded or even created

sub _config { $_[0] }

sub new {
    my $package = shift;

    my $self = bless( {@_}, $package );
    $self->{schema_id} ||= '';
    return $self->_inflate($self);
}

1;
