package DBR::Migrate::Operations::Alter;

use Moose;
use MooseX::StrictConstructor;
use namespace::autoclean;

has instance  => ( is => 'ro', required => 1, isa => 'DBR::Config::Instance' );
has from_name => ( is => 'ro', isa => 'Maybe[Str]', default => undef );
has to_name   => ( is => 'ro', isa => 'Maybe[Str]', default => undef );
has columns   => ( is => 'ro', isa => 'ArrayRef', default => sub { [] } );
has indexes   => ( is => 'ro', isa => 'ArrayRef', default => sub { [] } );
has fkeys     => ( is => 'ro', isa => 'ArrayRef', default => sub { [] } );

sub BUILD {
    my ($self) = @_;

    confess 'from_name or to_name is required' unless defined($self->to_name) || defined($self->from_name);

    my %used_from;
    my %used_to;
    for my $c (@{ $self->columns }) {
        confess 'column change spec must be a hash' unless $c and ref($c) eq 'HASH';
        my %tmp = %$c;

        if (exists($tmp{from_name}) || exists($tmp{from_type})) {
            confess 'cannot drop columns when creating table' if !defined($self->from_name);
            my $from_name = delete $tmp{from_name} or confess 'from_name required in from spec';
            defined(my $from_type = delete $tmp{from_type}) or confess 'from_type required in from spec';
            $used_from{$from_name}++ and confess 'duplicate from_name '.$from_name;
        }

        if (exists($tmp{to_name}) || exists($tmp{to_type})) {
            confess 'cannot create columns when dropping table' if !defined($self->to_name);
            my $to_name = delete $tmp{to_name} or confess 'to_name required in to spec';
            defined(my $to_type = delete $tmp{to_type}) or confess 'to_type required in to spec';
            $used_to{$to_name}++ and confess 'duplicate to_name '.$to_name;
        }

        if (%tmp) {
            confess 'extraneous field in column spec '.((keys %tmp)[0]);
        }

        if (!$c->{from_name} && !$c->{to_name}) {
            confess 'to or from spec required';
        }
    }
}

sub _run {
    my ($self) = @_;

    if (defined($self->{to_name}) && !defined($self->{from_name})) {
        return $self->_run_create;
    }

    if (defined($self->{from_name}) && !defined($self->{to_name})) {
        return $self->_run_drop;
    }

    if ($self->{instance}->module eq 'Mysql') {
        return $self->_run_alter_mysql;
    } else {
        return $self->_run_alter_sqlite;
    }
}

sub _run_create {
    my ($self) = @_;

    # compile into SQL
    # this one is easy from a safety POV b/c you can always just drop the table


    my @plan;
    push @plan, do   => 'CREATE TABLE ...';
    push @plan, undo => 'DROP TABLE ...';
    push @plan, do   => 'CREATE INDEX ...';

    return $self->_runplan(@plan);
}

sub _run_drop {
    my ($self) = @_;

    # check against current schema, make sure the table would be *exactly* recreated by a reversal

    my @plan;
    push @plan, chk  => 'SELECT COUNT(*) FROM ...';
    push @plan, do   => 'DROP TABLE ...';

    return $self->_runplan(@plan);
}

sub _run_alter_mysql {
    my ($self) = @_;

    # compare to current schema, build out an ALTER command
}

sub _run_alter_sqlite {
    my ($self) = @_;

    # compare to current schema
    # if there are any column or foreign key changes:
    #   CREATE TABLE <new_or_temp> (, , ,)
    #   INSERT INTO <new_or_temp> SELECT ... FROM <old>
    #   CREATE INDEX ...
    #   if (new == old):
    #     DROP TABLE <old>
    #     ALTER TABLE <temp> RENAME TO <old>

    # otherwise just index and name changes
    # ALTER TABLE <old> RENAME TO <new>
    # DROP INDEX ...
    # CREATE INDEX ...
}

__PACKAGE__->meta->make_immutable;

1;
