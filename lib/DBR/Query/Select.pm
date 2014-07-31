# The contents of this file are Copyright (c) 2010 Daniel Norman
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License as
# published by the Free Software Foundation.

###########################################
package DBR::Query::Select;

use strict;
use base 'DBR::Query';
use Carp;
use DBR::Record::Maker;
use Scalar::Util 'weaken';

sub _params    { qw (fields tables where builder limit offset orderby lock quiet_error) }
sub _reqparams { qw (fields tables) }
sub _validate_self {
    my ($self) = @_;
    # this is our chance to do late fixups after all fields are set

    # convert to historical if needed
    my $sess = $self->{session};
    if ($self->{session}->query_time_mode) {
        my @tbls = @{$self->{tables}};
        croak('joins must be run simulated in time-query mode') if @tbls != 1;
        my $cdc = $tbls[0]->cdc_type;
        if ($cdc->{logged}) {
            $self->_convert_to_historical;
        }
    }

    # field set is final, can set the indices now
    my $lastidx = -1;
    for (@{$self->{fields}}) {
        $_->index( ++$lastidx );
    }
    $self->{last_idx} = $lastidx;

    1; # not actually validating
}

sub _convert_to_historical {
    my ($self) = @_;

    # rebuild the statement to query the log table
    my $basetable = $self->{tables}->[0];
    # preserve alias so that we can cheat and reuse the where tree
    my $logtable = $basetable->cdc_type->{log_table}->clone( alias => $basetable->alias, instance_id => $self->{instance}->guid )
        or croak('failed to rebind table object');

    $self->{tables} = [$logtable];
    $self->{is_historical} = 1;

    # we're going to be consistently including log fields in the field array.
    # what we're not consistent about is that orderby and where are still referencing non-log fields, and DBR::Record::Maker uses the log field names when code expects non-log field names
    # both of these depend on the common naming, beyond the fact of the common naming being used for matching

    my %include;
    map { $include{$_->name} = $_ } @{ $self->{fields} };
    my %logfields;
    map { $logfields{$_->name} = $_ } @{ $logtable->fields };

    my $nfields = [ map { $logfields{$_->name} } @{ $self->{fields} } ];

    my @newcon = $self->{where} || ();

    for my $fd (values %logfields) {
        my $name = $fd->name;
        if (!$include{$name} && ($name eq 'cdc_start_time' || $name eq 'cdc_end_time' || $name eq 'cdc_row_version')) {
            push @$nfields, $fd;
        }
    }

    my $fd;
    if ($fd = $logfields{cdc_start_time}) {
        # starts after end of range -> cannot be interested
        push @newcon, DBR::Query::Part::Compare->new( field => $fd, operator => 'le', value => $fd->makevalue($self->{session}->query_end_time) );
    }

    if ($fd = $logfields{cdc_end_time}) {
        # must end after the first second we care about
        push @newcon, DBR::Query::Part::Compare->new( field => $fd, operator => 'gt', value => $fd->makevalue($self->{session}->query_start_time) );
    }

    $self->{splitfield} = $self->{splitfield} && $logfields{$self->{splitfield}->name};
    $self->{fields} = $nfields;
    $self->{where} = @newcon ? DBR::Query::Part::And->new( @newcon ) : undef;
}

sub fields{
      my $self = shift;
      exists( $_[0] ) or return wantarray?( @{$self->{fields}||[]} ) : $self->{fields} || undef;

      my @fields = $self->_arrayify(@_);
      scalar(@fields) || croak('must provide at least one field');

      for (@fields){
	    ref($_) =~ /^DBR::Config::Field/ || croak('must specify field as a DBR::Config::Field object'); # Could also be ::Anon
      }
      $self->{fields}   = \@fields;

      return 1;
}


sub sql{
      my $self = shift;
      my $conn   = $self->instance->connect('conn') or return $self->_error('failed to connect');
      my $sql;

      my $tables = join(',', map { $_->sql( $conn ) } @{$self->{tables}} );
      my $fields = join(',', map { $_->sql( $conn ) } @{$self->{fields}} );

      $sql = "SELECT $fields FROM $tables";
      $sql .= ' WHERE ' . $self->{where}->sql($conn) if $self->{where};
      if (@{ $self->{orderby} || [] }) {
          $sql .= ' ORDER BY ' . join(', ', map { $_->sql($conn) } @{ $self->{orderby} || [] });
      }
      $sql .= ' FOR UPDATE'                          if $self->{lock} && $conn->can_lock;
      $sql .= ' LIMIT ' . $self->_limit_clause       if ($self->{limit} || $self->{offset}) && !$self->{is_historical};

      $self->_logDebug2( $sql );
      return $sql;
}

sub is_historical { $_[0]{is_historical} }
sub lastidx  { $_[0]{last_idx} }
sub can_be_subquery { scalar( @{ $_[0]->fields || [] } ) == 1 }; # Must have exactly one field

sub run {
      my $self = shift;
      croak('DBR::Query::Select::run is not usable on historical queries') if $self->{is_historical};
      return $self->{sth} ||= $self->instance->getconn->prepare( $self->sql ) || confess "Failed to prepare"; # only run once
}

# use this if you're taking an arrayref and want to support point-in-time queries (which cannot be reduced to a sth)
sub _exec {
    my $self = shift;

    if ($self->{is_historical}) {
        return $self->_historical_fetch;
    } else {
        my $sth = $self->{sth} ||= $self->instance->getconn->prepare( $self->sql ) || confess "Failed to prepare"; # only run once
        defined( $sth->execute ) or croak 'failed to execute statement (' . $sth->errstr. ')';
        return $sth->fetchall_arrayref or croak 'failed to execute statement (' . $sth->errstr. ')';
    }
}

# when doing a historical function calculation query we try to keep down
# queries by pulling all data for the active range once and then post-filtering
# it. now filter out rows for the desired point in time, eliminating dups and
# preserving order. why dups?  because of clock skew, the timestamps on a row
# can be non-monotonic.  which means some validity spans are empty while others
# overlap...
sub _historical_fetch {
    my ($self) = @_;

    my $sess = $self->{session};
    my $qc = $sess->query_cache;
    my $focus = $sess->query_selected_time; # notionally this is the middle of a second, all recorded stamps are beginnings of seconds
    my $bp = $sess->time_breakpoint_queue;

    # multi-point time queries will make a looot of redundant fetches
    # may make sense to drag this cache to a higher level, so that e.g. record-makers can be reused as well
    my $sql = $self->sql;
    my $rowsp = $qc ? \$qc->{ $self->instance->getconn }->{ $sql } : \do { my $x };
    my $rows = $$rowsp ||= do {
        my $sth = $self->instance->getconn->prepare( $sql ) || confess "Failed to prepare";
        defined( $sth->execute ) or croak 'failed to execute statement (' . $sth->errstr. ')';
        $sth->fetchall_arrayref or croak 'failed to execute statement (' . $sth->errstr . ')';
    };

    # now filter, being careful not to damage the original $rows
    $rows = [@$rows];

    my ($endix,$startix,$verix,@pk_indices);
    for my $f (@{ $self->{fields} }) {
        $endix = $f->index if $f->name eq 'cdc_end_time';
        $startix = $f->index if $f->name eq 'cdc_start_time';
        $verix = $f->index if $f->name eq 'cdc_row_version';
        push @pk_indices, $f->index if $f->is_pkey && $f->name ne 'cdc_row_version';
    }
    my %best;
    for my $row (@$rows) {
        $bp->{$row->[$startix]} = 1;
        $bp->{$row->[$endix]} = 1 if defined($endix);
        if ($row->[$startix] > $focus) { $row = undef; next; }
        if (defined($endix) && $row->[$endix] <= $focus) { $row = undef; next; }
        if (defined $verix) {
            my $bp = \$best{ join "\x{110000}", @$row[@pk_indices] };
            if (!$$bp) {
                $$bp = \$row;
            } elsif ($$$bp->[$verix] < $row->[$verix]) {
                $$$bp = undef;
                $$bp = \$row;
            } else {
                $row = undef;
            }
        }
    }

    @$rows = grep { $_ } @$rows;
    splice(@$rows, 0, $self->{offset}) if $self->{offset};
    splice(@$rows, $self->{limit}) if $self->{limit} && @$rows > $self->{limit};

    return $rows;
}

sub reset {
      my $self = shift;
      return $self->{sth} && $self->{sth}->finish;
}

sub orderby {
    my $self = shift;
    exists($_[0]) ? ($self->{orderby} = $_[0]) : $self->{orderby};
}

# HERE - it's a little funky that we are handling split queries here,
# but non-split queries in ResultSet. Not horrible... just funky.
sub fetch_segment{
      my $self = shift;
      my $value = shift;

      return ( $self->{spvals} ||= $self->_do_split )->{ $value } || [];
}

sub _do_split{
      my $self = shift;

      # Should have a splitfield if we're getting here. Don't check for it. speeed.
      defined( my $idx = $self->{splitfield}->index ) or croak 'field object must provide an index';

      my %groupby;

      # this was an eval, but there is scant to be gained replacing pp_padsv with pp_const
      for my $rec ($self->fetch_all_records) {
          push @{$groupby{ $rec->[0][$idx] }}, $rec;
      }

      return \%groupby;
}

sub fetch_all_records {
    my $self = shift;

    my $rows = $self->_exec;
    my $recobj = $self->get_record_obj;
    my $class = $recobj->class;

    my @out;
    while (@$rows) {
        my $chunk = [splice @$rows, 0, 1000];
        my $buddy = [$chunk, $recobj];
        push @out, map( bless([$_,$buddy],$class), @$chunk );
        map { weaken $_ } @$chunk;
    }

    return wantarray ? @out : \@out;
}

sub fetch_column {
    my ($self) = @_;
    my $rows = $self->_exec;
    return [map($_->[0], @$rows)];
}

sub fetch_fieldmap {
    my ($self, $pk, $field) = @_;
    confess('should not get here for a historical fetch - all late fetching should be direct on the log') if $self->{is_historical};
    my $sth = $self->run;
    $sth->execute() or return $self->_error('Failed to execute sth');

    my $lut = {};

    my $e = qq{
        while (my \$row = \$sth->fetchrow_arrayref) {
            \$lut->${\ join "", map("{\$row->[".$_->index."]}",@$pk) } = \$row->[${\$field->index}];
        }
    };
    eval $e;
    Carp::confess($@) if $@;
    return $lut;
}

sub splitfield { return $_[0]->{splitfield} }

sub get_record_obj{
      my $self = shift;

      # Only make the record-maker object once per query. Even split queries should be able to share the same one.
      return $self->{recordobj} ||= DBR::Record::Maker->new(
							    session  => $self->{session},
							    query    => $self,  # This value is not preserved by the record maker, thus no memory leak
							   ) or confess ('failed to create record class');
}

sub DESTROY{
      my $self = shift;

      # Can't finish the sth when going out of scope, it might live longer than this object.

      return 1;
}
1;
