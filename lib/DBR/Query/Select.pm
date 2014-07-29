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
sub _validate_self{ 1 } # If I exist, I'm valid

sub fields{
      my $self = shift;
      exists( $_[0] ) or return wantarray?( @{$self->{fields}||[]} ) : $self->{fields} || undef;

      my @fields = $self->_arrayify(@_);
      scalar(@fields) || croak('must provide at least one field');

      my $lastidx = -1;
      for (@fields){
	    ref($_) =~ /^DBR::Config::Field/ || croak('must specify field as a DBR::Config::Field object'); # Could also be ::Anon
	    $_->index( ++$lastidx );
      }
      $self->{last_idx} = $lastidx;
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
      $sql .= ' LIMIT ' . $self->_limit_clause       if $self->{limit} || $self->{offset};

      $self->_logDebug2( $sql );
      return $sql;
}

sub lastidx  { $_[0]{last_idx} }
sub can_be_subquery { scalar( @{ $_[0]->fields || [] } ) == 1 }; # Must have exactly one field

sub run {
      my $self = shift;
      croak('DBR::Query::Select::run is not usable in time query mode') if $self->{session}->query_time_mode;
      return $self->{sth} ||= $self->instance->getconn->prepare( $self->sql ) || confess "Failed to prepare"; # only run once
}

# use this if you will take either a sth or an arrayref
sub _exec {
    my $self = shift;

    if ($self->{session}->query_time_mode) {
        my @tbls = @{$self->{tables}};
        croak('joins must be run simulated in time-query mode') if @tbls != 1;
        my $cdc = $tbls[0]->cdc_type;
        if (!$cdc->{logged}) {
            # assume this table is permanently valid
            return $self->{sth} ||= $self->instance->getconn->prepare( $self->sql ) || confess "Failed to prepare"; # only run once
        }
        # rebuild the statement to query the log table
        my $newtable = DBR::Config::Table->new(
            session     => $self->{session},
            table_id    => $cdc->{log_table}->{table_id},
            instance_id => $self->{instance}->guid,
            alias       => $tbls[0]->alias,
        ) or croak('failed to rebind table object');

        my $newfields = [ @{ $self->{fields} } ];
        my $last_real_idx = $self->{last_idx};
        my %index;
        my @pk_indices;
        map { $index{$_->name} = $_->index } @$newfields;
        for my $fd (@{$newtable->fields}) {
            next unless $fd->name eq 'cdc_start_time' || $fd->name eq 'cdc_end_time' || $fd->is_pkey;
            if (!defined($index{$fd->name})) {
                my $ix = scalar @$newfields;
                $fd->index($ix);
                $index{$fd->name} = $ix;
                push @$newfields, $fd;
            }
            if ($fd->name !~ /^cdc_/) { push @pk_indices, $index{$fd->name} }
        }

        # in addition to the obvious blehness of overriding tables and fields, this is additionally creating a somewhat invalid state by mixing base table fields with log table fields
        my $sql = do {
            local $self->{tables} = [$newtable];
            local $self->{fields} = $newfields;
            local $self->{offset};
            local $self->{limit};
            $self->sql;
        };

        my $sth = $self->instance->getconn->prepare( $sql ) || confess "Failed to prepare";
        defined( $sth->execute ) or croak 'failed to execute statement (' . $sth->errstr. ')';
        my $rows = $sth->fetchall_arrayref or croak 'failed to execute statement (' . $sth->errstr . ')';

        # now filter out rows for the desired point in time, eliminating dups and preserving order
        # why dups?  because of clock skew, the timestamps on a row can be non-monotonic.  which means some validity spans are empty while others overlap...
        my $endix = $index{cdc_end_time};
        my $startix = $index{cdc_start_time};
        my $verix = $index{cdc_row_version};
        my $focus = $self->{session}->query_selected_time; # notionally this is the middle of a second, all recorded stamps are beginnings of seconds
        my %best;
        for my $row (@$rows) {
            if ($row->[$startix] > $focus) { @$row = (); next; }
            if (defined($endix) && $row->[$endix] <= $focus) { @$row = (); next; }
            if (defined $verix) {
                my $bp = \$best{ join "\x{110000}", @$row[@pk_indices] };
                if (!$$bp) {
                    $$bp = $row;
                } elsif ($$bp->[$verix] < $row->[$verix]) {
                    @{$$bp} = ();
                    $$bp = $row;
                } else {
                    @$row = ();
                }
            }
        }

        @$rows = grep { @$_ } @$rows;
        map { splice(@$_, $last_real_idx+1) } @$rows;
        splice(@$rows, 0, $self->{offset}) if $self->{offset};
        splice(@$rows, $self->{limit}) if $self->{limit} && @$rows > $self->{limit};

        return $rows;
    } else {
        return $self->{sth} ||= $self->instance->getconn->prepare( $self->sql ) || confess "Failed to prepare"; # only run once
    }
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

    my $rows = $self->_exec or croak 'failed to execute';

    if (ref($rows) ne 'ARRAY') {
        my $sth = $rows;
        defined( $sth->execute ) or croak 'failed to execute statement (' . $sth->errstr. ')';
        $rows = $sth->fetchall_arrayref or croak 'failed to execute statement (' . $sth->errstr . ')';
    }

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
    my $sth = $self->_exec or croak 'failed to execute';

    if (ref($sth) eq 'ARRAY') {
        return [ map($_->[0], @$sth) ];
    }

    $sth->execute;

    my ($val,@list);

    $sth->bind_col(1, \$val) || die "Failed to bind column";
    push @list, $val while $sth->fetch;

    $sth->finish;
    return \@list;
}

sub fetch_fieldmap {
    my ($self, $pk, $field) = @_;
    my $sth = $self->_exec or return $self->_error('failed to execute');
    my $getrow = 'shift(@$sth)';

    if (ref($sth) ne 'ARRAY') {
        $getrow = '$sth->fetchrow_arrayref()';
        $sth->execute() or return $self->_error('Failed to execute sth');
    }

    my $lut = {};

    my $e = qq{
        while (my \$row = $getrow) {
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
