# the contents of this file are Copyright (c) 2009 Daniel Norman
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License as
# published by the Free Software Foundation.

package DBR::Config::ScanDB;

use strict;
use base 'DBR::Common';
use DBR::Config::Field;
use DBR::Config::Schema;

sub new {
      my( $package ) = shift;
      my %params = @_;
      my $self = {
		  session   => $params{session},
		  conf_instance => $params{conf_instance},
		  scan_instance => $params{scan_instance},
		 };

      bless( $self, $package );

      return $self->_error('session object must be specified')   unless $self->{session};
      return $self->_error('conf_instance object must be specified')   unless $self->{conf_instance};
      return $self->_error('scan_instance object must be specified')   unless $self->{scan_instance};

      $self->{schema_id} = $self->{scan_instance}->schema_id or
	return $self->_error('Cannot scan an instance that has no schema');

      return( $self );
}

sub scan{
    my $self = shift;
    my %params = @_; 

    my $dbh = $self->{conf_instance}->connect || die "failed to connect to config db";
    $dbh->begin;

    my $sch_info = $self->{scan_instance}->phys_schema;

    foreach my $table (keys %$sch_info) {
        print "Updating $table\n" if $params{pretty};

        $self->update_table($table, $sch_info->{$table});
    }

      $dbh->commit;

      #HACK - the scanner should load up the in-memory representation at the same time
      DBR::Config::Schema->load(
            session   => $self->{session},
            schema_id => $self->{schema_id},
            instance  => $self->{conf_instance},
          ) or die "Failed to reload schema";

      return 1;
}

sub update_table{
    my ($self, $name, $info) = @_;

    my $dbh = $self->{conf_instance}->connect || die "failed to connect to config db";

    my $table_id = $dbh->dbr_tables->where( schema_id => $self->{schema_id}, name => $name )->next->table_id;
    $table_id ||= $dbh->dbr_tables->insert( schema_id => $self->{schema_id}, name => $name );

    my $fieldmap = $dbh->dbr_fields->where( table_id => $table_id )->hashmap_single('name');

    foreach my $name (sort keys %{$info->{columns}}) {
        my $field = $info->{columns}{$name};

        my $type = $field->{type};
        ($type) = split ' ', $type;
        my $typeid = DBR::Config::Field->get_type_id($type) or die( "Invalid type '$type'" );
        my $ref = {
            is_nullable => $field->{is_nullable},
            is_signed   => $field->{is_signed},
            is_pkey     => $field->{is_pkey} || 0,
            data_type   => $typeid,
            max_value   => $field->{max_value} || 0,
            $self->{conf_instance}->meta_version >= 2 ? (
                decimal_digits => $field->{decimal_digits} || 0,
            ):(),
        };

        my $record = delete $fieldmap->{$name};
        if ($record) {
            $record->set( %$ref );
        } else {
            $dbh->dbr_fields->insert( table_id => $table_id, name => $name, %$ref );
        }
    }

    map { $_->delete } values %$fieldmap;
}

1;
