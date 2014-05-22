# the contents of this file are Copyright (c) 2009 Daniel Norman
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License as
# published by the Free Software Foundation.

package DBR::Config::Schema;

use strict;
use base 'DBR::Common';

use DBR::Config::Table;
use Clone;

my %TABLES_BY_NAME;
my %INSTANCE_LOOKUP;
my %SCHEMAS_BY_ID;
my %SCHEMAS_BY_HANDLE;
my $NEXT_METACIRC_ID = -1;

sub load{
      my( $package ) = shift;
      my %params = @_;

      my $self = { session => $params{session} };
      bless( $self, $package ); # Dummy object

      my $instance = $params{instance} || return $self->_error('instance is required');

      my $dbrh = $instance->connect || return $self->_error("Failed to connect to ${\$instance->name}");

      my $schema_ids = $params{schema_id} || return $self->_error('schema_id is required');
      $schema_ids = [$schema_ids] unless ref($schema_ids) eq 'ARRAY';

      return 1 unless @$schema_ids;

      return $self->_error('Failed to select instances') unless
	my $schemas = $params{inject}{schemas} || $dbrh->select(
				    -table => 'dbr_schemas',
				    -fields => 'schema_id handle display_name',
				    -where  => { schema_id => ['d in', @{$schema_ids}] },
				   );

      my @schema_ids; # track the schema ids from this request seperately from the global cache
      foreach my $schema (@$schemas){
	    $SCHEMAS_BY_ID{  $schema->{schema_id} } = $schema;
	    $SCHEMAS_BY_HANDLE{ $schema->{handle} } = $schema->{schema_id};

	    push @schema_ids, $schema->{schema_id};
      }

      DBR::Config::Table->load(
			       session => $self->{session},
			       instance => $instance,
                               inject => $params{inject},
			       schema_id => \@schema_ids,
			      ) or return $package->_error('failed to load tables');

      return 1;
}

sub list_schemas {
      return Clone::clone( [ sort { ($a->{display_name} || '') cmp ($b->{display_name} || '') } values %SCHEMAS_BY_ID ] );
}

sub _register_table{
      my $package = shift; # no dummy $self object here, for efficiency
      my %params = @_;

      my $schema_id = $params{schema_id} or return $package->_error('schema_id is required');
      $SCHEMAS_BY_ID{ $schema_id } or return $package->_error('invalid schema_id');

      my $name      = $params{name}      or return $package->_error('name is required');
      my $table_id  = $params{table_id}  or return $package->_error('table_id is required');

      $TABLES_BY_NAME{ $schema_id } -> { $name } = $table_id;

      return 1;
}

sub _register_instance{
      my $package = shift; # no dummy $self object here, for efficiency
      my %params = @_;

      my $schema_id = $params{schema_id} or die 'schema_id is required';
      my $class     = $params{class}     or die 'class is required';
      defined(my $tag = $params{tag})    or die 'tag is required';
      my $guid      = $params{guid}      or die 'guid is required';

      $INSTANCE_LOOKUP{ $schema_id }{ $tag }{ $class } = $guid;

      return 1;
}

sub _get_bootstrap_schema {
    my ($package, %params) = @_;

    my $MIN_VER = 1;
    my $MAX_VER = 2;

    # Since positive values are used for 'user' objects, our meta-meta-catalog uses negative IDs
    my $ver = int($params{version});
    unless ($ver >= $MIN_VER && $ver <= $MAX_VER) { die "Invalid meta_version $ver in config file" }
    if (!$SCHEMAS_BY_ID{ -$ver }) {
        $package->load(
            inject => $package->_build_bootstrap_schema($ver),
            session => $params{session},
            instance => $params{instance},
            schema_id => [-$ver],
        );
    }

    return -$ver;
}

sub _build_bootstrap_schema {
    my ($package, $ver) = @_;

    my @COL_INFO = qw{
        1:2  dbr_schemas        schema_id        INTEGER:UN:NN
        1:2  dbr_schemas        handle           VARCHAR:50
        1:2  dbr_schemas        display_name     VARCHAR:50
        2    dbr_schemas        owned_by_migration  BOOLEAN:NN:DEF=0

        1:2  dbr_instances      instance_id      INTEGER:UN:NN
        1:2  dbr_instances      schema_id        INTEGER:UN:NN
        1:2  dbr_instances      handle           VARCHAR:50:NN
        1:2  dbr_instances      class            VARCHAR:50:NN
        1:2  dbr_instances      tag              VARCHAR:250
        1:2  dbr_instances      dbname           VARCHAR:250
        2    dbr_instances      prefix           VARCHAR:250
        1:2  dbr_instances      username         VARCHAR:250
        1:2  dbr_instances      password         VARCHAR:250
        1:2  dbr_instances      host             VARCHAR:250
        1:2  dbr_instances      dbfile           VARCHAR:250
        1:2  dbr_instances      module           VARCHAR:50:NN
        1:2  dbr_instances      readonly         BOOLEAN

        1:2  dbr_tables         table_id         INTEGER:UN:NN
        1:2  dbr_tables         schema_id        INTEGER:UN:NN
        1:2  dbr_tables         name             VARCHAR:250:NN
        1:2  dbr_tables         display_name     VARCHAR:250
        1:2  dbr_tables         is_cachable      BOOLEAN

        1:2  dbr_fields         field_id         INTEGER:UN:NN
        1:2  dbr_fields         table_id         INTEGER:UN:NN
        1:2  dbr_fields         name             VARCHAR:250:NN
        1:2  dbr_fields         data_type        TINYINT:UN:NN
        1:2  dbr_fields         is_nullable      BOOLEAN
        1:2  dbr_fields         is_signed        BOOLEAN
        1:2  dbr_fields         max_value        INTEGER:UN:NN
        2    dbr_fields         decimal_precision INTEGER
        1:2  dbr_fields         display_name     VARCHAR:250
        1:2  dbr_fields         is_pkey          BOOLEAN:DEF=0
        1:2  dbr_fields         index_type       TINYINT
        1:2  dbr_fields         trans_id         TINYINT:UN
        1:2  dbr_fields         regex            VARCHAR:250
        1:2  dbr_fields         default_val      VARCHAR:250

        2    indexes            id               INTEGER:UN:NN
        2    indexes            field_id         INTEGER:UN:NN
        2    indexes            refinement_of_id INTEGER:UN
        2    indexes            prefix_length    INTEGER
        2    indexes            is_unique        BOOLEAN:NN:DEF=0

        2    managed_rows       id               INTEGER:UN:NN
        2    managed_rows       table_id         INTEGER:UN:NN

        2    managed_values     id               INTEGER:UN:NN
        2    managed_values     row_id           INTEGER:UN:NN
        2    managed_values     field_id         INTEGER:UN:NN
        2    managed_values     value            VARCHAR:255

        2    migrations         id               INTEGER:UN:NN
        2    migrations         name             VARCHAR:255:NN
        2    migrations         active           BOOLEAN:NN:DEF=0
        2    migrations         pinned           BOOLEAN:NN:DEF=0
        2    migrations         crashed          BOOLEAN:NN:DEF=0
        2    migrations         content          LONGBLOB:NN

        1:2  dbr_relationships  relationship_id  INTEGER:UN:NN
        1:2  dbr_relationships  from_name        VARCHAR:45:NN
        1:2  dbr_relationships  from_table_id    INTEGER:UN:NN
        1:2  dbr_relationships  from_field_id    INTEGER:UN:NN
        1:2  dbr_relationships  to_name          VARCHAR:45:NN
        1:2  dbr_relationships  to_table_id      INTEGER:UN:NN
        1:2  dbr_relationships  to_field_id      INTEGER:UN:NN
        1:2  dbr_relationships  type             TINYINT:UN:NN

        1:2  cache_scopes       scope_id         INTEGER:UN:NN
        1:2  cache_scopes       digest           CHAR:32:NN
        1:2  cache_fielduse     row_id           INTEGER:UN:NN
        1:2  cache_fielduse     scope_id         INTEGER:UN:NN
        1:2  cache_fielduse     field_id         INTEGER:UN:NN

        1:2  enum               enum_id          INTEGER:UN:NN
        1:2  enum               handle           VARCHAR:250
        1:2  enum               name             VARCHAR:250
        1:2  enum               override_id      INTEGER:UN

        1:2  enum_legacy_map    row_id           INTEGER:UN:NN
        1:2  enum_legacy_map    context          VARCHAR:250
        1:2  enum_legacy_map    field            VARCHAR:250
        1:2  enum_legacy_map    enum_id          INTEGER:UN:NN
        1:2  enum_legacy_map    sortval          INTEGER

        1:2  enum_map           row_id           INTEGER:UN:NN
        1:2  enum_map           field_id         INTEGER:UN:NN
        1:2  enum_map           enum_id          INTEGER:UN:NN
        1:2  enum_map           sortval          INTEGER
    };

    my @REL_INFO = qw{
        dbr_instances.schema_id          instances           schema      dbr_schemas.schema_id
        dbr_tables.schema_id             tables              schema      dbr_schemas.schema_id
        dbr_fields.table_id              fields              table       dbr_tables.table_id
        dbr_relationships.from_table_id  from_relationships  from_table  dbr_tables.table_id
        dbr_relationships.from_field_id  from_relationships  from_field  dbr_fields.field_id
        dbr_relationships.to_table_id    to_relationships    to_table    dbr_tables.table_id
        dbr_relationships.to_field_id    to_relationships    to_field    dbr_fields.field_id
        cache_fielduse.scope_id          field_use           scope       cache_scopes.scope_id
        enum_legacy_map.enum_id          legacy_maps         enum        enum.enum_id
        enum_map.field_id                enum_maps           field       dbr_fields.field_id
        enum_map.enum_id                 field_maps          enum        enum.enum_id
        managed_values.row_id            values              row         managed_rows.id
        managed_values.field_id          managed_values      field       dbr_fields.field_id
        managed_rows.table_id            managed_rows        table       dbr_tables.table_id
        indexes.field_id                 indexes             field       dbr_fields.field_id
        indexes.refinement_of_id         refinements         refinement_of  indexes.id
    };

    my (@fake_schemas, @fake_tables, @fake_fields, @fake_relationships);
    my (%table_ids, %field_ids);

    push @fake_schemas, { schema_id => -$ver, handle => "dbr_meta_$ver", display_name => "dbr_meta_$ver" };

    while (my ($verlist, $table_name, $field_name, $type_info) = splice(@COL_INFO, 0, 4)) {
        next if $verlist !~ /\b$ver\b/;

        my $finfo = { is_pkey => 0, name => $field_name, display_name => undef, index_type => undef, trans_id => undef, regex => undef };
        $finfo->{default_val} = $type_info =~ s/:DEF=(.*)// ? $1 : undef;
        $finfo->{is_nullable} = $type_info =~ s/:NN$// ? 0 : 1;
        $finfo->{is_signed}   = $type_info =~ s/:UN$// ? 0 : 1;
        $finfo->{max_value}   = $type_info =~ s/:(\d+)$// ? $1 : 0;
        $finfo->{data_type}   = DBR::Config::Field->get_type_id($type_info);

        $finfo->{table_id} = $table_ids{ $table_name };
        unless ($finfo->{table_id}) {
            $finfo->{table_id} = $table_ids{ $table_name } = $NEXT_METACIRC_ID--;
            push @fake_tables, { table_id => $finfo->{table_id}, schema_id => -$ver, name => $table_name, display_name => undef, is_cachable => undef };
            $finfo->{is_pkey} = 1;
        }

        $finfo->{field_id} = $NEXT_METACIRC_ID--;
        $field_ids{"$table_name.$field_name"} = [@{$finfo}{'table_id','field_id'}];
        push @fake_fields, [@{$finfo}{ qw'field_id table_id name data_type is_nullable is_signed is_pkey trans_id max_value regex default_val' }];
    }

    while (my ($from, $back, $fore, $to) = splice(@REL_INFO, 0, 4)) {
        $from = $field_ids{$from} || next;
        $to = $field_ids{$to} || next;
        push @fake_relationships, { relationship_id => $NEXT_METACIRC_ID--, from_name => $back, from_table_id => $from->[0], from_field_id => $from->[1], to_name => $fore, to_table_id => $to->[0], to_field_id => $to->[1], type => 2 }; #CHILDOF
    }

    return { relationships => \@fake_relationships, tables => \@fake_tables, fields => \@fake_fields, schemas => \@fake_schemas };
}

###################### BEGIN OBJECT ORIENTED CODE ######################

sub new {
  my( $package ) = shift;
  my %params = @_;
  my $self = {
	      session     => $params{session},
	     };

  bless( $self, $package );

  return $self->_error('session is required') unless $self->{session};

  if ($params{schema_id}){
	$self->{schema_id} = $params{schema_id};
  }elsif($params{handle}){
	$self->{schema_id} = $SCHEMAS_BY_HANDLE{ $params{handle} } or return $self->_error("handle $params{handle} is invalid");
  }else{
	return $self->_error('schema_id is required');
  }

  return $self->_error("schema_id $self->{schema_id} is not defined") unless $SCHEMAS_BY_ID{ $self->{schema_id} };

  return( $self );
}

sub get_table{
      my $self  = shift;
      my $tname = shift or return $self->_error('name is required');
      my $inst = $_[0] && ref($_[0]) ? shift()->guid : shift() || -1;

      my $table_id = $TABLES_BY_NAME{ $self->{schema_id} } -> { $tname } || return $self->_error("table $tname does not exist");

      my $table = DBR::Config::Table->new(
					  session   => $self->{session},
					  table_id => $table_id,
                                          instance_id => $inst,
					 ) or return $self->_error('failed to create table object');
      return $table;
}

sub tables{
      my $self  = shift;
      my $inst = $_[0] && ref($_[0]) ? shift()->guid : shift() || -1;

      my @tables;

      foreach my $table_id (    values %{$TABLES_BY_NAME{ $self->{schema_id}} }   ) {

	    my $table = DBR::Config::Table->new(
						session   => $self->{session},
						table_id => $table_id,
                                                instance_id => $inst,
					       ) or return $self->_error('failed to create table object');
	    push @tables, $table;
      }


      return  wantarray ? @tables : \@tables;
}

sub get_instance{
      my $self  = shift;
      my $class = shift || 'master';
      my $tag   = shift;
      $tag = '' if !defined($tag);
      
      my $lu = $INSTANCE_LOOKUP{ $self->{schema_id} } || {};
      my $guid = $lu->{$tag}{$class} || $lu->{''}{$class} or return $self->_error("instance " . $self->handle . "-$class-$tag does not exist");

      my $instance = DBR::Config::Instance->lookup(
						   session => $self->{session},
						   guid    => $guid,
						  ) or return $self->_error('failed to create table object');
      return $instance;
}

sub instances{
      my $self  = shift;

      my @instances;

      foreach my $classref ( values %{$INSTANCE_LOOKUP{ $self->{schema_id}} }   ) {
            foreach my $guid ( values %$classref ){
                  my $instance = DBR::Config::Instance->lookup(
                                                               session => $self->{session},
                                                               guid    => $guid,
                                                              ) or return $self->_error('failed to create instance object');
                  push @instances, $instance;
            }
      }


      return wantarray ? @instances : \@instances;
}



sub schema_id {
      my $self = shift;
      return $self->{schema_id};
}

sub handle {
      my $self = shift;
      my $schema = $SCHEMAS_BY_ID{ $self->{schema_id} } or return $self->_error( 'lookup failed' );
      return $schema->{handle};
}

sub display_name {
      my $self = shift;
      my $schema = $SCHEMAS_BY_ID{ $self->{schema_id} } or return $self->_error( 'lookup failed' );
      return $schema->{display_name} || '';
}

1;
