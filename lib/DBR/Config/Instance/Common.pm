package DBR::Config::Instance::Common;

use strict;
use base 'DBR::Common';

use Carp;

#here is a list of the currently supported databases and their connect string formats
my %connectstrings = (
		      Mysql  => 'dbi:mysql:host=-hostname-;mysql_enable_utf8=1',
		      SQLite => 'dbi:SQLite:dbname=-dbfile-',
		      Pg     => 'dbi:Pg:dbname=-database-;host=-hostname-',
		     );

my %CONCACHE;

sub flush_all_handles {
      # can be run with or without an object
      my $cache = \%CONCACHE;

      foreach my $cachekey (keys %$cache){
	    my $conn = $cache->{ $cachekey };
	    if($conn){
		  $conn->disconnect();
		  delete $cache->{ $cachekey };
	    }
      }

      return 1;
}

sub _inflate {
    my ($self, $config) = @_;

    $config->{connectstring} = $connectstrings{$config->{module}} || return $self->_error("module '$config->{module}' is not a supported database type");

    my $connclass = 'DBR::Misc::Connection::' . $config->{module};
    return $self->_error("Failed to Load $connclass ($@)") unless eval "require $connclass";

    $config->{connclass} = $connclass;

    my $reqfields = $connclass->required_config_fields or return $self->_error('Failed to determine required config fields');

    foreach my $name (@$reqfields){
        return $self->_error( $name . ' parameter is required' ) unless $config->{$name};
    }

    foreach my $key (keys %{$config}) {
        $config->{connectstring} =~ s/-$key-/$config->{$key}/;
    }

    $config->{connectid} = join "\0", $config->{connectstring}, ($config->{user} || ''), ($config->{password} || '');

    $self;
}

sub connect{
      my $self = shift;
      my $flag = shift || '';

      if (lc($flag) eq 'dbh') {
	    return $self->getconn->dbh;
      }elsif (lc($flag) eq 'conn') {
	    return $self->getconn;
      } else {
	    return DBR::Handle->new(
				    conn     => $self->getconn,
				    session  => $self->{session},
				    instance => $self,
				   ) or confess 'Failed to create Handle object';
      }
}

sub getconn{
      my $self = shift;

      my $config = $self->_config;
      my $dedup  = $config->{connectid};
      my $conn = $CONCACHE{ $dedup };

      # conn-ping-zoom!!
      return $conn if $conn && $conn->ping; # Most of the time, we are done right here

      if ($conn) {
	    $conn->disconnect();
	    $conn = $CONCACHE{ $dedup } = undef;
	    $self->_logDebug('Handle went stale');
      }

      # if we are here, that means either the connection failed, or we never had one

      $self->_logDebug2('getting a new connection');
      $conn = $self->_new_connection() or confess "Failed to connect to ${\$self->handle}, ${\$self->class}";

      $self->_logDebug2('Connected');

      return $CONCACHE{ $dedup } = $conn;
}

sub phys_schema { $_[0]->getconn->schema_info($_[0]) }

sub _new_connection{
      my $self = shift;

      my $config = $self->_config;
      my @params = ($config->{connectstring}, $config->{user}, $config->{password});

      my $dbh = DBI->connect(@params) or
	return $self->_error("Error: Failed to connect to db $config->{handle},$config->{class}");

      my $connclass = $config->{connclass};

      return $self->_error("Failed to create $connclass object") unless
	my $conn = $connclass->new(
				   session => $self->{session},
				   dbh     => $dbh
				  );

      return $conn;
}

sub is_readonly   { $_[0]->_config->{readonly} }
sub handle        { $_[0]->_config->{handle}   }
sub class         { $_[0]->_config->{class}    }
sub tag           { $_[0]->_config->{tag}    }
sub guid          { $_[0]->_config->{guid}     }
sub module        { $_[0]->_config->{module}   }
sub host          { $_[0]->_config->{hostname}      }
sub username      { $_[0]->_config->{user}      }
sub password      { $_[0]->_config->{password}      }
sub database      { $_[0]->_config->{database}      }
sub dbfile        { $_[0]->_config->{dbfile}      }
sub prefix        { $_[0]->_config->{prefix}      }
sub dbr_bootstrap { $_[0]->_config->{dbr_bootstrap} }
sub schema_id     { $_[0]->_config->{schema_id} }
sub meta_version  { $_[0]->_config->{meta_version} }
sub name          { return $_[0]->handle . ' ' . $_[0]->class }

#shortcut to fetch the schema object that corresponds to this instance
sub schema{
      my $self = shift;
      my %params = @_;

      my $schema_id = $self->schema_id || return ''; # No schemas here

      my $schema = DBR::Config::Schema->new(
					    session   => $self->{session},
					    schema_id => $schema_id,
					   ) || return $self->_error("failed to fetch schema object for schema_id $schema_id");

      return $schema;
}

1;
