package DBR::Query::RecMaker;

use strict;
use base 'DBR::Common';
use Symbol qw( qualify_to_ref delete_package);
use DBR::Query::RecHelper;
use DBR::Query::Part;

#IDPOOL is a revolving door of package ids... we do this to guard against memory leaks... juuust in case
my @IDPOOL = (1..200);
my $classidx = 200; #overflow

my $BASECLASS = 'DBR::_R';

sub new {
      my( $package ) = shift;
      my %params = @_;
      my $self = {
		  logger   => $params{logger},
		  instance => $params{instance},
		  query    => $params{query},
		  rowcache => $params{rowcache},
		 };

      bless( $self, $package ); # BS object

      $self->{logger}   or return $self->_error('logger is required');
      $self->{instance} or return $self->_error('instance object must be specified');
      $self->{query}    or return $self->_error('query is required');
      $self->{rowcache} or return $self->_error('rowcache is required');

      $self->{scope} = $self->{query}->scope  or return $self->_error('failed to fetch scope object');

      $self->{classidx} = (shift @IDPOOL) || ++$classidx;
      #print STDERR "PACKAGEID: $self->{classidx}\n";

      $self->_prep or return $self->_error('prep failed');

      return $self;
}

sub class { $_[0]->{recordclass} }

sub _prep{
      my $self = shift;

      my $class = $BASECLASS . $self->{classidx};
      $self->{recordclass} = $class;

      my $fields = $self->{query}->fields or return $self->_error('Failed to get query fields');
      $fields = [ @$fields ]; #Shallow clone

      my @table_ids;
      # It's important that we preserve the specific field objects from the query. They have payloads that new ones do not.
      foreach my $field (@$fields){
	    my $field_id = $field->field_id or next; # Anon fields have no field_id
	    my $table_id = $field->table_id;
	    $self->{fieldmap}->{ $field_id } = $field;

	    push @table_ids, $table_id;
      }

      my %tablemap;
      my %pkmap;
      my %flookup;
      my @allrelations;
      foreach my $table_id ($self->_uniq( @table_ids )){

	    my $table = DBR::Config::Table->new(
						logger   => $self->{logger},
						table_id => $table_id,
					       ) or return $self->_error('Failed to create table object');

	    my $allfields = $table->fields or return $self->_error('failed to retrieve fields for table');

	    my @pk;
	    #We need to check to make sure that all PK fields are included in the query results.
	    #These are field objects, but don't use them elsewhere. They are devoid of query indexes
	    foreach my $checkfield (@$allfields){
		  my $field = $self->{fieldmap}->{ $checkfield->field_id };

		  if( $checkfield->is_pkey ){
			if(!$field){
			      return $self->_error('Resultset is missing primary key field ' . $checkfield->name);
			}

			push @pk, $field;
		  }else{
			if(!$field){
			      push @$fields, $checkfield; #not in the resultset, but we should still know about it
			      $self->{fieldmap}->{ $checkfield->field_id } = $checkfield;
			}
		  }
		  $field ||= $checkfield;

		  $flookup{$field->name} = $field;
	    }

	    $tablemap{$table_id} = $table;
	    $pkmap{$table_id}    = \@pk;

	    my $relations = $table->relations or return $self->_error('failed to retrieve relations for table');
	    push @allrelations, @$relations;

      }


      my $helper = DBR::Query::RecHelper->new(
					      logger   => $self->{logger},
					      instance => $self->{instance},
					      tablemap => \%tablemap,
					      pkmap    => \%pkmap,
					      flookup  => \%flookup,
					      scope    => $self->{scope},
					      lastidx  => $self->{query}->lastidx,
					      rowcache => $self->{rowcache},
					     ) or return $self->_error('Failed to create RecHelper object');

      my $mode = 'rw';
      foreach my $field (@$fields){
	    my $mymode = $mode;
	    $mymode = 'ro' if $field->is_readonly or $self->{instance}->is_readonly;
	    $self->_mk_accessor(
				mode  => $mymode,
				index => $field->index,
				field => $field,
				helper => $helper,
			       ) or return $self->_error('Failed to create accessor');
      }

      foreach my $relation (@allrelations){
	    $self->_mk_relation(
				relation => $relation,
				helper   => $helper,
			       ) or return $self->_error('Failed to create relation');
      }

      $self->_mk_method(
			method => 'set',
			helper => $helper,
		       ) or $self->_error('Failed to create set method');

      $self->_mk_method(
			method => 'get',
			helper => $helper,
		       ) or $self->_error('Failed to create get method');
      return 1;
}

sub _mk_accessor{
      my $self = shift;
      my %params = @_;

      my $mode = $params{mode} or return $self->_error('Mode is required');
      my $helper = $params{helper} or return $self->_error('helper is required');

      my $field = $params{field};
      my $method = $field->name;

      my $obj      = '$_[0]';
      my $record   = $obj;
      my $setvalue = '$_[1]';
      my $value;

      my $idx = $params{index};
      if(defined $idx){ #did we actually fetch this?
	    $value = $record . '[' . $idx . ']';
      }else{
	    $value = "\$h->getfield( $record, \$f )";
      }

      my $code;
      my $trans;
      if ($trans = $field->translator){
	    $value = "\$t->forward($value)";
      }

      if($mode eq 'rw' && $field){
	    $code = "   exists( $setvalue ) ? \$h->setfield( $record, \$f, $setvalue ) : $value   ";
      }elsif($mode eq 'ro'){
	    $code = "   $value   ";
      }
      $code = "sub {$code}";
      $self->_logDebug2($code);

      my $subref = _eval_accessor($helper,$field,$trans,$code) or $self->_error('Failed to eval accessor' . $@);

      my $symbol = qualify_to_ref( $self->{recordclass} . '::' . $method );
      *$symbol = $subref;

      return 1;
}

#Seperate sub for scope cleanliness
# This creates a blend of custom written perl code, and closure.
sub _eval_accessor{
      my $h = shift;
      my $f = shift;
      my $t = shift;

      return eval shift;
}


sub _mk_relation{
      my $self = shift;
      my %params = @_;

      my $relation = $params{relation} or return $self->_error('relation is required');
      my $helper = $params{helper} or return $self->_error('helper is required');

      my $method = $relation->name;

      my $obj      = '$_[0]';
      my $record   = $obj;

      my $fids = $relation->field_ids or return $self->_error('failed to retrieve field_ids');
      return $self->_error('multifield mappings are not currently supported') if scalar(@$fids) > 1;

      my $field = $self->{fieldmap}->{ $fids->[0] } or return $self->_error("field_id '$fids->[0]' is not valid");

      my $code = "\$h->getrel( $record, \$r, \$f )";

      $code = "sub {$code}";
      $self->_logDebug2("$method = $code");

      my $subref = _eval_relation($helper,$relation,$field,$code) or $self->_error('Failed to eval relation' . $@);

      my $symbol = qualify_to_ref( $self->{recordclass} . '::' . $method );
      *$symbol = $subref;

      return 1;
}

#Seperate sub for scope cleanliness
# This creates a blend of custom written perl code, and closure.
sub _eval_relation{
      my $h = shift;
      my $r = shift;
      my $f = shift;

      return eval shift;
}


sub _mk_method{
      my $self = shift;
      my %params = @_;

      my $helper = $params{helper} or return $self->_error('helper is required');
      my $method = $params{method} or return $self->_error('method is required');

      my $obj      = 'shift';
      my $record   = $obj;

      my $code = "\$h->$method($record,\@_)";

      $code = "sub {$code}";
      $self->_logDebug2($code);

      my $subref = _eval_method($helper,$code) or $self->_error('Failed to eval method' . $@);
      my $symbol = qualify_to_ref( $self->{recordclass} . '::' . $method );
      *$symbol = $subref;

      return 1;
}

#Seperate sub for scope cleanliness
sub _eval_method{
      my $h = shift;
      return eval shift;
}

sub DESTROY{ # clean up the temporary object from the symbol table
      my $self = shift;
      $self->_logDebug2('Destroy');
      push @IDPOOL, $self->{classidx};

      my $class = $self->{recordclass};
      #print STDERR "DESTROY $class, $self->{classidx}\n";
      Symbol::delete_package($class);
}

1;



1;
