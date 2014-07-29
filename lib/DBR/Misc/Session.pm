package DBR::Misc::Session;

use strict;
use base 'DBR::Common';
use DateTime::TimeZone;
use Carp;

sub new {
      my( $package ) = shift;

      my %params = @_;
      my $self = {
		  logger   => $params{logger},
		  admin    => $params{admin} ? 1 : 0,
		  fudge_tz => $params{fudge_tz},
		  use_exceptions => $params{use_exceptions} ? 1 : 0,
                  tag      => defined($params{tag}) ? $params{tag} : ''
		 };

      bless( $self, $package );

      croak ('logger is required') unless $self->{logger};

      my $tz = '';
      $self->{tzref} = \$tz;
      $self->timezone('server') or confess "failed to initialize timezone";

      return $self;
}

sub tag{
      my $self = shift;
      if(exists $_[0]){
            my $set = shift;
            return $self->{tag} = defined($set) ? $set : '';
      }
      return $self->{tag};
}

# an opaque integer which is stored in change data capture records
sub user_id {
    return @_ > 1 ? ($_[0]{user_id} = $_[1]) : ($_[0]{user_id});
}

sub cdc_log_shipping_sub {
    return @_ > 1 ? ($_[0]{cdc_log_shipping_sub} = $_[1]) : ($_[0]{cdc_log_shipping_sub});
}

sub query_time_mode { @_ > 1 ? ($_[0]{query_time_mode} = $_[1]) : ($_[0]{query_time_mode}) }
sub query_start_time { @_ > 1 ? ($_[0]{query_start_time} = $_[1]) : ($_[0]{query_start_time}) }
sub query_end_time { @_ > 1 ? ($_[0]{query_end_time} = $_[1]) : ($_[0]{query_end_time}) }
sub query_selected_time { @_ > 1 ? ($_[0]{query_selected_time} = $_[1]) : ($_[0]{query_selected_time}) }
sub query_cache { @_ > 1 ? ($_[0]{query_cache} = $_[1]) : ($_[0]{query_cache}) }
sub cdc_mock_time { @_ > 1 ? ($_[0]{cdc_mock_time} = $_[1]) : ($_[0]{cdc_mock_time}) }
sub time_breakpoint_queue { @_ > 1 ? ($_[0]{time_breakpoint_queue} = $_[1]) : ($_[0]{time_breakpoint_queue}) }

# just a conservative little thing to avoid blatant duplication in the output of query_history without causing too many errors
sub __deepeq {
    my ($x,$y) = @_;
    return 1 if !defined($x) && !defined($y);
    return 0 if !defined($x) || !defined($y);
    return 0 if ref($x) ne ref($y);
    return ($x eq $y) if !ref($x);
    return 1 if Scalar::Util::refaddr($x) == Scalar::Util::refaddr($y);
    if (ref($x) eq 'ARRAY') {
        return 0 if @$x != @$y;
        for (0 .. $#$x) {
            __deepeq($x->[$_],$y->[$_]) or return 0;
        }
        return 1;
    }
    if (ref($x) eq 'HASH') {
        return 0 if keys(%$x) != keys(%$y);
        for (keys %$x) {
            exists($y->{$_}) or return 0;
            __deepeq($x->{$_},$y->{$_}) or return 0;
        }
        return 1;
    }
    return 0;
}

sub query_point_in_time {
    my ($self, $time, $fn) = @_;

    $time = int $time;
    local($self->{query_time_mode}) = 1;
    local($self->{query_start_time}) = local($self->{query_end_time}) = local($self->{query_selected_time}) = $time;
    local($self->{time_breakpoint_queue}) = {};

    return $fn->();
}

sub query_history {
    my ($self, $from, $to_ex, $fn) = @_;

    $from = int($from);
    $to_ex = int($to_ex);

    local($self->{query_time_mode}) = 1;
    local($self->{query_start_time}) = $from;
    local($self->{query_end_time}) = $to_ex - 1;
    local($self->{time_breakpoint_queue}) = { $to_ex > $from ? ($from => 1) : () };
    local($self->{query_cache}) = {};

    my %datapoints;

    while (1) {
        my @test = grep { $_ >= $from && $_ < $to_ex && !exists($datapoints{$_}) } keys %{$self->{time_breakpoint_queue}};
        %{$self->{time_breakpoint_queue}} = ();

        last unless @test;

        for my $t (@test) {
            local $self->{query_selected_time} = $t;
            $datapoints{$t} = $fn->();
        }
    }

    my @out;
    for my $time (sort { $a <=> $b } keys %datapoints) {
        if (!@out || !__deepeq($datapoints{$time}, $out[-1]{value})) {
            $out[-1]{end} = $time if @out;
            push @out, { start => $time, value => $datapoints{$time} };
        }
    }
    $out[-1]{end} = $to_ex;

    return wantarray ? @out : \@out;
}

# change data recorder.  this isn't the right place but I'm not sure what is
sub record_change_data {
    my ($self, @logs) = @_;

    my (%insts, %dedup);

    for my $l (@logs) {
        my $inst = $insts{$l->{ihandle}}{$l->{itag}} ||= DBR::Config::Instance->lookup( session => $self, handle => $l->{ihandle}, tag => $l->{itag} ) or croak('failed to lookup instance');
        push @{$dedup{$inst} ||= [$inst]}, $l;
    }

    for my $lst (values %dedup) {
        my $inst = shift @$lst;
        $inst->_record_change_data(@$lst);
    }
}


sub _sync_cdc { !$_[0]{cdc_log_shipping_sub} }

sub timezone {
      my $self = shift;
      my $tz   = shift;

      return ${$self->{tzref}} unless defined($tz);

      if($tz eq 'server' ){
	    eval {
		  my $tzobj = DateTime::TimeZone->new( name => 'local');
		  $tz = $tzobj->name;
	    };
	    if($@){
		  if($self->{fudge_tz}){
			$self->_log( "Failed to determine local timezone. Fudging to UTC");
			$tz = 'UTC';
		  }else{
			return $self->_error( "Failed to determine local timezone ($@)" );
		  }
	    }
      }

      DateTime::TimeZone->is_valid_name( $tz ) or return $self->_error( "Invalid Timezone '$tz'" );

      $self->_logDebug2('Set timezone to ' . $tz);

      return ${$self->{tzref}} = $tz;
}
sub timezone_ref{ $_[0]->{tzref} }

sub is_admin{ $_[0]->{admin} }
sub use_exceptions{ $_[0]->{use_exceptions} }

sub _session { $_[0] }

sub _log{
      my $self    = shift;
      my $message = shift;
      my $mode    = shift;

      my ( undef,undef,undef, $method) = caller(2);
      $self->{logger}->log($message,$method,$mode);

      return 1;
}

sub _directlog{
      my $self = shift;
      my $message = shift;
      my $method  = shift;
      my $mode    = shift;

      $self->{logger}->log($message,$method,$mode)
}

1;
