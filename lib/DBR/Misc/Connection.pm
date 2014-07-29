# the contents of this file are Copyright (c) 2009 Daniel Norman
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License as
# published by the Free Software Foundation.

package DBR::Misc::Connection;

use strict;
use base 'DBR::Common';

sub required_config_fields { [qw(database hostname user password)] };

sub new {
      my( $package ) = shift;

      my %params = @_;
      my $self = {
		  session  => $params{session},
		  dbh     => $params{dbh},
		 };

      bless( $self, $package );

      return $self->_error('session is required') unless $self->{session};
      return $self->_error('dbh is required')    unless $self->{dbh};
      $self->{lastping} = time; # assume the setup of the connection as being a good ping

      return $self;
}

sub dbh     { $_[0]->{dbh} }
sub do      { my $self = shift;  return $self->_wrap($self->{dbh}->do(@_))       }
sub prepare { my $self = shift;  return $self->_wrap($self->{dbh}->prepare(@_))  }
sub execute { my $self = shift;  return $self->_wrap($self->{dbh}->execute(@_))  }
sub selectrow_array { my $self = shift;  return $self->_wrap($self->{dbh}->selectrow_array(@_))  }
sub disconnect { my $self = shift; return $self->_wrap($self->{dbh}->disconnect(@_))  }
sub quote { shift->{dbh}->quote(@_)  }
sub quote_identifier { shift->{dbh}->quote_identifier(@_) }
sub can_lock { 1 }

sub ping {
      my $self = shift;

      #$self->_logDebug3('PING'); # Logging is inefficient
      return 1 if $self->{lastping} + 2 > time; # only ping every 5 seconds

      #$self->_logDebug3('REAL PING'); # Logging is inefficient
      $self->{dbh}->ping or return undef;
      $self->{lastping} = time;
      return 1;
}

# if you throw an exception or call back into DBR (including to add hooks) from a rollback hook,
# DBR is not guaranteed to do anything remotely useful.
sub add_rollback_hook {
    my ($self, $hook, @args) = @_;

    return unless $self->{_intran};
    # need to maintain temporary compatibility with some downstream code that pokes directly into the lists to implement hook deduplication; will modify it O(soon) to use the API
    push @{ $self->{_on_rollback} ||= [] }, @args ? [$hook, @args] : $hook;
}

sub add_pre_commit_hook {
    my ($self, $hook, @args) = @_;

    return $hook->(@args) unless $self->{_intran};
    push @{ $self->{_pre_commit} ||= [] }, @args ? [$hook, @args] : $hook;
}

sub add_post_commit_hook {
    my ($self, $hook, @args) = @_;

    return $hook->(@args) unless $self->{_intran};
    push @{ $self->{_post_commit} ||= [] }, @args ? [$hook, @args] : $hook;
}

sub begin {
      my $self = shift;
      return $self->_error('Transaction is already open - cannot begin') if $self->{'_intran'};

      $self->_logDebug('BEGIN');
      $self->{dbh}->do('BEGIN') or return $self->_error('Failed to begin transaction');
      $self->{_intran} = 1;

      return 1;
}

my %HOOK_PRIORITY;

sub set_hook_priority {
    my ($pkg, $sub, $prio) = @_;
    $HOOK_PRIORITY{ $sub } = $prio;
}

sub _run_hooks {
    my ($self, $list, $lifo) = @_;

    my (%todo, %dedup);

    while (1) {
        while ($list && @$list) {
            my $ent = shift(@$list);
            my ($sub, @args) = ref($ent) eq 'CODE' ? $ent : @$ent;
            my $p = $HOOK_PRIORITY{$sub} || '500';
            push @{$todo{$p}}, $sub unless $dedup{$sub};
            push @{$dedup{$sub}}, @args;
        }

        return unless %todo;
        my ($p) = sort keys %todo;
        my $plist = $todo{$p};

        my $sub = $lifo ? pop(@$plist) : shift(@$plist);
        delete $todo{$p} if !@$plist;
        my $args = delete $dedup{$sub};
        $sub->($lifo ? reverse(@$args) : @$args);
    }
}

sub commit{
      my $self = shift;
      return $self->_error('Transaction is not open - cannot commit') if !$self->{'_intran'};

      $self->_logDebug('COMMIT');

      my $precommit = $self->{_pre_commit};
      $self->_run_hooks( $precommit );

      $self->{dbh}->do('COMMIT') or return $self->_error('Failed to commit transaction');

      $self->{_intran} = 0;

      my $postcommit = $self->{_post_commit};
      $self->{_on_rollback} = $self->{_pre_commit} = $self->{_post_commit} = undef;
      $self->_run_hooks( $postcommit );

      return 1;
}

sub rollback{
      my $self = shift;
      return $self->_error('Transaction is not open - cannot rollback') if !$self->{'_intran'};

      $self->_logDebug('ROLLBACK');
      $self->{dbh}->do('ROLLBACK') or return $self->_error('Failed to rollback transaction');

      $self->{_intran} = 0;

      my $hooks = $self->{_on_rollback};
      $self->{_on_rollback} = $self->{_pre_commit} = $self->{_post_commit} = undef;
      $self->_run_hooks( $hooks, 1 );

      return 1;
}

sub _cdc_capture {
    my ($self, %p) = @_;
    # oldversion, new, old, table

    my $ship = $self->{session}->cdc_log_shipping_sub;

    my $inst = $p{table}->sql_instance;
    my $obj = {
        user_id => $self->{session}->user_id, HANDLER => $ship, SESSION => $self->{session},
        table => $p{table}->name, ihandle => $inst->handle, itag => $inst->tag || '',
        new => $p{new}, old => $p{old},
    };

    if ($ship) {
        $self->add_post_commit_hook(\&__cdc_ship, $obj);
    } else {
        $self->add_pre_commit_hook(\&__cdc_ship, $obj);
    }
}

sub __cdc_ship {
    my (@logs) = @_;

    my $handler = $logs[0]->{HANDLER};
    my $session = $logs[0]->{SESSION};

    my $txtime = $session->cdc_mock_time || time;
    for my $l (@logs) {
        delete $l->{HANDLER};
        delete $l->{SESSION};
        $l->{time} = $txtime;
    }

    if ($handler) {
        $handler->(@logs);
    } else {
        $session->record_change_data(@logs);
    }
}

######### ability check stubs #########

sub can_trust_execute_rowcount{ 0 }

############ sequence stubs ###########
sub prepSequence{
      return 1;
}
sub getSequenceValue{
      return -1;
}
#######################################

sub b_intrans{ $_[0]->{_intran} ? 1:0 }
sub b_nestedTrans{ 0 }

sub quiet_next_error{
      my $self = shift;

      $self->{dbh}->{PrintError} = 0;

      return 1;
}

sub _wrap{
      my $self = shift;

      #reset any variables now
      $self->{dbh}->{PrintError} = 1;

      return wantarray?@_:$_[0];
}
1;
