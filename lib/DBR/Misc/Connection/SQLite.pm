package DBR::Misc::Connection::SQLite;

use strict;
use base 'DBR::Misc::Connection';

sub required_config_fields {   [ qw(dbfile) ]   };

sub getSequenceValue{
      my $self = shift;
      my $call = shift;

      my ($insert_id)  = $self->{dbh}->func('last_insert_rowid');
      return $insert_id;

}

sub can_lock { 0 }

sub _index_info {
    my ($self, $inst) = @_;

    local $self->{dbh}->{RaiseError} = 1;
    my @out;

    for my $t (@{ $self->table_info($inst) }) {
        for my $i (@{ $self->{dbh}->selectall_arrayref("PRAGMA index_list(".$self->quote_identifier($t->{TABLE_NAME}).")", {Slice=>{}}) }) {
            next if $i->{name} =~ /^sqlite_/;
            for my $c (@{ $self->{dbh}->selectall_arrayref("PRAGMA index_info(".$self->quote_identifier($i->{name}).")", {Slice=>{}}) }) {
                push @out, { TABLE_NAME => $t->{TABLE_NAME}, INDEX_NAME => $i->{name}, NON_UNIQUE => $i->{unique}?0:1, COLUMN_NAME => $c->{name}, SUB_PART => undef, _seq => $c->{seqno} };
            }
        }
    }

    @out = sort { $a->{TABLE_NAME} cmp $b->{TABLE_NAME} || $a->{INDEX_NAME} cmp $b->{INDEX_NAME} || $a->{_seq} <=> $b->{_seq} } @out;
    return \@out;
}

sub _table_info {
    my $self = shift;

    return [ grep { $_->{TABLE_NAME} !~ /^sqlite_/ } @{ $self->SUPER::_table_info(@_) } ];
}

1;
