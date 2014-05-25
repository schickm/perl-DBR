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
    my ($self, $inst, $tbl) = @_;

    local $self->{dbh}->{RaiseError} = 1;
    my @out;

    for my $i (@{ $self->{dbh}->selectall_arrayref("PRAGMA index_list(".$self->quote_identifier($tbl).")", {Slice=>{}}) }) {
        next if $i->{name} =~ /^sqlite_/;
        for my $c (@{ $self->{dbh}->selectall_arrayref("PRAGMA index_info(".$self->quote_identifier($i->{name}).")", {Slice=>{}}) }) {
            push @out, { TABLE_NAME => $tbl, INDEX_NAME => $i->{name}, NON_UNIQUE => $i->{unique}?0:1, COLUMN_NAME => $c->{name}, SUB_PART => undef, _seq => $c->{seqno} };
        }
    }

    @out = sort { $a->{INDEX_NAME} cmp $b->{INDEX_NAME} || $a->{_seq} <=> $b->{_seq} } @out;
    return \@out;
}

sub _fk_info {
    my ($self, $inst, $tbl) = @_;

    local $self->{dbh}->{RaiseError} = 1;
    my @out;

    for my $i (@{ $self->{dbh}->selectall_arrayref("PRAGMA foreign_key_list(".$self->quote_identifier($tbl).")", {Slice=>{}}) }) {
        push @out, { TABLE_NAME => $tbl, COLUMN_NAME => $i->{from}, REFERENCED_TABLE_SCHEMA => '', REFERENCED_TABLE_NAME => $i->{table},
            REFERENCED_COLUMN_NAME => $i->{to} };
    }

    return \@out;
}

sub _table_info {
    my $self = shift;

    return [ grep { $_->{TABLE_NAME} !~ /^sqlite_/ } @{ $self->SUPER::_table_info(@_) } ];
}

1;
