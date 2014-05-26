package t::lib::Test;

use Test::More;
use DBR;
use DBR::Util::Logger;
use DBR::Config::ScanDB;
use DBR::Config::SpecLoader;
use DBR::Config::Schema;
use File::Path;
use DBR::Sandbox;

our @EXPORT = qw(connect_ok setup_schema_ok dump_instance load_instance);
our $VERSION = '1';

use base 'Exporter';

# Delete temporary files
sub clean {
	#unlink( 'test-subject-db.sqlite' );
	#unlink( 'test-config-db.sqlite'  );
}

# Clean up temporary test files both at the beginning and end of the
# test script.
BEGIN { clean() }
END   { clean() }

sub connect_ok {
        my $attr = { @_ };
        my $dbfile = delete $attr->{dbfile} || ':memory:';
        my @params = ( "dbi:SQLite:dbname=$dbfile", '', '' );
        if ( %$attr ) {
            push @params, $attr;
        }
        my $dbh = DBI->connect( @params );
        Test::More::isa_ok( $dbh, 'DBI::db' );
        return $dbh;
}

sub setup_schema_ok{
    my $testid = shift;
    my %params = @_;

    my $dbr = DBR::Sandbox->provision( schema => $testid, version => $params{version} || 1, quiet => 1 );
    
    Test::More::ok( $dbr, 'Setup Schema' );
    return $dbr;
}

sub dump_instance {
    my ($inst) = @_;
    my $conn = $inst->getconn;

    my $sch = $conn->flush_schema->schema_info($inst);

    my @out;
    for my $k (sort keys %$sch) {
        my $tbl = $sch->{$k};
        my $cols = $tbl->{columns};
        my @col_list = sort keys %$cols;
        my @column_parts;
        my @fk_parts;
        my @pk_parts;
        my @indexes;
        my @pkey = map("Q($_)", grep($cols->{$_}{is_pkey},values %$cols));
        for my $cn (@col_list) {
            my $ci = $cols->{$cn};
            my $col = "Q($cn) $ci->{type}";
            if ($ci->{decimal_digits}) {
                $col .= "($ci->{max_value},$ci->{decimal_digits})";
            } elsif ($ci->{max_value}) {
                $col .= "($ci->{max_value})";
            }
            if (!$ci->{is_signed}) {
                $col .= " UNSIGNED";
            }
            if (!$ci->{is_nullable} && !$ci->{is_pkey}) {
                $col .= " NOT NULL";
            }
            if ($ci->{is_pkey} && @pkey == 1) {
                $col .= " PRIMARY KEY";
                $col .= " AUTOINCREMENT" if $ci->{type} =~ /INT/;
            }
            push @column_parts, $col;
            if ($ci->{ref_table}) {
                push @fk_parts, "FOREIGN KEY (Q($cn)) REFERENCES ".($ci->{ref_dbname} ? "Q($ci->{ref_dbname}).Q($ci->{ref_table})" : "Q($ci->{ref_table})")." (Q($ci->{ref_field}))";
            }
        }

        push @pk_parts, "PRIMARY KEY (".join(', ',@pkey).")" if @pkey > 1;

        for my $ix (values %{$tbl->{indexes}}) {
            my @bits;
            for my $p (@{$ix->{parts}}) {
                push @bits, "Q($p->{column})" . ($p->{prefix_length} ? " ($p->{prefix_length})" : "");
            }
            my $unique = $ix->{unique} ? " UNIQUE" : "";
            push @indexes, "CREATE$unique INDEX Q() ON TBL($k) (".join(', ',@bits).");";
        }

        @indexes = sort @indexes;
        my $ii = 0;
        map { s/Q\(\)/Q(${k}__${ii})/; $ii++ } @indexes;

        push @out, "CREATE TABLE TBL($k) (".join(', ',@column_parts,@pk_parts,@fk_parts).");";
        push @out, @indexes;

        for my $row (@{ $inst->connect->select( -table => $k, -fields => \@col_list ) }) {
            my @fld;
            my @val;

            for my $c (@col_list) {
                push @fld, "Q($c)";
                push @val, $cols->{$c}->{type} =~ /INT/ ? $row->{$c} : $conn->quote($row->{$c});
            }

            push @out, "INSERT INTO TBL($k) (".join(', ',@fld).") VALUES (".join(', ',@val).");";
        }
    }

    return @out;
}

sub load_instance {
    my ($inst, @sql) = @_;

    my $conn = $inst->getconn;
    local $conn->dbh->{RaiseError} = 1;
    for my $tbl (keys %{$inst->phys_schema}) {
        $conn->dbh->do('DROP TABLE '.$conn->table_ref($inst,$tbl));
    }
    $conn->flush_schema;

    for my $stmt (@sql) {
        $stmt =~ s{TBL\((\w+)\)}{ $conn->table_ref($inst, $1) }eg;
        $stmt =~ s{Q\((\w+)\)}{ $conn->quote_identifier($1) }eg;
        $stmt =~ s/AUTOINCREMENT/AUTO_INCREMENT/g if $inst->module eq 'Mysql';
        $stmt =~ s/(\([0-9, ]+\)) (UNSIGNED)/$2 $1/g if $inst->module ne 'Mysql';
        $conn->dbh->do($stmt);
    }
}

1;
