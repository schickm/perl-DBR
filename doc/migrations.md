# Concepts

## Schema

The schema refers collectively to that which the migration system works on.  It
consists of the physical MariaDB schema, plus special DBR metadata
(relationships, defaults, translators) and the contents of constants-like
tables.

## Operation

An operation is a discrete unit of change that can be performed on the
database.  Operations include table changes (including corresponding changes to
the live data), changes to the contents of constants tables, and changes to DBR
metadata.  Operations can be performed in the up (forwards) or down (reverse)
direction. Operations always fail in situations where reversibility could not
be guaranteed; for instance attempting to create a table when it already
exists.

## Migration

Is a bundle of one or more operations, together with a name, description, and
timestamp.  Migrations are the smallest unit of memory in the system – the
database stores the set of migrations which have been performed.  If a
migration fails halfway through, the operations which have been performed must
be reversed before `dbr-migrate` can exit.

## Migration sets

`dbr-migrate` maintains a database table `dbr.migrations` holding those migrations
which have been applied.  When a migration is applied or unapplied, the table
is updated.  You will also have a migration set maintained as a folder in
version control.

## Genesis migration

A virtual migration created by `dbr-migrate bootstep` containing all schemas,
tables, fields, and relationships which existed when the bootstrap was run.  It
is structurally an ordinary migration and can be used as such to populate a new
empty database system, as long as you're careful to ensure that each database
system only runs one genesis migration and never loses it.  Attempting to do so
will almost surely fail; the genesis migration cannot be rolled back if you
have any data, and trying to load a new one will fail if there are any tables
in common.

# Workflows

## Starting with migrations

To set up a migration-ready DBR system:

1. Check out this branch and install it.

2. Upgrade your DBR schema to version 2 using
`example/schemas/dbr_schema_mariadb_v1_to_v2.sql`.

3. Tell DBR about this by adding `meta_version=2` to your DBR configuration
file.

4. Run `dbr-migrate bootstrap`.  This will scan your database one last time (in
particular, populating the index meta table you just created), then mark all of
your schemas as migration-managed and add a genesis migration containing all of
the tables you had prior.

5. Run `dbr-migrate save` to put this new migration in a file.  During the
pre-deployment phase, your ability to share migrations will be limited because
each system will have its own genesis migration that cannot be shared with any
other system.  At deployment time, a genesis migration will be generated on the
production system and used to reinitialize all development systems.

6. You are done.  Note that once the genesis migration is created, `dbr-scan`
and `dbr-load-spec` will become ineffective; all changes must be done through
`dbr-migrate`.

7. To return to the previous state:

        DELETE * FROM migrations;
        UPDATE dbr_schemas SET owned_by_migration = 0;

## Updating / switching branches

Suppose you've just checked out some new code, possibly on a different branch.
It does not work because your database schema does not match what the code
expects.  To fix this, check out the corresponding version of gt-schema and
run:

    dbr-migrate -f /etc/DBR_gt.conf load /opt/gt/gt-schema/migrations

We may decide to put the directory name in `DBR_gt.conf` so that you don't have
to pass it explicitly.  Anyway, the `load` subcommand causes dbr-migrate to
cause your database to match the directory contents.  First, all migrations in
the active set which are not in the directory are unapplied, in reverse
chronological order.  Then, all migrations which are in the directory which are
not in the database are applied, in chronological order.  If it works, your
database now matches the code and you can get to work.  If any step fails, the
process is automatically backed out.

## Making changes

Technically, `load` is all you need to manage changes – you can create
migrations with a text editor and test them with `load`.  But this is likely to
get annoying, so there will probably be a number of subcommands for
automatically creating and applying migrations based on `ALTER TABLE`
specifications and `INSERT`/`UPDATE`s.  There may also be a facility for
automatically incorporating out of band changes to table contents into
migrations, for use with tables such as `feed_schema_element`; it is not clear
at this time whether that is actually possible or desirable, though.

After you have made a number of changes through this mechanism, you may wish to
commit to version control.  To this end there is a `save` command which is the
opposite of `load`, creating (and deleting – you really should only run this in
a git repository) files to make a migrations directory match your database.

It is also not clear whether it is possible or desirable to use tools such as
MySQL Workbench within this framework.

# Data representation

## Migration files

These are JSON text files, kept in the migration directory with names that
encode the UTC timestamp in the format of `YYYYMMDDHHMMSS_do_stuff.json`.  Each
contains an object with a `comments` string key and an `operations` array key.
Each operation is expressed in order in the operations list as an object whose
keys depend on the operation type.

## Database

There is a new table which holds migrations that are known to the system.  Each
can be active or inactive, and can also be crashed (unrecoverable failure
requires manual intervention).  If a migration is neither active nor crashed,
it can be removed from the table (this may be automatic).  Migration contents
are stored in JSON format as in the files.

    CREATE TABLE migrations (
            id INT(10) UNSIGNED NOT NULL AUTO\_INCREMENT PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            active TINYINT(1) UNSIGNED,
            crashed TINYINT(1) UNSIGNED,
            contents LONGBLOB
    );

## Migrations DDL

The `dbr-migrate alter` command takes input in a SQL-like language to concisely
describe changes to your schema.

    use rule :ignorecase;

    rule TOP { [ <ddl> ';' ]* }

    rule ddl:create-schema { CREATE SCHEMA <name> DISPLAY <string> }
    rule ddl:drop-schema   { DROP SCHEMA <name> }
    rule ddl:create-table  { CREATE TABLE <qualified-name> '(' <create-part> ** ',' ')' }
    rule ddl:alter-table   { ALTER TABLE <qualified-name> '(' <alter-part> ** ',' ')' }
    rule ddl:drop-table    { DROP TABLE <qualified-name }
    rule ddl:upsert        { UPSERT INTO <qualified-name> <field-list> VALUES <tuple> ** ',' }
    rule ddl:delete        { DELETE FROM <qualified-name> WITH ID '(' <integer> ** ',' ')' }

    rule create-part { <column-def> | <index-def> }
    rule column-def { <name> <type> <unsigned>? <not-null>? <translator>? <default>? <primary-key>? [DEFAULT <value>]? <auto-index>? <relationship>? }
    rule create-part:index { [UNIQUE]? INDEX '(' <index-part> ** ',' ')' }

    rule index-part { <name> [ '(' <integer> ')' ]? } # do we have a use case for ASC/DESC?
    rule index-def { [UNIQUE]? INDEX '(' <index-part> ** ',' ')' }

    rule alter-part { ADD <column-def> | ADD <index-def> | DROP <index-def> | DROP <name> | CHANGE <name> TO <column-def> | RENAME TABLE <name> }

    # PKEY = INTEGER UNSIGNED NOT NULL PRIMARY KEY
    # ID = INTEGER UNSIGNED
    # TIME = INTEGER UNSIGNED UNIXTIME
    # PERCENT = DECIMAL(5,2) UNSIGNED PERCENT }
    # MONEY = INTEGER DOLLARS
    # ENUM(...) = SMALLINT UNSIGNED NOT NULL ENUM(...)
    rule macro-type { PKEY | ID | TIME | PERCENT | MONEY | ENUM '(' <enum-option> ** ',' ')' }
    rule scalar-type { INTEGER | SMALLINT | TINYINT | MEDIUMINT | BIGINT | [ '' | TINY | MEDIUM | LONG ][ BLOB | TEXT ] }
    rule string-type { CHAR | VARCHAR | BINARY | VARBINARY }
    rule type { <macro-type> | <scalar-type> | <string-type> '(' <integer> ')' | DECIMAL '(' <integer> ',' <integer> ')' }
    rule auto-index { [UNIQUELY] INDEXED }
    rule primary-key { PRIMARY KEY } # autoincrement implied
    rule not-null { NOT NULL }
    rule unsigned { UNSIGNED }
    rule translator { DOLLARS | UNIXTIME | PERCENT | ENUM '(' <enum-option> ** ',' ')' }
    rule enum-option { <name> [ DISPLAY <string> ]? [ ID <integer> ]? }
    rule relationship { REFERENCES <double-qualified-name> [WITHOUT INDEX]? AS <name> REVERSE <name> }

    rule field-list { '(' <name> ** ',' ')' }
    rule  tuple { '(' <value> ** ',' ')' }
    token value { <string> | <integer> | NULL }

    token string        { "'" $<content>=[ [ <![']>+ | "''"  ]* ] "'" }
    token quoted-name   { '"' $<content>=[ [ <!["]>+ | '""'  ]* ] '"' }
    token integer       { -? [0-9]+ }
    token name          { <ident> | <quoted-name> }
    rule qualified-name { <name> '.' <name> }

# Internal concepts

## Physical operation

This is a small operation that directly references instances (whereas logical
operations reference schemas) and generally corresponds to a single `INSERT`,
`DELETE`, `CREATE`, `ALTER`, or `DROP` statement.  A logical operation is
executed in two steps: first a plan is generated as a sequence of physical
operations, then those operations are executed.  Most of the responsibility for
reversibility and atomicity lies at the physical operation layer (for instance
the `DROP` operation will fail if there are rows), however it is also necessary
for the logical operations to ensure plan stability.  If the generated plan
contains physical operations that remove the data required for the reversed
logical operation to generate its plan, then the logical operation will not
actually be reversible.  Beware.

## Foreign key handling

DBR uses foreign key constraints opportunistically.  They aren't required for
operation, and in some cases must be omitted.  A foreign key is created for a
relationship if the underlying database is capable of supporting it without
undue penalty; both sides of the relationship must be indexed, both instances
must be co-located (running on the same database connection; SQLite does not
allow foreign keys between databases on a single connection but we don't
currently support shared connections on SQLite anyway), and both sides must
have the same data type and size.  Foreign keys are created as these
requirements are met and deleted as they are violated.  Precise behavior
differs between (de)instantiation and normal migrations:

* For normal migrations, we expect the database to pass through a sequence of
  valid states, so the constraints are maintained throughout the process.
  `ALTER` and `DROP` operations which require constraints to be removed are
  automatically preceded with `ALTER` operations to remove them, and conversely
  for `ALTER` and `CREATE`.  It may be necessary to remove a constraint for one
  alter only, for instance if a table is being renamed or its primary key type
  changed.  This will frequently cause multiple alteration cycles; for instance
  logically adding a field and relation will generate two `ALTER` statements,
  one to add the field and one to add the foreign key.  Since these statements
  can be slow for large tables, we will investigate optimizations later.

* Instantiation of schemas creates many tables in an order not compatible with
  the foreign keys, so the foreign keys must be disabled during the process.
  This will need to be revisited if we support a database later where foreign
  key checking is mandatory.

# Command reference

## load
### --migration-dir
### --common-prefix
### --no-atomic
## save
###--migration-dir
## step
### --up
### --down
### --destructive-force
### --falsify
## ls
### --active
## alter
## squash
## bootstrap

# Plan of attack

1. Spec and implement physical operation layer
1. Spec and implement logical operations
1. Spec and implement the driver
