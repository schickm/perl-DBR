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

# Workflows

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

