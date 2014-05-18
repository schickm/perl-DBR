package DBR::Migrate::Operations::ChangeTable;

use strict;
use warnings;
use parent 'DBR::Migrate::Operations::Base';

# this is a doozy, partly because of the need to combine alter tables for speed, and also because SQL doesn't let you just create a table without adding columns at the same time

# has schema
# has old_name
# has new_name
# has fields
  # has ref [for indexes, relationships]
  # has old_name
  # has old_typestr
  # has old_notional_value
  # has new_name
  # has new_typestr
  # has new_notional_value
# has indexes
  # has components
    # has ref
    # has length
  # has unique
  # has creating
# has relationships
  # has foreign_ref
  # has primary_ref [schema.table.field OR refno]
  # has forward_name
  # has reverse_name
  # has creating


1;
