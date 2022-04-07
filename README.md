# datajoint-extensions

Additional tools written to extend the Datajoint library for use within the Moser research
group at the Kavli Institute, NTNU.


## PopulateManager

An improved, scaling, tool to manage table population across many additional processes
without saturating the database server. Relies on Redis and the python package `rq`


## KeySourceFiller

A persistent alternative to the ephemeral `table.key_source` view. Useful for tables where
key_source calculations become excessively complicated, either due to complex logic or very
large tables. Other table logic might be required to accommodate this change (for example, 
if the key_source logic involves subtractions), and for large tables, the key_source table
_itself_ may take substantial space. 
