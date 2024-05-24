# Uniqueness examples in SQL
Suppose you have a table with (potentially) duplicate rows, for example:

    example=# select first_name, last_name, email_address from duplicates_example;
     first_name | last_name |      email_address       
    ------------+-----------+--------------------------
     Charmaine  | Doe       | charmaine@employer.com
     Charmaine  | Doe       | charmaine@employer.com
     Charmaine  | Doe       | charmaine@employer.com
     Jason      | Friedman  | jason@jasonsfriedman.com
     Jason      | Friedman  | jason@jasonsfriedman.com
     Jason      | Friedman  | jason@jasonsfriedman.com
     Someone    | Else      | someone@example.com
    (7 rows)

The task is to remove duplicate rows WITHOUT using a temporary table.

    example=# select first_name, last_name, email_address from duplicates_example;
     first_name | last_name |      email_address       
    ------------+-----------+--------------------------
     Charmaine  | Doe       | charmaine@employer.com
     Jason      | Friedman  | jason@jasonsfriedman.com
     Someone    | Else      | someone@example.com
    (3 rows)

## PostgreSQL
Use PostgreSQL's [unique CTID row identifier](https://www.postgresql.org/docs/current/ddl-system-columns.html#DDL-SYSTEM-COLUMNS-CTID):

    with keepers as (
    select
        first_name,
        last_name,
        email_address,
        max(ctid) as max_ctid
    from
        duplicates_example
    group by
        1,
        2,
        3
    )
    delete
    from
        duplicates_example
    where
        not exists (
        select
            'x'
        from
            keepers
        where
            keepers.max_ctid = duplicates_example.ctid
    );

## Other SQL dialects
| Dialect   | Options                                                                                                                       |
|-----------|-------------------------------------------------------------------------------------------------------------------------------|
| Oracle    | [ROWID]( https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/ROWID-Pseudocolumn.html) seems to be equivalent. |
| BigQuery  | `insert overwrite into duplicates_example select distinct * from duplicates_example;`                                         |
| Snowflake | `insert overwrite into duplicates_example select distinct * from duplicates_example;`                                         |
