# Grammar Overview

```
 !start: ( block | show_statement )*
    block: statement _TERMINATOR PARSE_COMMENT?
    ?statement: concept
    | datasource
    | function
    | multi_select_statement
    | select_statement
    | persist_statement
    | rowset_derivation_statement
    | import_statement
    | copy_statement
    | merge_statement
    | rawsql_statement
    
    _TERMINATOR:  ";"i /\s*/
```

Full grammar in trilogy.lark.
