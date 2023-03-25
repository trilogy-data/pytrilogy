# Grammar Overview

The grammar starts with the !start rule, which matches zero or more occurrences of either a block or a comment. A block is a statement followed by an optional comment. A statement can be one of several types of declarations or expressions, including concept, datasource, select, or import_statement.

The grammar also defines rules for various constructs such as comment, concept_declaration, concept_property_declaration, datasource, window_item, order_list, and expr. The expr rule is used to match expressions, including function calls, operators, and literals.

The grammar includes rules for defining functions and aggregates such as concat, count, sum, max, and min. There are also rules for date and time functions such as fdate, fdatetime, ftimestamp, fsecond, fminute, fhour, fday, fweek, fmonth, and fyear.

The grammar also defines rules for common language constructs such as identifiers, strings, and numbers. Finally, the grammar includes a few other constructs such as metadata, limit, and logical operators.

