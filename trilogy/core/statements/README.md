# Statement Design


## Assert statements
Used for DQ checks.

Unique in that we need to constrain to specific datasources/validation. 



## Concept check

# value comparison

two scalar values

assert expr from_datasource? = expr from_datasource?


assert not max(len(name.split(' '))) =1; 


assert sum(revenue) from datasource1 = sum(revenue) from datasource2;


## parallel block

begin parallel;

assert a == 1;
assert b == 2;

end parallel;


