

## Execution Plan

## Always pass up local optional
## pass up filter
Eg

where z = 2,
x, sum(y)


loop 1
start with sum(y), pass up x and condition z

loop 2
only scalar left

get x, y, u via scalar resolution


## With Complex in Where caluse

where count(z) = 2,
x, sum(y)


loop 1
start with sum(y), pass up x and condition z

loop 2
only scalar left for output
x, y

but condition has count(z) = 2

need one more loop with all concepts

loop 3:

x, y, z

get x,y,z via scalar resolution


## Where Clause With Output Condition

This is a tricky one

where count(z) = a
select
    x,
    count(z)

First pass

We need to get count(z), x AND a because we need to evaluate the condition here

so required becomes x, a, count(z) by x

loop 1:
    count(z) optional x, a no filter
