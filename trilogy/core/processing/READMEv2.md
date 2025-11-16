

## Execution Plan


Query discovery is a recursive loop.

Order of concepts to be discovered is prioritized by lineage type, with an additional sort so that if one concept
is derived from another concept, the parent is delayed in order.

This ensures that the parent is typically included in discovery of the child node path and results in a more 'ergonomic' query.

In rare cases, a node may return partial results. Then the discovery loop will attempt to merge those.

If it cannot merge, it will attempt to discover new concepts to inject into the search path that will result in a mergable graph.

[see concept injection for more.]

## Filtering
Filtering via where clauses is always pushed up as high as possible by passing the condition object through to sourcing. 

If at any point we have a discovery loop where the contents of the where clause are included in the filtering, we need to immediately inject them.

If we never hit that point, filtering will be injected when we have only root or constant nodes left.

## Psuedonyms

Pseudonyms should always be handled by a node returning the pseudonymous type.

Ex: if A* has been merged into B*, and B* cannot be found, but A* can, the node returning A* should return A* and let the merge loop reconcile. 


To be more specific about desired behavior:

Nodes must accurately track the *actual* concept being returned, because pseudonyms can have different rendering.

Completion checks should always check for the matching concepts _or_ matching pseudonyms.

The final query should map the output label to whatever the user actually specified.

Things that might need work:
- merging concepts with partial modifiers relies on datasources being updated with partial. If you merge a calculation, we should have additional tests that upstream discoveyr of the calculation
verifies all partial constraints. 


## Canonical Addressing

Canonical addressing is our method of enabling materialized calculations to be identified early.

Canonical addresses are deterministic based on lineage. If a concept is a root, they are the normal address. If a concept is derived, they are the hash of the lineage.

This means that if someone materializes a table with UPPER(x) as fun; and you select UPPER(X) as bar; we should be able to recognize that we can satisfy your query out of that materialized address.

In discovery, the present a challenge. With no materialized derivations, the discovery loop is easy.

1. Unpack derived concepts
2. repeat until we only have roots
3. see if we can get these; if not, search for join keys to inject

But with materialized values, at 1, we may _not_ want to unpack the concept. We should treat it as a metaphorical root, and only unpack if needed. 

Solution:
Move materialized values later in discovery;

When sourcing a materialized concept, first attempt a select discovery with it treated as a root

Implementation
The first attempt at this used pseudonyms to handle this.

When a concept was materialized, create a new _root_ version, then add a pseudonym with original derivation. This was nice in that it played well with existing discovery mechanisms. 

Cons: new concept. 
[TODO: determine how to do this? ]


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
