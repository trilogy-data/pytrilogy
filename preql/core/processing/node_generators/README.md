


For any specialized node, unpack the specialized concept X and fetch the rest.

For unspecialized node, attempt to fetch concept X and the rest. If cannot, attempt to see 
if all combinations of others + X can be found, and return a merge node with all of those.

If not all combinations can be found, return what can be found.
