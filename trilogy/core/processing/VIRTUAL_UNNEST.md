# Virtual Unnest Nodes


## The Problem

We want to create a dynamic list - let's say a list of quarters. We can use an unnest or a union to create a constant field with [1,2,3,4].

Now we have data - let's say quarter:sales that doesn't have all quarters. Maybe it has {2:3, 3:4} How do we merge this?

If we run a naive merge 

```
auto quarters <- unnest([1,2,3,4]);

merge data_quarter into ~quarters;

Default discovery will say that since the unnest has no dependencies, we can just create it de-novo after fetching the sales.

So we'll end up with 

{1:3, 2:3, 3:3, 4:3, 1:4, 2:4, 3:4, 4:4}.

When we want:

{1:null,2:3, 3:4, 4: null}


## Fix

We need to regard a unnest node constant as a unique datasource.