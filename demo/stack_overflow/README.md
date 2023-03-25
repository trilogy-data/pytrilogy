## Demo 

The demo semantic layer is based on the public biquery stack overflow dataset.

It demonstrates how to organize components, combine them for reuse, and create metrics at various grains.

View the interactive demo [here](TODO).

Or import and execute this locally.

```python
#TODO

```

## Example queries

```sql
SELECT
    user.display_name,
    user.location,
    post.avg_post_length,
    post.post_count
where 
    user.location = 'Germany'
order by
    post.post_count desc
limit 10
```