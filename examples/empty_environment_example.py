

from preql import Dialects, Environment

environment = Environment()

environment.parse('''

key name string;
key gender string;
key state string;
key year int;
key name_count int;
auto name_count.sum <- sum(name_count);

datasource usa_names(
    name:name,
    number:name_count,
    year:year,
    gender:gender,
    state:state
)
address bigquery-public-data.usa_names.usa_1910_2013;

'''
)
executor = Dialects.BIGQUERY.default_executor(environment=environment)

results = executor.execute_text(
'''SELECT
    name,
    name_count.sum
order by
    name_count.sum desc
limit 10;
'''

)
# multiple queries can result from one text batch
for row in results:
    # get results for first query
    answers = row.fetchall()
    for x in answers:
        print(x)



