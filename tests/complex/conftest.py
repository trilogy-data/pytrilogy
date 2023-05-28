from logging import DEBUG

from pytest import fixture

from preql import parse
from preql.constants import logger

logger.setLevel(DEBUG)

DECLARATIONS = r"""
key user_id int metadata(description="the description");
property user_id.display_name string metadata(description="The display name ");
property user_id.about_me string metadata(description="User provided description");


key post_id int;
property post_id.post_text string;
metric post_count <-count(post_id);
metric avg_post_length_by_post_id <- avg(len(post_text)) by post_id;

datasource posts (
    user_id: user_id,
    id: post_id,
    text: post_text
    )
    grain (post_id)
    address bigquery-public-data.stackoverflow.post_history
;

property post_length <- len(post_text);

select
    user_id,
    count(post_id) -> user_post_count
;

metric avg_user_post_count <- avg(user_post_count);


datasource users (
    id: user_id,
    display_name: display_name,
    about_me: about_me,
    )
    grain (user_id)
    address bigquery-public-data.stackoverflow.users
;


select
    user_id,
    avg(post_length)-> user_avg_post_length
;
    
select
    avg_user_post_count
;


    """


@fixture(scope="session")
def stackoverflow_environment():
    env, parsed = parse(DECLARATIONS)
    yield env
