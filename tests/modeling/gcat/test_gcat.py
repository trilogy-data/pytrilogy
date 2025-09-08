from pathlib import Path

from trilogy import Dialects, Environment
from trilogy.core.env_processor import concept_to_node, generate_graph
from trilogy.core.models.core import DataType
from trilogy.core.processing.node_generators.node_merge_node import (
    determine_induced_minimal_nodes,
)
from trilogy.hooks import DebuggingHook

ROOT = Path(__file__).parent


def test_environment():
    DebuggingHook()
    env = Environment(
        working_path=Path(__file__).parent,
    )
    base = Dialects.DUCK_DB.default_executor(environment=env)
    base.execute_raw_sql(ROOT / "setup.sql")
    base.parse_text(
        """import launch;
"""
    )
    # base.validate_environment()


def test_join():
    DebuggingHook()
    env = Environment(
        working_path=Path(__file__).parent,
    )
    base = Dialects.DUCK_DB.default_executor(environment=env)

    queries = base.parse_text(
        """import launch;


where vehicle.name like '%Falcon%'
select 
platform.class,
# platform.name,
vehicle.name,
count(launch_tag) as launches;"""
    )

    sql = base.generate_sql(queries[-1])
    assert "FULL JOIN" in sql[0], sql[0]


def test_date_filter():

    DebuggingHook()
    env = Environment(
        working_path=Path(__file__).parent,
    )
    base = Dialects.DUCK_DB.default_executor(environment=env)

    queries = base.parse_text(
        """import launch;

where
  launch_date between date_sub( current_date(), YEAR, 100) and current_date()
select
  date_trunc( launch_date, year) as year,
  case
    when date_diff(date_add(current_date(), YEAR, -100), current_date(), YEAR) <= 5
    then date_trunc(launch_date, month )
    else null
  end as month,
  count( launch_tag ) as launch_count,
  sum(payload.mass::float) as total_payload_mass_kg,
  # avg(count(launch.id)) over order by year rows between 4 preceding and current row as launch_count_5yr_ma,
  # avg(sum(payload.mass_kg)) over order by year rows between 4 preceding and current row as mass_5yr_ma,
  # sum(count(launch.id)) over order by year asc as launch_count_cumulative,
  # sum(sum(payload.mass_kg)) over order by year asc as total_payload_mass_kg_cumulative
order by
  year asc
limit 2000;
"""
    )

    sql = base.generate_sql(queries[-1])
    assert "date_add(current_date(), -100 * INTERVAL 1 year)" in sql[0]


def test_case_key():
    DebuggingHook()
    env = Environment(
        working_path=Path(__file__).parent,
    )
    base = Dialects.DUCK_DB.default_executor(environment=env)

    queries = base.parse_text(
        """import launch;

key launch_filter <- CASE WHEN launch_type_code = 'O' then "Orbital"
WHEN launch_type_code = 'D' then 'Deep Space'
ELSE 'Other' END;

        SELECT 
    vehicle.name,
     vehicle.variant,
       vehicle.class, vehicle.length, vehicle.leo_capacity,
launch_count,
vehicle.family,
vehicle.launch_mass, vehicle.to_thrust,
vehicle.diameter, round(sum(orb_pay),2) as total_mass,
array_to_string(array_agg(launch_filter), ', ') as launch_targets
order by total_mass desc limit 1;
"""
    )

    sql = base.generate_sql(queries[-1])
    assert "_launch_code" in sql[0], sql[0]


def test_nested_calc_failure():
    env = Environment(
        working_path=Path(__file__).parent,
    )
    base = Dialects.DUCK_DB.default_executor(environment=env)

    queries = base.parse_text(
        """import launch_dashboard;

auto filtered_launch <- launch_tag ? success_flag = 'E';

SELECT vehicle.name,
launch_count,
count(launch_tag ? was_complete_success) as successful_launches,
count(filtered_launch) as pad_aborts,



 limit 1;

"""
    )

    sql = base.generate_sql(queries[-1])
    assert "INVALID_REFERENCE_BUG" not in sql[0], sql[0]


def test_equals_comparison():

    env = Environment(
        working_path=Path(__file__).parent,
    )
    base = Dialects.DUCK_DB.default_executor(environment=env)

    queries = base.parse_text(
        """import launch_dashboard;
        where orb_pay is not null
select
  site.state_code,
  
  log(CASE WHEN sum(orb_pay)::int = 0 then 1 else sum(orb_pay)::int END, 10) as log_scale_orbital_tons,
  launch_count
order by
  log_scale_orbital_tons desc
limit 15;
"""
    )

    sql = base.generate_sql(queries[-1])
    assert """WHEN cast(sum("wakeful"."orb_pay") as int) = 0 THEN 1""" in sql[0], sql[0]


def test_environment_cleanup():
    """
    Test to ensure that the environment cleanup works correctly.
    """
    env = Environment(
        working_path=Path(__file__).parent,
    )
    base = Dialects.DUCK_DB.default_executor(environment=env)
    base.parse_text(
        """import launch_dashboard;

        """
    )
    pre_concepts = set(base.environment.concepts.keys())
    queries = base.parse_text(
        """

    key x int;
        
auto datetime_function <- current_datetime();
auto timestamp_function <- current_timestamp();
auto date_function <- current_date();

        SELECT launch_count, 
        count(site.state_code) as countries, 
        current_datetime() as datetime_function,
        current_timestamp() as timestamp_function,
        current_date() as date_function,
        date_diff(min(launch_date), current_date(), year) as launch_days, 
        struct( first_launch -> min(launch_date), last_launch -> max(launch_date)) as launch_date_range,
        min(launch_date) as min_date;
        """
    )

    query = queries[-1]
    assert "local.datetime_function" in query.locally_derived
    assert (
        base.environment.concepts["local.datetime_function"].datatype
        == DataType.DATETIME
    )
    assert "local.datetime_function" in query.locally_derived
    for c in query.locally_derived:
        base.environment.remove_concept(c)
    base.environment.remove_concept("local.x")
    post_concepts = set(base.environment.concepts.keys())
    assert (
        pre_concepts == post_concepts
    ), f"Environment cleanup did not remove locally derived concepts: {post_concepts - pre_concepts}"

    queries = base.parse_text(
        """

select
launch_filter,
#launch_count
order by launch_filter asc
;"""
    )


def test_join_inclusion():
    """
    Test to ensure that the environment cleanup works correctly.
    """
    env = Environment(
        working_path=Path(__file__).parent,
    )
    base = Dialects.DUCK_DB.default_executor(environment=env)
    base.parse_text(
        """import launch_dashboard;

        """
    )
    queries = base.parse_text(
        """
import launch_dashboard;
where vehicle.name like '%Falcon%'

SELECT 
vehicle.full_name,
launch_count,
count(launch_tag ? was_complete_success) as successful_launches,
# count(launch_tag ? success_flag = 'E') as pad_aborts,
# count(vehicle.family) by * as all_vehicles,
# round(sum(orb_pay),2) as total_mass,
# array_to_string(array_distinct(array_agg(launch_filter)), ', ') as launch_targets
order by launch_count desc
limit 6;



        """
    )

    sql = base.generate_sql(queries[-1])
    assert (
        'LEFT OUTER JOIN "launch_info" as "launch_info" on "vehicle_lv_info"."LV_Name" = "launch_info"."LV_Type" AND "vehicle_lv_info"."LV_Variant" = "launch_info"."Variant"'
        in sql[0]
    ), sql[0]


def test_joint_join_concept_injection_components():
    from trilogy.hooks import DebuggingHook

    DebuggingHook()
    env = Environment(
        working_path=Path(__file__).parent,
    )
    base = Dialects.DUCK_DB.default_executor(environment=env)
    base.parse_text(
        """import launch;
        """
    )

    test_env = env.materialize_for_select()
    g = generate_graph(test_env)

    target_select_concepts = [
        test_env.concepts[x]
        for x in ["vehicle.class", "local.launch_tag", "vehicle.name"]
    ]
    path = determine_induced_minimal_nodes(
        g,
        nodelist=[concept_to_node(x) for x in target_select_concepts],
        accept_partial=False,
        filter_downstream=False,
        environment=env,
    )

    print(path.nodes)
    assert "c~vehicle.variant@Grain<vehicle.variant>" in path.nodes, path.nodes

    env = Environment(
        working_path=Path(__file__).parent,
    )
    base = Dialects.DUCK_DB.default_executor(environment=env)
    base.parse_text(
        """import launch;
        """
    )

    test_env = env.materialize_for_select()
    g = generate_graph(test_env)

    target_select_concepts = [
        test_env.concepts[x]
        for x in ["vehicle.class", "local.launch_tag", "vehicle.variant"]
    ]
    path = determine_induced_minimal_nodes(
        g,
        nodelist=[concept_to_node(x) for x in target_select_concepts],
        accept_partial=False,
        filter_downstream=False,
        environment=env,
    )

    print(path.nodes)
    assert "c~vehicle.name@Grain<vehicle.name>" in path.nodes, path.nodes


def test_joint_join_concept_injection():
    """
    Test to ensure that the environment cleanup works correctly.
    """
    env = Environment(
        working_path=Path(__file__).parent,
    )
    from trilogy.hooks import DebuggingHook

    DebuggingHook()
    base = Dialects.DUCK_DB.default_executor(environment=env)
    queries = base.parse_text(
        """import launch;

select vehicle.class, launch_count;
        """
    )
    sql = base.generate_sql(queries[-1])
    assert (
        'LEFT OUTER JOIN "launch_info" as "launch_info" on "vehicle_lv_info"."LV_Name" = "launch_info"."LV_Type" AND "vehicle_lv_info"."LV_Variant" = "launch_info"."Variant"'
        in sql[0]
    ), sql[0]


def test_join_transform():

    env = Environment(
        working_path=Path(__file__).parent,
    )
    from trilogy.hooks import DebuggingHook

    DebuggingHook()
    base = Dialects.DUCK_DB.default_executor(environment=env)
    queries = base.parse_text(
        """import launch;
  

WHERE
    vehicle.stage_no = '1'
    and org.state_code = 'US'
SELECT
    CASE WHEN vehicle.name != vehicle.stage.name THEN CASE WHEN vehicle.variant != '-' THEN concat(vehicle.name,'-',vehicle.variant)
ELSE vehicle.name
END
ELSE vehicle.name
END -> stage_identifier,
    sum(orb_pay) -> orbital_payload,
ORDER BY
    orbital_payload desc
LIMIT 10
;
        """
    )
    sql = base.generate_sql(queries[-1])
    assert (
        'STRING_SPLIT( "launch_info"."Agency" , \'/\' )[1] as "first_org"' in sql[0]
    ), sql[0]
    assert "BUG" not in sql[0], sql[0]


# [GEN_MERGE_NODE] Was able to resolve graph through weak component resolution - final graph [['local.category', 'local.launch_tag'], ['payload.jcat', 'payload.launch_tag']]
# [GEN_MERGE_NODE] fetching subgraphs [['local.category', 'local.launch_tag'], ['payload.jcat', 'payload.launch_tag']]


def test_full_join_issue():
    from trilogy.hooks import DebuggingHook

    DebuggingHook()

    env = Environment(
        working_path=Path(__file__).parent,
    )
    base = Dialects.DUCK_DB.default_executor(environment=env)
    queries = base.parse_text(
        """import launch_dashboard;


select
    orbit_code,
    payload.jcat.count as payload_count,
limit 50;


        """
    )
    # assert env.concepts['payl']
    sql = base.generate_sql(queries[-1])
    assert "1=1" not in sql[0], sql[0]
