from logging import INFO
from pathlib import Path

from trilogy import Dialects, Environment, Executor
from trilogy.core.enums import Derivation, Granularity, Purpose
from trilogy.core.env_processor import concept_to_node, generate_graph
from trilogy.core.exceptions import ModelValidationError
from trilogy.core.models.author import Grain
from trilogy.core.models.build import BuildDatasource, BuildGrain
from trilogy.core.models.core import DataType
from trilogy.core.processing.discovery_utility import is_pushdown_aliased_concept
from trilogy.core.processing.node_generators.node_merge_node import (
    determine_induced_minimal_nodes,
)
from trilogy.hooks import DebuggingHook
from trilogy.parser import parse_text
from trilogy.parsing.render import Renderer

ROOT = Path(__file__).parent

"""WITH 
datasource_lvs_info_base as (
SELECT
    "lvs_info"."LV_Name" as "name",
    "lvs_info"."LV_Variant" as "variant",
    "lvs_info"."Stage_Name" as "stage_name",
    sum(1) as "grain_check"
FROM
    "lvs_info"
GROUP BY 
    "lvs_info"."LV_Name",
    "lvs_info"."LV_Variant",
    "lvs_info"."Stage_Name")
SELECT
    "datasource_lvs_info_base"."name" as "name",
    "datasource_lvs_info_base"."variant" as "variant",
    "datasource_lvs_info_base"."stage_name" as "stage_name",
    "datasource_lvs_info_base"."grain_check" as "grain_check"
FROM
    "datasource_lvs_info_base"
WHERE
    "datasource_lvs_info_base"."grain_check" > 1

ORDER BY 
    CASE
        WHEN "datasource_lvs_info_base"."name" is null THEN 1
        ELSE 0
        END desc
LIMIT (100)"""


def test_environment(gcat_env):
    DebuggingHook()

    gcat_env.parse_text(
        """import launch;
"""
    )
    try:
        gcat_env.validate_environment()
    except ModelValidationError as e:
        for x in e.children or []:
            raise x
    assert "year" in gcat_env.environment.concepts["launch_date.year"].datatype.traits


def test_case(gcat_env: Executor):
    base = gcat_env
    with open(ROOT / "fuel_dashboard.preql", "r") as f:
        raw = f.read()
        _, statements = parse_text(raw, environment=base.environment)
    assert len(statements) == 5
    rendered = Renderer().render_statement_string(statements)
    _, statements = parse_text(rendered, environment=base.environment)
    rendered = Renderer().render_statement_string(statements)
    assert "True" not in rendered, rendered


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
    assert """WHEN cast(sum("launch_info"."OrbPay") as int) = 0 THEN 1""" in sql[0], sql[0]


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
        struct(  min(launch_date)->first_launch,  max(launch_date)->last_launch) as launch_date_range,
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


def test_environment_cleanup_multiselect():
    """
    Test to ensure that the environment cleanup works correctly.
    """
    env = Environment(
        working_path=Path(__file__).parent,
    )
    from trilogy.hooks import INFO, DebuggingHook

    DebuggingHook(INFO)
    base = Dialects.DUCK_DB.default_executor(environment=env)
    base.parse_text(
        """import satcat;
auto launches <- count(jcat?  owner.code = 'PLAN') by launch_date;
auto decoms <- count(jcat ? decom_date is not null and owner.code = 'PLAN' ) by decom_date;

key launch_spine <- date_spine(date_add(current_date(), day, -6000), current_date());
key decom_spine <- date_spine(date_add(current_date(), day, -6000), current_date());

merge launch_date into ~launch_spine;
merge decom_date into ~decom_spine;
        """
    )
    pre_concepts = set(base.environment.concepts.keys())
    queries = base.parse_text(
        """
select
    launch_spine,
    sum launches order by launch_spine asc as cumulative_launches,
having
    cumulative_launches >1
merge
select
    decom_spine,
    sum decoms order by decom_spine asc as cumulative_decoms,
having cumulative_decoms >1
align date:launch_spine,decom_spine;
        """
    )

    query = queries[-1]
    assert "local.date" in query.locally_derived
    assert base.environment.concepts["local.date"].datatype == DataType.DATE
    assert "local.date" in query.locally_derived
    assert "local.date.month" in base.environment.concepts
    for c in query.locally_derived:
        base.environment.remove_concept(c)
    post_concepts = set(base.environment.concepts.keys())
    assert (
        pre_concepts == post_concepts
    ), f"Environment cleanup did not remove locally derived concepts: {post_concepts - pre_concepts}"

    # check we can materialize it safely
    assert "local.date.month" not in base.environment.concepts
    base.environment.materialize_for_select()


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
        environment=test_env,
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
        environment=test_env,
    )

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
    assert '"launch_info"."FirstAgency"' in sql[0], sql[0]
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


def test_full_join_issue_2():
    from trilogy.hooks import DebuggingHook

    DebuggingHook()

    env = Environment(
        working_path=Path(__file__).parent,
    )
    base = Dialects.DUCK_DB.default_executor(environment=env)
    queries = base.parse_text(
        """
import launch_dashboard;

select 

   payload_label,
      --  vehicle.name,
    orbit_code,
    payload.jcat.count as payload_count,
    --rank vehicle.name order by sum(orb_pay) by vehicle.name desc  as vehicle_rank
having
    vehicle_rank = 1
limit 50;

        """
    )
    sql = base.generate_sql(queries[-1])
    assert "1=1" not in sql[0], sql[0]


def test_join_discovery():
    from trilogy.hooks import DebuggingHook

    DebuggingHook()

    env = Environment(
        working_path=Path(__file__).parent,
    )
    base = Dialects.DUCK_DB.default_executor(environment=env)
    queries = base.parse_text(
        """import launch_dashboard;

where
  org.flag = 'abc123'
SELECT
    count(vehicle.family) by __preql_internal.all_rows -> all_vehicles,
LIMIT 1
;
"""
    )
    sql = base.generate_sql(queries[-1])
    assert "1=1" not in sql[0], sql[0]


def test_join_discovery_two():
    from trilogy.hooks import DebuggingHook

    DebuggingHook()

    env = Environment(
        working_path=Path(__file__).parent,
    )
    base = Dialects.DUCK_DB.default_executor(environment=env)
    queries = base.parse_text(
        """import launch_dashboard;

SELECT
    org.flag,
    vehicle.name,
    vehicle.variant,

LIMIT 1
;
"""
    )
    sql = base.generate_sql(queries[-1])
    assert (
        '"vehicle_lv_info" on "cheerful"."vehicle_name" = "vehicle_lv_info"."LV_Name" AND "cheerful"."vehicle_variant" = "vehicle_lv_info"."LV_Variant"'
        in sql[0]
    ), sql[0]


def test_should_group(gcat_env: Executor):
    from trilogy.core.models.build import BuildGrain
    from trilogy.hooks import DebuggingHook

    DebuggingHook()

    base = gcat_env
    queries = base.parse_text(
        """import launch;


SELECT
    vehicle.stage.engine.group,
    sum(orb_pay) as payload_to_orbit_one,
    sum(group orb_pay by launch_tag, vehicle.stage.engine.group) as  payload_to_orbit,
    launch_count,
    count(group launch_tag by vehicle.stage.engine.group) as launch_count_two,
    count_distinct(launch_tag) as launches
order by launch_count desc limit 15;
"""
    )
    build_env = base.environment.materialize_for_select()
    validation_components = "local.launch_tag,vehicle.name,vehicle.stage.engine.name,vehicle.stage.name,vehicle.variant".split(
        ","
    )
    pregrain = BuildGrain.from_concepts(validation_components, environment=build_env)
    assert "vehicle.stage.engine.name" not in pregrain.components, pregrain
    base.generate_sql(queries[-1])
    results = base.execute_query(queries[-1])
    for row in results.fetchall():
        assert row.launches == row.launch_count, row


def test_flag(gcat_env: Executor):
    from trilogy.hooks import DebuggingHook

    DebuggingHook()

    queries = gcat_env.parse_text(
        """import launch;

        select org.flag;
        """
    )
    gcat_env.generate_sql(queries[-1])
    results = gcat_env.execute_query(queries[-1])
    assert len(results.fetchall()) == 4


def test_array_agg(gcat_env: Executor):

    from trilogy.hooks import DebuggingHook

    DebuggingHook()

    queries = gcat_env.parse_text(
        """import launch;


SELECT
    # launch_count,
    # sum(orb_pay) as payload_to_orbit,
    # round(avg(group(vehicle.stage.engine.isp) by flight_id),2) -> average_isp,
    array_agg(
       struct(
        sum(orb_pay) by vehicle.stage.engine.fuel->payload,

        vehicle.stage.engine.fuel -> fuel
        )
    ) as fuel_payloads
;"""
    )

    # gcat_env.generate_sql(queries[-1])
    # assert len(gcat_env.environment.concepts['fuel_readout'].lineage.concept_arguments) == 2, gcat_env.environment.concepts['fuel_readout'].lineage.concept_arguments
    results = gcat_env.execute_query(queries[-1])
    assert len(results.fetchall()) == 1


def test_parenthetical_basic_parentheses(gcat_env: Executor):

    from trilogy.hooks import DebuggingHook

    DebuggingHook()

    queries = gcat_env.parse_text(
        """
import fuel_dashboard;
import std.display;

SELECT
    vehicle.stage.engine.fuel,
    @calc_percent(
        count(launch_tag ? was_complete_success),
        count(launch_tag),
        2
        ) as success_rate,
    @calc_percent(
        count(launch_tag ? was_complete_success),
        (count(launch_tag)),
        2
        ) as success_rate2
;
"""
    )
    results = gcat_env.execute_query(queries[-1])
    for row in results.fetchall():
        assert 0 <= row.success_rate <= 1, row


def test_parenthetical_basic(gcat_env: Executor):

    from logging import INFO

    from trilogy.hooks import DebuggingHook

    DebuggingHook(level=INFO)

    queries = gcat_env.parse_text(
        """
import fuel_dashboard;
import std.display;
WHERE
    vehicle.stage_no in ('2', '3', '4') and  sum(orb_pay) by vehicle.stage.engine.fuel >0
SELECT
    array_sort(array_agg(
        struct(
        round(coalesce(sum(orb_pay) by vehicle.stage.engine.fuel,0),3)  ->payload , 
        vehicle.stage.engine.fuel  ->fuel,
        (count(launch_tag) by vehicle.stage.engine.fuel) ->success_rate 
       
         )
    ), desc) as fuel_payloads
;

"""
    )
    results = gcat_env.execute_query(queries[-1])
    seen = []
    for row in results.fetchall()[0].fuel_payloads:
        seen.append(row["fuel"])
        # assert 0 <= row["success_rate"] <= 100, row
    if len(seen) != len(set(seen)):
        raise AssertionError(f"Duplicate values in {seen}")


def test_parenthetical(gcat_env: Executor):

    from trilogy.hooks import DebuggingHook

    DebuggingHook()

    queries = gcat_env.parse_text(
        """
import fuel_dashboard;
import std.display;
WHERE
    vehicle.stage_no in ('2', '3', '4') and  sum(orb_pay) by vehicle.stage.engine.fuel >0
SELECT
    launch_count,
    sum(orb_pay) as payload_to_orbit,
    count(vehicle.full_name) as rockets,
    round(avg(group(vehicle.stage.engine.isp) by flight_id),2) -> average_isp,
    array_sort(array_agg(
        struct(
        sum(orb_pay) by vehicle.stage.engine.fuel ->payload , 
        vehicle.stage.engine.fuel  ->fuel,
        @calc_percent(
            (count(launch_tag ? was_complete_success ) by vehicle.stage.engine.fuel), 
            (count(launch_tag) by vehicle.stage.engine.fuel)
            , 2
         ) ->success_rate 
       
         )
    ), desc) as fuel_payloads
;
"""
    )
    results = gcat_env.execute_query(queries[-1])
    assert len(results.fetchall()) == 1


def test_filter_node_group_injection(gcat_env: Executor):

    from trilogy.hooks import DebuggingHook

    DebuggingHook()

    queries = gcat_env.parse_text(
        """
import fuel_dashboard;
import std.display;
select
    count(launch_tag ? vehicle.stage.engine.fuel = 'Kero' and vehicle.stage_no in ('0', '1')) as fuel_launches
limit 1;

"""
    )
    results = gcat_env.execute_query(queries[-1])
    q1 = results.fetchall()[0].fuel_launches

    queries = gcat_env.parse_text(
        """
import fuel_dashboard;
import std.display;
where vehicle.stage.engine.fuel = 'Kero' and vehicle.stage_no in ('0', '1')
select
    count(launch_tag) as fuel_launches
limit 1500;

"""
    )
    results = gcat_env.execute_query(queries[-1])
    q2 = results.fetchall()[0].fuel_launches
    assert q1 == q2, (q1, q2)


def test_aggregate_optimization(gcat_env: Executor):
    from logging import INFO

    from trilogy.hooks import DebuggingHook

    DebuggingHook(level=INFO)

    queries = gcat_env.parse_text(
        """
    import fuel_dashboard;
    datasource fuel_aggregates (
  launch_tag,
  orb_pay,
  org_state_code:org.state_code,
  org_hex:org.hex,
  vehicle_stage_engine_name:vehicle.stage.engine.name,
  vehicle_stage_engine_fuel:vehicle.stage.engine.fuel,
  vehicle_stage_engine_oxidizer: vehicle.stage.engine.oxidizer,
  vehicle_stage_engine_group:vehicle.stage.engine.group,
  stage_no: vehicle.stage_no,
  lv_type: vehicle.name,
  lv_variant: vehicle.variant,
  launch_date_year:launch_date.year,

)
grain (launch_tag, vehicle.stage_no)
address fuel_dashboard_agg;

                                  WHERE
        vehicle.stage_no in ('0', '1')
SELECT
    stage_identifier,
    org.state_code,
    org.hex,
    sum(orb_pay)-> orbital_payload,
ORDER BY
    orbital_payload desc
LIMIT 10
;
"""
    )
    query = gcat_env.generate_sql(queries[-1])

    assert (
        'LEFT OUTER JOIN "launch_info" as "launch_info" on "fuel_aggregates"."launch_tag" = "launch_info"."Launch_Tag"'
        in query[0]
    ), query[0]

    # results = gcat_env.execute_query(queries[-1])
    # q2 = results.fetchall()[0]["fuel_launches"]
    # assert q1 == q2, (q1, q2)


def test_no_duplicates(gcat_env: Executor):

    from logging import INFO

    from trilogy.hooks import DebuggingHook

    DebuggingHook(level=INFO)

    queries = gcat_env.parse_text(
        """
import fuel_dashboard;
WHERE
    vehicle.stage_no in ('2', '3', '4')
SELECT
    stage_identifier,
    org.state_code,
    org.hex,
    sum(orb_pay) -> orbital_payload,
ORDER BY
    orbital_payload desc
LIMIT 10
;

"""
    )
    del gcat_env.environment.datasources["launch_info"]
    # del gcat_env.environment.datasources['payload.launch.launch_info']
    query = gcat_env.generate_sql(queries[-1])

    fuel_aggregates: BuildDatasource = gcat_env.environment.datasources[
        "fuel_aggregates"
    ]
    assert "org.code" in fuel_aggregates.concepts

    assert "launch_info" not in query[0], query[0]


def test_big_group_by(gcat_env: Executor):
    from logging import INFO

    from trilogy.hooks import DebuggingHook

    DebuggingHook(level=INFO)
    base = gcat_env
    base.execute_raw_sql(ROOT / "setup.sql")
    queries = base.parse_text(
        """import fuel_dashboard;
        WHERE
        vehicle.stage_no in ('0', '1')

        and vehicle.stage.engine.isp >300
SELECT
    stage_identifier,
    org.state_code,
    org.hex,
    sum(orb_pay)-> orbital_payload,
ORDER BY
    orbital_payload desc
LIMIT 10
;"""
    )
    sql = base.generate_sql(queries[-1])
    assert (
        """GROUP BY 
    "fuel_aggregates"."launch_tag",
    "fuel_aggregates"."orb_pay",
    "fuel_aggregates"."org_hex",
    "fuel_aggregates"."org_state_code",
    CASE"""
        in sql[0]
    ), sql[0]


def test_wrong_global_join_agg(gcat_env: Executor):
    from logging import INFO

    from trilogy.hooks import DebuggingHook

    DebuggingHook(level=INFO)

    base = gcat_env
    queries = base.parse_text(
        """import satcat;
import std.color;


where owner.code = 'PLAN'
select

    array_agg( struct(
        bus -> bus_name, count(jcat) by bus ->bus_count
    )) as per_bus_counts,
        # count(jcat) by * as total_satellites,
;
"""
    )
    sql = base.generate_sql(queries[-1])
    # assert base.environment.concepts["per_bus_counts"].
    assert '''"highfalutin"."bus" = "quizzical"."bus"''' not in sql[0], sql[0]


def test_merge_with_filter(gcat_env: Executor):
    from logging import INFO

    from trilogy.hooks import DebuggingHook

    DebuggingHook(level=INFO)
    base = gcat_env
    queries = base.parse_text(
        """
import satcat;
where owner.code = 'PLAN'
select
launch_date, 
sum case when jcat is not null then 1 else 0 end order by launch_date asc as running_total
merge
select
decom_date,
sum case when jcat is not null then 1 else 0 end order by decom_date asc as running_decom
align
    display_date: launch_date,decom_date
;
"""
    )
    sql = base.generate_sql(queries[-1])
    results = base.execute_query(queries[-1])
    assert len(results.fetchall()) > 0, sql


def test_date_spine(gcat_env: Executor):

    DebuggingHook(level=INFO)
    base = gcat_env
    queries = base.parse_text(
        """import satcat;
const target_company <- 'PLAN';

auto launches <- count(jcat ? owner.code = target_company) by launch_date;

key chart_spine <- date_spine(date_add(current_date(), day, -60), current_date());

merge launch_date into ~chart_spine;

where launch.org.name like '%Rocket%'
select
    chart_spine,
    launches
having
    chart_spine >= date_add(current_date(), day, -60)
order by
    chart_spine asc;
    """
    )

    assert base.environment.concepts["chart_spine"].purpose == Purpose.KEY
    assert base.environment.concepts["chart_spine"].derivation == Derivation.UNNEST
    assert base.environment.concepts["chart_spine"].granularity == Granularity.MULTI_ROW
    assert (
        "local.chart_spine" in base.environment.datasources["satcat"].partial_concepts
    )
    assert Grain.from_concepts(
        [base.environment.concepts["chart_spine"]], environment=base.environment
    ).components == {
        "local.chart_spine",
    }
    build_env = base.environment.materialize_for_select()
    assert build_env.concepts["local.chart_spine"].purpose == Purpose.KEY
    assert build_env.concepts["local.chart_spine"].derivation == Derivation.UNNEST
    assert build_env.concepts["local.chart_spine"].granularity == Granularity.MULTI_ROW
    assert BuildGrain.from_concepts(
        ["local.chart_spine"], environment=build_env
    ).components == {
        "local.chart_spine",
    }
    sql = base.generate_sql(queries[-1])
    results = base.execute_query(queries[-1]).fetchall()
    assert len(results) == 61, sql


def test_date_spine_local_filter(gcat_env: Executor):

    DebuggingHook(level=INFO)

    base = gcat_env
    queries = base.parse_text(
        """import satcat;   
        import satcat;

auto launches <- count(jcat?  owner.code = 'PLAN') by launch_date;

key launch_spine <- date_spine(date_add(current_date(), day, -6000), current_date());

merge launch_date into ~launch_spine;

select
    launch_spine,
    sum launches order by launch_spine asc as cumulative_launches,
having
    cumulative_launches >1
;
"""
    )

    sql = base.generate_sql(queries[-1])
    results = base.execute_query(queries[-1]).fetchall()
    assert len(results) > 100, sql


def test_recursion_error(gcat_env: Executor):
    from logging import INFO

    from trilogy.hooks import DebuggingHook

    DebuggingHook(level=INFO)

    base = gcat_env
    queries = base.parse_text(
        """import satcat;
        def sort(x)-> x.bus_count;

select
    --count(owner.name)  as owner_count,
    case 
        when owner_count>1 then 'Many (' || owner_count::string || ')' 
        else any(owner.name) 
        end as 
    headline_name,
    count(jcat ? decom_date is null) as total_satellites,
    count(jcat ? decom_date is not null) as decommed_satellites,
    array_sort(array_agg( struct(   
        bus -> bus_name, count(jcat) by bus ->bus_count
    )),desc) as per_bus_counts,
    min(launch_date)::string as first_launch,
    max(launch_date)::string as last_launch

;
"""
    )
    headline_name = base.environment.concepts["headline_name"]
    assert headline_name.purpose == Purpose.PROPERTY

    sql = base.generate_sql(queries[-1])
    results = base.execute_query(queries[-1])
    assert len(results.fetchall()) == 1, sql


def test_extra_filter(gcat_env: Executor):
    from logging import INFO

    from trilogy.hooks import DebuggingHook

    DebuggingHook(level=INFO)

    base = gcat_env
    queries = base.parse_text(
        """import satcat;


auto launches <- count(jcat ? base_category = 'P') by launch_date;
auto decoms <- count(jcat ? decom_date is not null and base_category = 'P'  ) by decom_date;

key launch_spine <- date_spine(date_add(current_date(), day, -60000), current_date());
key decom_spine <- date_spine(date_add(current_date(), day, -60000), current_date());


merge launch_date into ~launch_spine;
merge decom_date into ~decom_spine;


select
    launch_spine,
    sum launches order by launch_spine asc as cumulative_launches,
having
    cumulative_launches >=1
merge
select
    decom_spine,
    sum decoms order by decom_spine asc as cumulative_decoms,
having cumulative_decoms >=1
align date:launch_spine,decom_spine;
"""
    )
    sql = base.generate_sql(queries[-1])
    results = base.execute_query(queries[-1])
    assert len(results.fetchall()) > 0, sql
    assert "date_add(current_date(), -60000 * INTERVAL 1 day)," in sql[0], sql[0]


def test_extra_filter_two(gcat_env: Executor):
    from logging import INFO

    from trilogy.hooks import DebuggingHook

    DebuggingHook(level=INFO)

    base_env = gcat_env.environment
    gcat_env.environment = gcat_env.environment.duplicate()
    # purge our aggregate to trigger conditions
    if "fuel_aggregates" in gcat_env.environment.datasources:
        del gcat_env.environment.datasources["fuel_aggregates"]
    queries = gcat_env.parse_text(
        """import fuel_dashboard2;

WHERE
    era = 'Apollo'
SELECT
    launch_date.year,
    vehicle.stage_no
;
"""
    )
    _ = gcat_env.generate_sql(queries[-1])

    gcat_env.environment = base_env


def test_spacex_alias_behavior():
    from trilogy.hooks import DebuggingHook

    DebuggingHook()

    env = Environment(
        working_path=Path(__file__).parent,
    )

    queries = """key x int;
   auto total <- count(x);

   select
    total as fun;
    """
    env.parse(queries)
    build_env = env.materialize_for_select()
    assert is_pushdown_aliased_concept(build_env.concepts["local.fun"])


def test_spacex_aggregates(gcat_env: Executor):
    from logging import INFO

    from trilogy.hooks import DebuggingHook

    DebuggingHook(level=INFO)

    cmd = """import launch_base;

# WHERE local.launch_date.year >= 2015 
#   AND org.name = 'SpaceX'
# SELECT
#     local.launch_date.year,
#     sum(local.launch_count) as spacex_launches
# ORDER BY local.launch_date.year ASC;

WHERE local.launch_date.year >= 2015 
  AND org.name = 'SpaceX'
SELECT
    local.launch_date.year,
    local.launch_count,
ORDER BY local.launch_date.year ASC;"""

    results = gcat_env.execute_text(cmd)[-1].fetchall()

    for row in results:
        if row.launch_date_year == 2023:
            assert row.launch_count < 100, row

    cmd = """

WHERE local.launch_date.year >= 2015 
  AND org.name = 'SpaceX'
SELECT
    local.launch_date.year,
    local.launch_count as spacex_launches,
ORDER BY local.launch_date.year ASC;"""

    results = gcat_env.execute_text(cmd)[-1].fetchall()

    build_env = gcat_env.environment.materialize_for_select()
    assert is_pushdown_aliased_concept(build_env.concepts["local.spacex_launches"])

    for row in results:
        if row.launch_date_year == 2023:
            assert row.spacex_launches < 100, row
