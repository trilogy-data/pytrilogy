
WITH 
friendly as (
SELECT
    "sales"."c" as "_s01_cid",
    sum("sales"."a") as "_s01_s_rev"
FROM
    "sales"
WHERE
    "sales"."y" = 2001

GROUP BY
    1),
quizzical as (
SELECT
    "sales"."c" as "_w02_cid",
    sum("sales"."a") as "_w02_w_rev"
FROM
    "sales"
WHERE
    "sales"."y" = 2004

GROUP BY
    1),
young as (
SELECT
    "sales"."c" as "cust",
    sum("sales"."a") as "_s02_s_rev"
FROM
    "sales"
WHERE
    "sales"."y" = 2002

GROUP BY
    1),
abhorrent as (
SELECT
    "sales"."c" as "_s02_cid",
    "sales"."p" as "_s02_spref"
FROM
    "sales"
GROUP BY
    1,
    2),
cooperative as (
SELECT
    "sales"."c" as "cust",
    sum("sales"."a") as "_w01_w_rev"
FROM
    "sales"
WHERE
    "sales"."y" = 2003

GROUP BY
    1),
uneven as (
SELECT
    "sales"."c" as "_w01_cid",
    "sales"."f" as "_w01_wfname"
FROM
    "sales"
GROUP BY
    1,
    2),
busy as (
SELECT
    "friendly"."_s01_cid" as "s01_cid",
    "friendly"."_s01_s_rev" as "s01_s_rev"
FROM
    "friendly"),
cheerful as (
SELECT
    "quizzical"."_w02_cid" as "w02_cid",
    "quizzical"."_w02_w_rev" as "w02_w_rev",
    coalesce("quizzical"."_w02_cid") as "_virt_presence_9100423883841189"
FROM
    "quizzical"),
late as (
SELECT
    "abhorrent"."_s02_cid" as "_s02_cid",
    "abhorrent"."_s02_spref" as "_s02_spref",
    "young"."_s02_s_rev" as "_s02_s_rev"
FROM
    "abhorrent"
    INNER JOIN "young" on "abhorrent"."_s02_cid" = "young"."cust"),
juicy as (
SELECT
    "cooperative"."_w01_w_rev" as "_w01_w_rev",
    "uneven"."_w01_cid" as "_w01_cid",
    "uneven"."_w01_wfname" as "_w01_wfname"
FROM
    "uneven"
    INNER JOIN "cooperative" on "uneven"."_w01_cid" = "cooperative"."cust"),
courageous as (
SELECT
    "busy"."s01_cid" as "code",
    "busy"."s01_cid" as "s01_cid",
    "busy"."s01_cid" as "w02_cid"
FROM
    "busy"),
thoughtful as (
SELECT
    "cheerful"."w02_cid" as "w02_cid",
    "cheerful"."w02_w_rev" as "w02_w_rev"
FROM
    "cheerful"
GROUP BY
    1,
    2,
    "cheerful"."_virt_presence_9100423883841189"),
macho as (
SELECT
    "late"."_s02_cid" as "s02_cid",
    "late"."_s02_s_rev" as "s02_s_rev",
    "late"."_s02_spref" as "s02_spref",
    coalesce("late"."_s02_cid") as "_virt_presence_8055547620844531"
FROM
    "late"),
vacuous as (
SELECT
    "juicy"."_w01_cid" as "w01_cid",
    "juicy"."_w01_w_rev" as "w01_w_rev",
    "juicy"."_w01_wfname" as "w01_wfname",
    coalesce("juicy"."_w01_cid") as "_virt_presence_1717560985782532"
FROM
    "juicy"),
cool as (
SELECT
    "macho"."s02_cid" as "s02_cid",
    "macho"."s02_cid" as "w02_cid",
    "macho"."s02_spref" as "pref_out",
    "macho"."s02_spref" as "s02_spref"
FROM
    "macho"),
vast as (
SELECT
    "vacuous"."w01_cid" as "w01_cid",
    "vacuous"."w01_cid" as "w02_cid",
    "vacuous"."w01_wfname" as "first_name",
    "vacuous"."w01_wfname" as "w01_wfname"
FROM
    "vacuous"),
protective as (
SELECT
    "vacuous"."_virt_presence_1717560985782532" as "_virt_presence_1717560985782532",
    "vacuous"."w01_cid" as "w01_cid",
    "vacuous"."w01_w_rev" as "w01_w_rev",
    "vacuous"."w01_wfname" as "w01_wfname"
FROM
    "vacuous"
GROUP BY
    1,
    2,
    3,
    4),
concerned as (
SELECT
    "thoughtful"."w02_w_rev" as "w02_w_rev",
    "vacuous"."w01_w_rev" as "w01_w_rev",
    "vacuous"."w01_wfname" as "w01_wfname",
    coalesce("thoughtful"."w02_cid","vacuous"."w01_cid") as "w01_cid",
    coalesce("thoughtful"."w02_cid","vacuous"."w01_cid") as "w02_cid"
FROM
    "vacuous"
    INNER JOIN "thoughtful" on "vacuous"."w01_cid" = "thoughtful"."w02_cid"),
elated as (
SELECT
    "cool"."pref_out" as "pref_out",
    "cool"."s02_spref" as "s02_spref",
    "vast"."first_name" as "first_name",
    "vast"."w01_wfname" as "w01_wfname",
    coalesce("cool"."w02_cid","courageous"."code","vast"."w02_cid") as "code",
    coalesce("cool"."w02_cid","courageous"."w02_cid","vast"."w02_cid") as "w02_cid"
FROM
    "courageous"
    INNER JOIN "cool" on "courageous"."code" = "cool"."s02_cid"
    INNER JOIN "vast" on "cool"."s02_cid" = "vast"."w01_cid" AND "courageous"."code" = "vast"."w01_cid"),
hard as (
SELECT
    "macho"."_virt_presence_8055547620844531" as "_virt_presence_8055547620844531",
    "macho"."s02_spref" as "s02_spref",
    "protective"."_virt_presence_1717560985782532" as "_virt_presence_1717560985782532",
    "protective"."w01_wfname" as "w01_wfname",
    coalesce("macho"."s02_cid","protective"."w01_cid") as "s02_cid",
    coalesce("macho"."s02_cid","protective"."w01_cid") as "w01_cid"
FROM
    "macho"
    FULL JOIN "protective" on "macho"."s02_cid" = "protective"."w01_cid"),
premium as (
SELECT
    "cheerful"."_virt_presence_9100423883841189" as "_virt_presence_9100423883841189",
    "protective"."_virt_presence_1717560985782532" as "_virt_presence_1717560985782532",
    "protective"."w01_w_rev" as "w01_w_rev",
    "protective"."w01_wfname" as "w01_wfname",
    coalesce("cheerful"."w02_cid","protective"."w01_cid") as "w01_cid",
    coalesce("cheerful"."w02_cid","protective"."w01_cid") as "w02_cid"
FROM
    "protective"
    FULL JOIN "cheerful" on "protective"."w01_cid" = "cheerful"."w02_cid"),
scrawny as (
SELECT
    "concerned"."w01_w_rev" as "w01_w_rev",
    "concerned"."w01_wfname" as "w01_wfname",
    "concerned"."w02_w_rev" as "w02_w_rev",
    "macho"."s02_s_rev" as "s02_s_rev",
    "macho"."s02_spref" as "s02_spref",
    coalesce("concerned"."w01_cid","macho"."s02_cid") as "w01_cid",
    coalesce("concerned"."w02_cid","macho"."s02_cid") as "s02_cid",
    coalesce("concerned"."w02_cid","macho"."s02_cid") as "w02_cid"
FROM
    "concerned"
    INNER JOIN "macho" on "concerned"."w01_cid" = "macho"."s02_cid"),
sedate as (
SELECT
    "hard"."_virt_presence_1717560985782532" as "_virt_presence_1717560985782532",
    "hard"."w01_wfname" as "w01_wfname",
    coalesce("cheerful"."w02_cid","hard"."s02_cid","hard"."w01_cid") as "s02_cid",
    coalesce("cheerful"."w02_cid","hard"."s02_cid","hard"."w01_cid") as "w02_cid",
    coalesce("cheerful"."w02_cid","hard"."w01_cid") as "w01_cid"
FROM
    "hard"
    FULL JOIN "cheerful" on "hard"."s02_cid" = "cheerful"."w02_cid"),
puzzled as (
SELECT
    "premium"."_virt_presence_1717560985782532" as "_virt_presence_1717560985782532",
    "premium"."_virt_presence_9100423883841189" as "_virt_presence_9100423883841189",
    "premium"."w01_w_rev" as "w01_w_rev",
    "premium"."w01_wfname" as "w01_wfname",
    coalesce("premium"."w01_cid","premium"."w02_cid","vacuous"."w01_cid") as "w01_cid",
    coalesce("premium"."w01_cid","premium"."w02_cid","vacuous"."w01_cid") as "w02_cid"
FROM
    "premium"
    FULL JOIN "vacuous" on "premium"."w02_cid" = "vacuous"."w01_cid"),
charming as (
SELECT
    "scrawny"."s02_spref" as "_virt_filter_spref_2790973759491644",
    "scrawny"."w01_wfname" as "_virt_filter_wfname_1156876110096367",
    "scrawny"."w02_cid" as "_virt_filter_cid_7186477337277761"
FROM
    "scrawny"
    INNER JOIN "busy" on "scrawny"."s02_cid" = "busy"."s01_cid"
WHERE
    ("scrawny"."w02_w_rev" - "scrawny"."w01_w_rev") / "scrawny"."w01_w_rev" > ("scrawny"."s02_s_rev" - "busy"."s01_s_rev") / "busy"."s01_s_rev"
),
resonant as (
SELECT
    "hard"."s02_spref" as "s02_spref",
    coalesce("hard"."s02_cid","hard"."w01_cid","sedate"."s02_cid","sedate"."w02_cid") as "s02_cid",
    coalesce("hard"."s02_cid","hard"."w01_cid","sedate"."s02_cid","sedate"."w02_cid") as "w02_cid",
    coalesce("hard"."w01_cid","sedate"."w01_cid") as "w01_cid",
    coalesce("hard"."w01_wfname","sedate"."w01_wfname") as "w01_wfname"
FROM
    "hard"
    FULL JOIN "sedate" on "hard"."_virt_presence_1717560985782532" is not distinct from "sedate"."_virt_presence_1717560985782532" AND "hard"."s02_cid" is not distinct from "sedate"."w02_cid" AND "hard"."w01_wfname" is not distinct from "sedate"."w01_wfname"),
yellow as (
SELECT
    "hard"."s02_spref" as "_virt_filter_spref_5098477653302484",
    coalesce("hard"."s02_cid","hard"."w01_cid","sedate"."s02_cid","sedate"."w02_cid") as "_virt_filter_cid_2894806887956596",
    coalesce("hard"."w01_wfname","sedate"."w01_wfname") as "_virt_filter_wfname_18483971005351"
FROM
    "hard"
    FULL JOIN "sedate" on "hard"."_virt_presence_1717560985782532" is not distinct from "sedate"."_virt_presence_1717560985782532" AND "hard"."s02_cid" is not distinct from "sedate"."w02_cid" AND "hard"."w01_wfname" is not distinct from "sedate"."w01_wfname"
WHERE
    "hard"."_virt_presence_8055547620844531" is not null
),
puffy as (
SELECT
    "macho"."s02_spref" as "_virt_filter_spref_408932412295862",
    "puzzled"."w01_wfname" as "_virt_filter_wfname_8573584162978190",
    coalesce("macho"."s02_cid","puzzled"."w02_cid") as "_virt_filter_cid_7120092082074110"
FROM
    "puzzled"
    LEFT OUTER JOIN "macho" on "puzzled"."w01_cid" = "macho"."s02_cid"
WHERE
    "puzzled"."_virt_presence_9100423883841189" is not null
),
rambunctious as (
SELECT
    "macho"."s02_spref" as "_virt_filter_spref_8571718182910905",
    "puzzled"."w01_wfname" as "_virt_filter_wfname_2119854692870885",
    coalesce("macho"."s02_cid","puzzled"."w02_cid") as "_virt_filter_cid_9398315168254703"
FROM
    "puzzled"
    LEFT OUTER JOIN "macho" on "puzzled"."w01_cid" = "macho"."s02_cid"
WHERE
    "puzzled"."_virt_presence_1717560985782532" is not null
),
waggish as (
SELECT
    "macho"."s02_spref" as "_virt_filter_spref_8669387930012131",
    "puzzled"."w01_wfname" as "_virt_filter_wfname_1058454116340231",
    coalesce("macho"."s02_cid","puzzled"."w02_cid") as "_virt_filter_cid_3372101147059031"
FROM
    "puzzled"
    LEFT OUTER JOIN "macho" on "puzzled"."w01_cid" = "macho"."s02_cid"
WHERE
    "puzzled"."w01_w_rev" > 0
),
dapper as (
SELECT
    "resonant"."s02_spref" as "_virt_filter_spref_3579790197618583",
    "resonant"."w01_wfname" as "_virt_filter_wfname_8844299626133138",
    coalesce("busy"."s01_cid","resonant"."w02_cid") as "_virt_filter_cid_4716803360545490"
FROM
    "resonant"
    RIGHT OUTER JOIN "busy" on "resonant"."w01_cid" = "busy"."s01_cid"
WHERE
    "busy"."s01_s_rev" > 0
)
SELECT
    "elated"."code" as "code",
    "elated"."first_name" as "first_name",
    "elated"."pref_out" as "pref_out"
FROM
    "elated"
WHERE
    "elated"."code" is not null and exists (select 1 from yellow where yellow."_virt_filter_spref_5098477653302484" is not distinct from "elated"."s02_spref" and yellow."_virt_filter_wfname_18483971005351" is not distinct from "elated"."w01_wfname" and yellow."_virt_filter_cid_2894806887956596" is not distinct from "elated"."w02_cid") and exists (select 1 from rambunctious where rambunctious."_virt_filter_spref_8571718182910905" is not distinct from "elated"."s02_spref" and rambunctious."_virt_filter_wfname_2119854692870885" is not distinct from "elated"."w01_wfname" and rambunctious."_virt_filter_cid_9398315168254703" is not distinct from "elated"."w02_cid") and exists (select 1 from puffy where puffy."_virt_filter_spref_408932412295862" is not distinct from "elated"."s02_spref" and puffy."_virt_filter_wfname_8573584162978190" is not distinct from "elated"."w01_wfname" and puffy."_virt_filter_cid_7120092082074110" is not distinct from "elated"."w02_cid") and exists (select 1 from dapper where dapper."_virt_filter_spref_3579790197618583" is not distinct from "elated"."s02_spref" and dapper."_virt_filter_wfname_8844299626133138" is not distinct from "elated"."w01_wfname" and dapper."_virt_filter_cid_4716803360545490" is not distinct from "elated"."w02_cid") and exists (select 1 from waggish where waggish."_virt_filter_spref_8669387930012131" is not distinct from "elated"."s02_spref" and waggish."_virt_filter_wfname_1058454116340231" is not distinct from "elated"."w01_wfname" and waggish."_virt_filter_cid_3372101147059031" is not distinct from "elated"."w02_cid") and exists (select 1 from charming where charming."_virt_filter_spref_2790973759491644" is not distinct from "elated"."s02_spref" and charming."_virt_filter_wfname_1156876110096367" is not distinct from "elated"."w01_wfname" and charming."_virt_filter_cid_7186477337277761" is not distinct from "elated"."w02_cid")

ORDER BY 
    "elated"."code" asc nulls first
LIMIT (100)