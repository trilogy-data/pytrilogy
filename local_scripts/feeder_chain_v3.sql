
WITH 
late as (
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
vacuous as (
SELECT
    "sales"."c" as "cust",
    sum("sales"."a") as "_s02_s_rev"
FROM
    "sales"
WHERE
    "sales"."y" = 2002

GROUP BY
    1),
young as (
SELECT
    "sales"."c" as "cust",
    "sales"."p" as "pref"
FROM
    "sales"
GROUP BY
    1,
    2),
thoughtful as (
SELECT
    "sales"."c" as "cust",
    sum("sales"."a") as "_w01_w_rev"
FROM
    "sales"
WHERE
    "sales"."y" = 2003

GROUP BY
    1),
abundant as (
SELECT
    "sales"."c" as "cust",
    "sales"."f" as "fname"
FROM
    "sales"
GROUP BY
    1,
    2),
scrawny as (
SELECT
    "late"."_s01_cid" as "s01_cid",
    "late"."_s01_s_rev" as "s01_s_rev"
FROM
    "late"),
wakeful as (
SELECT
    "quizzical"."_w02_cid" as "w02_cid",
    "quizzical"."_w02_w_rev" as "w02_w_rev",
    coalesce("quizzical"."_w02_cid") as "_virt_presence_9100423883841189"
FROM
    "quizzical"),
sparkling as (
SELECT
    "vacuous"."_s02_s_rev" as "_s02_s_rev",
    "vacuous"."cust" as "_s02_cid",
    "young"."pref" as "_s02_spref"
FROM
    "young"
    INNER JOIN "vacuous" on "young"."cust" = "vacuous"."cust"),
uneven as (
SELECT
    "abundant"."fname" as "_w01_wfname",
    "thoughtful"."_w01_w_rev" as "_w01_w_rev",
    "thoughtful"."cust" as "_w01_cid"
FROM
    "abundant"
    INNER JOIN "thoughtful" on "abundant"."cust" = "thoughtful"."cust"),
cheerful as (
SELECT
    "wakeful"."w02_cid" as "w02_cid",
    "wakeful"."w02_w_rev" as "w02_w_rev"
FROM
    "wakeful"
GROUP BY
    1,
    2,
    "wakeful"."_virt_presence_9100423883841189"),
abhorrent as (
SELECT
    "sparkling"."_s02_cid" as "s02_cid",
    "sparkling"."_s02_s_rev" as "s02_s_rev",
    "sparkling"."_s02_spref" as "s02_spref",
    coalesce("sparkling"."_s02_cid") as "_virt_presence_8055547620844531"
FROM
    "sparkling"),
yummy as (
SELECT
    "uneven"."_w01_cid" as "w01_cid",
    "uneven"."_w01_w_rev" as "w01_w_rev",
    "uneven"."_w01_wfname" as "w01_wfname",
    coalesce("uneven"."_w01_cid") as "_virt_presence_1717560985782532"
FROM
    "uneven"),
kaput as (
SELECT
    "yummy"."_virt_presence_1717560985782532" as "_virt_presence_1717560985782532",
    "yummy"."w01_cid" as "w01_cid",
    "yummy"."w01_w_rev" as "w01_w_rev",
    "yummy"."w01_wfname" as "w01_wfname"
FROM
    "yummy"
GROUP BY
    1,
    2,
    3,
    4),
juicy as (
SELECT
    "cheerful"."w02_w_rev" as "w02_w_rev",
    "yummy"."w01_w_rev" as "w01_w_rev",
    "yummy"."w01_wfname" as "w01_wfname",
    coalesce("cheerful"."w02_cid","yummy"."w01_cid") as "w01_cid",
    coalesce("cheerful"."w02_cid","yummy"."w01_cid") as "w02_cid"
FROM
    "yummy"
    INNER JOIN "cheerful" on "yummy"."w01_cid" = "cheerful"."w02_cid"),
puzzled as (
SELECT
    "abhorrent"."_virt_presence_8055547620844531" as "_virt_presence_8055547620844531",
    "abhorrent"."s02_spref" as "s02_spref",
    "kaput"."_virt_presence_1717560985782532" as "_virt_presence_1717560985782532",
    "kaput"."w01_wfname" as "w01_wfname",
    coalesce("abhorrent"."s02_cid","kaput"."w01_cid") as "s02_cid",
    coalesce("abhorrent"."s02_cid","kaput"."w01_cid") as "w01_cid"
FROM
    "abhorrent"
    FULL JOIN "kaput" on "abhorrent"."s02_cid" = "kaput"."w01_cid"),
divergent as (
SELECT
    "kaput"."_virt_presence_1717560985782532" as "_virt_presence_1717560985782532",
    "kaput"."w01_w_rev" as "w01_w_rev",
    "kaput"."w01_wfname" as "w01_wfname",
    "wakeful"."_virt_presence_9100423883841189" as "_virt_presence_9100423883841189",
    coalesce("kaput"."w01_cid","wakeful"."w02_cid") as "w01_cid",
    coalesce("kaput"."w01_cid","wakeful"."w02_cid") as "w02_cid"
FROM
    "kaput"
    FULL JOIN "wakeful" on "kaput"."w01_cid" = "wakeful"."w02_cid"),
sweltering as (
SELECT
    "abhorrent"."s02_s_rev" as "s02_s_rev",
    "abhorrent"."s02_spref" as "s02_spref",
    "juicy"."w01_w_rev" as "w01_w_rev",
    "juicy"."w01_wfname" as "w01_wfname",
    "juicy"."w02_w_rev" as "w02_w_rev",
    coalesce("abhorrent"."s02_cid","juicy"."w01_cid") as "w01_cid",
    coalesce("abhorrent"."s02_cid","juicy"."w02_cid") as "s02_cid",
    coalesce("abhorrent"."s02_cid","juicy"."w02_cid") as "w02_cid"
FROM
    "juicy"
    INNER JOIN "abhorrent" on "juicy"."w01_cid" = "abhorrent"."s02_cid"),
sedate as (
SELECT
    "puzzled"."_virt_presence_1717560985782532" as "_virt_presence_1717560985782532",
    "puzzled"."_virt_presence_8055547620844531" as "_virt_presence_8055547620844531",
    "puzzled"."s02_spref" as "s02_spref",
    "puzzled"."w01_wfname" as "w01_wfname",
    coalesce("puzzled"."s02_cid","puzzled"."w01_cid","scrawny"."s01_cid") as "s01_cid"
FROM
    "puzzled"
    FULL JOIN "scrawny" on "puzzled"."s02_cid" = "scrawny"."s01_cid"),
waggish as (
SELECT
    "puzzled"."_virt_presence_1717560985782532" as "_virt_presence_1717560985782532",
    "puzzled"."_virt_presence_8055547620844531" as "_virt_presence_8055547620844531",
    "puzzled"."s02_spref" as "s02_spref",
    "puzzled"."w01_wfname" as "w01_wfname",
    "wakeful"."_virt_presence_9100423883841189" as "_virt_presence_9100423883841189",
    coalesce("puzzled"."s02_cid","puzzled"."w01_cid","wakeful"."w02_cid") as "s02_cid",
    coalesce("puzzled"."s02_cid","puzzled"."w01_cid","wakeful"."w02_cid") as "w02_cid",
    coalesce("puzzled"."w01_cid","wakeful"."w02_cid") as "w01_cid"
FROM
    "puzzled"
    FULL JOIN "wakeful" on "puzzled"."s02_cid" = "wakeful"."w02_cid"),
busy as (
SELECT
    "divergent"."_virt_presence_1717560985782532" as "_virt_presence_1717560985782532",
    "divergent"."_virt_presence_9100423883841189" as "_virt_presence_9100423883841189",
    "divergent"."w01_w_rev" as "w01_w_rev",
    "divergent"."w01_wfname" as "w01_wfname",
    coalesce("divergent"."w01_cid","divergent"."w02_cid","yummy"."w01_cid") as "w01_cid",
    coalesce("divergent"."w01_cid","divergent"."w02_cid","yummy"."w01_cid") as "w02_cid"
FROM
    "divergent"
    FULL JOIN "yummy" on "divergent"."w02_cid" = "yummy"."w01_cid"),
friendly as (
SELECT
    "scrawny"."s01_cid" as "s01_cid",
    "sweltering"."s02_cid" as "s02_cid",
    "sweltering"."s02_spref" as "_virt_filter_spref_2790973759491644",
    "sweltering"."w01_cid" as "w01_cid",
    "sweltering"."w01_wfname" as "_virt_filter_wfname_1156876110096367",
    coalesce("scrawny"."s01_cid","sweltering"."w02_cid") as "_virt_filter_cid_7186477337277761",
    coalesce("scrawny"."s01_cid","sweltering"."w02_cid") as "w02_cid"
FROM
    "sweltering"
    INNER JOIN "scrawny" on "sweltering"."s02_cid" = "scrawny"."s01_cid"
WHERE
    ("sweltering"."w02_w_rev" - "sweltering"."w01_w_rev") / "sweltering"."w01_w_rev" > ("sweltering"."s02_s_rev" - "scrawny"."s01_s_rev") / "scrawny"."s01_s_rev"
),
puffy as (
SELECT
    "puzzled"."s02_spref" as "s02_spref",
    coalesce("puzzled"."s02_cid","puzzled"."w01_cid","waggish"."s02_cid","waggish"."w02_cid") as "s02_cid",
    coalesce("puzzled"."s02_cid","puzzled"."w01_cid","waggish"."s02_cid","waggish"."w02_cid") as "w02_cid",
    coalesce("puzzled"."w01_cid","waggish"."w01_cid") as "w01_cid",
    coalesce("puzzled"."w01_wfname","waggish"."w01_wfname") as "w01_wfname"
FROM
    "puzzled"
    FULL JOIN "waggish" on "puzzled"."_virt_presence_1717560985782532" is not distinct from "waggish"."_virt_presence_1717560985782532" AND "puzzled"."s02_cid" is not distinct from "waggish"."w02_cid" AND "puzzled"."w01_wfname" is not distinct from "waggish"."w01_wfname"),
rambunctious as (
SELECT
    "puzzled"."s02_spref" as "_virt_filter_spref_5098477653302484",
    coalesce("puzzled"."s02_cid","puzzled"."w01_cid","waggish"."s02_cid","waggish"."w02_cid") as "_virt_filter_cid_2894806887956596",
    coalesce("puzzled"."w01_wfname","waggish"."w01_wfname") as "_virt_filter_wfname_18483971005351"
FROM
    "puzzled"
    FULL JOIN "waggish" on "puzzled"."_virt_presence_1717560985782532" is not distinct from "waggish"."_virt_presence_1717560985782532" AND "puzzled"."s02_cid" is not distinct from "waggish"."w02_cid" AND "puzzled"."w01_wfname" is not distinct from "waggish"."w01_wfname"
WHERE
    "puzzled"."_virt_presence_8055547620844531" is not null
),
premium as (
SELECT
    "abhorrent"."s02_spref" as "_virt_filter_spref_408932412295862",
    "busy"."w01_wfname" as "_virt_filter_wfname_8573584162978190",
    coalesce("abhorrent"."s02_cid","busy"."w02_cid") as "_virt_filter_cid_7120092082074110"
FROM
    "busy"
    LEFT OUTER JOIN "abhorrent" on "busy"."w01_cid" = "abhorrent"."s02_cid"
WHERE
    "busy"."_virt_presence_9100423883841189" is not null
),
protective as (
SELECT
    "abhorrent"."s02_spref" as "_virt_filter_spref_8571718182910905",
    "busy"."w01_wfname" as "_virt_filter_wfname_2119854692870885",
    coalesce("abhorrent"."s02_cid","busy"."w02_cid") as "_virt_filter_cid_9398315168254703"
FROM
    "busy"
    LEFT OUTER JOIN "abhorrent" on "busy"."w01_cid" = "abhorrent"."s02_cid"
WHERE
    "busy"."_virt_presence_1717560985782532" is not null
),
charming as (
SELECT
    "abhorrent"."s02_spref" as "_virt_filter_spref_8669387930012131",
    "busy"."w01_wfname" as "_virt_filter_wfname_1058454116340231",
    coalesce("abhorrent"."s02_cid","busy"."w02_cid") as "_virt_filter_cid_3372101147059031"
FROM
    "busy"
    LEFT OUTER JOIN "abhorrent" on "busy"."w01_cid" = "abhorrent"."s02_cid"
WHERE
    "busy"."w01_w_rev" > 0
),
hard as (
SELECT
    "puffy"."s02_spref" as "_virt_filter_spref_3579790197618583",
    "puffy"."w01_wfname" as "_virt_filter_wfname_8844299626133138",
    coalesce("puffy"."w01_cid","puffy"."w02_cid","scrawny"."s01_cid") as "s01_cid",
    coalesce("puffy"."w02_cid","scrawny"."s01_cid") as "_virt_filter_cid_4716803360545490",
    coalesce("puffy"."w02_cid","scrawny"."s01_cid") as "w02_cid"
FROM
    "puffy"
    RIGHT OUTER JOIN "scrawny" on "puffy"."w01_cid" = "scrawny"."s01_cid"
WHERE
    "scrawny"."s01_s_rev" > 0
)
SELECT
    coalesce("friendly"."s01_cid","friendly"."w02_cid") as "code",
    "waggish"."w01_wfname" as "first_name",
    "waggish"."s02_spref" as "pref_out"
FROM
    "sedate"
    RIGHT OUTER JOIN "waggish" on "sedate"."_virt_presence_1717560985782532" is not distinct from "waggish"."_virt_presence_1717560985782532" AND "sedate"."_virt_presence_8055547620844531" is not distinct from "waggish"."_virt_presence_8055547620844531" AND "sedate"."s01_cid" is not distinct from "waggish"."w02_cid" AND "sedate"."s02_spref" is not distinct from "waggish"."s02_spref" AND "sedate"."w01_wfname" is not distinct from "waggish"."w01_wfname"
    INNER JOIN "hard" on "sedate"."s01_cid" is not distinct from "hard"."s01_cid" AND "waggish"."w02_cid" is not distinct from "hard"."s01_cid"
    INNER JOIN "friendly" on "waggish"."w02_cid" = "friendly"."s01_cid" AND coalesce("sedate"."s01_cid", "hard"."s01_cid") = "friendly"."s01_cid"
WHERE
    coalesce("friendly"."s01_cid","friendly"."w02_cid") is not null and exists (select 1 from rambunctious where rambunctious."_virt_filter_spref_5098477653302484" is not distinct from "waggish"."s02_spref" and rambunctious."_virt_filter_wfname_18483971005351" is not distinct from "waggish"."w01_wfname" and rambunctious."_virt_filter_cid_2894806887956596" is not distinct from coalesce("friendly"."s01_cid","friendly"."w02_cid")) and exists (select 1 from protective where protective."_virt_filter_spref_8571718182910905" is not distinct from "waggish"."s02_spref" and protective."_virt_filter_wfname_2119854692870885" is not distinct from "waggish"."w01_wfname" and protective."_virt_filter_cid_9398315168254703" is not distinct from coalesce("friendly"."s01_cid","friendly"."w02_cid")) and exists (select 1 from premium where premium."_virt_filter_spref_408932412295862" is not distinct from "waggish"."s02_spref" and premium."_virt_filter_wfname_8573584162978190" is not distinct from "waggish"."w01_wfname" and premium."_virt_filter_cid_7120092082074110" is not distinct from coalesce("friendly"."s01_cid","friendly"."w02_cid")) and exists (select 1 from hard where hard."_virt_filter_spref_3579790197618583" is not distinct from "waggish"."s02_spref" and hard."_virt_filter_wfname_8844299626133138" is not distinct from "waggish"."w01_wfname" and hard."_virt_filter_cid_4716803360545490" is not distinct from coalesce("friendly"."s01_cid","friendly"."w02_cid")) and exists (select 1 from charming where charming."_virt_filter_spref_8669387930012131" is not distinct from "waggish"."s02_spref" and charming."_virt_filter_wfname_1058454116340231" is not distinct from "waggish"."w01_wfname" and charming."_virt_filter_cid_3372101147059031" is not distinct from coalesce("friendly"."s01_cid","friendly"."w02_cid")) and exists (select 1 from friendly where friendly."_virt_filter_spref_2790973759491644" is not distinct from "waggish"."s02_spref" and friendly."_virt_filter_wfname_1156876110096367" is not distinct from "waggish"."w01_wfname" and friendly."_virt_filter_cid_7186477337277761" is not distinct from coalesce("friendly"."s01_cid","friendly"."w02_cid"))

GROUP BY
    1,
    2,
    3,
    "waggish"."_virt_presence_1717560985782532",
    "waggish"."_virt_presence_8055547620844531",
    "waggish"."_virt_presence_9100423883841189"
ORDER BY 
    "code" asc nulls first
LIMIT (100)