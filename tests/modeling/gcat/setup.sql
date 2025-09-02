INSTALL httpfs;

LOAD httpfs;

CREATE OR REPLACE TABLE launch_info AS 
SELECT 
    trim(Launch_Tag) Launch_Tag,
    Launch_JD,
    Launch_Date,
    LV_Type,
    Variant,
    Flight_ID,
    Flight,
    Mission,
    FlightCode,
    Platform,
    Launch_Site,
    Launch_Pad,
    Ascent_Site,
    Ascent_Pad,
    Apogee,
    Apoflag,
    Range,
    RangeFlag,
    Dest,
    OrbPay::float OrbPay,
    Agency,
    LaunchCode,
    case when FailCode= '-' then null else FailCode End FailCode,
    "Group",
    Category,
    LTCite,
    Cite,
    Notes
FROM read_csv_auto('tests/modeling/gcat/launch_cleaned.tsv',
sample_size=-1);

CREATE OR REPLACE TABLE platform_info as
SELECT * 
from read_csv_auto('tests/modeling/gcat/platforms.cleaned.tsv',
sample_size=-1);

CREATE OR REPLACE TABLE lv_info as
SELECT * EXCLUDE (LEO_Capacity, GTO_Capacity, LV_FAMILY),

trim(lv_family) as LV_Family,
cast(case when LEO_Capacity = '-' then null else LEO_Capacity END as float) LEO_Capacity, cast(case when GTO_Capacity='-' then null else GTO_CAPACITY END as float) GTO_Capacity
from read_csv_auto('tests/modeling/gcat/lv.cleaned.tsv',
sample_size=-1);


CREATE OR REPLACE TABLE launch_sites as
SELECT * EXCLUDE(latitude, longitude), cast(case when latitude='-' then null else latitude end as float) latitude, cast(case when longitude='-' then null else longitude end as float) longitude
from read_csv_auto('tests/modeling/gcat/sites.cleaned.tsv',
sample_size=-1);


CREATE OR REPLACE TABLE organizations as
SELECT *
from read_csv_auto('tests/modeling/gcat/orgs.cleaned.tsv',
sample_size=-1);

CREATE OR REPLACE TABLE satcat as
SELECT *
from read_csv_auto('tests/modeling/gcat/satcat.cleaned.tsv',
sample_size=-1);

