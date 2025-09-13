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
    split(Agency, '/')[1] FirstAgency,
    LaunchCode,
    FailCode,
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
SELECT * 

from read_csv_auto('tests/modeling/gcat/lv.cleaned.tsv',
sample_size=-1);

CREATE OR REPLACE TABLE lvs_info as
SELECT * 
from read_csv_auto('tests/modeling/gcat/lvs.cleaned.tsv',
sample_size=-1);

CREATE OR REPLACE TABLE stages as
SELECT * 
from read_csv_auto('tests/modeling/gcat/stages.cleaned.tsv',
sample_size=-1);

CREATE OR REPLACE TABLE engines AS
SELECT * EXCLUDE (Fuel), COALESCE(Fuel, 'Unspecified') AS Fuel
FROM read_csv_auto('tests/modeling/gcat/engines.cleaned.tsv',
sample_size=-1);

CREATE OR REPLACE TABLE launch_sites as
SELECT * 
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


