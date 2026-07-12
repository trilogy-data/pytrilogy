INSTALL httpfs;

LOAD httpfs;

-- Tables mirror the parquet files published to GCS by
-- https://github.com/greenmtnboy/space_reporting (the source for the
-- trilogy-public-models gcat_space model), aliased back to the original
-- GCAT TSV column names these test models bind to. Flag columns dropped
-- upstream are NULL-filled.

CREATE OR REPLACE TABLE launch_info AS
SELECT
    "id" AS Launch_Tag,
    "_launch_jd" AS Launch_JD,
    "launch_date" AS Launch_Date,
    "vehicle_name" AS LV_Type,
    "vehicle_variant" AS Variant,
    "flight_id" AS Flight_ID,
    "flight" AS Flight,
    "mission" AS Mission,
    "flight_code" AS FlightCode,
    "platform_code" AS Platform,
    "site_key" AS Launch_Site,
    "launch_pad" AS Launch_Pad,
    "ascent_site" AS Ascent_Site,
    "ascent_pad" AS Ascent_Pad,
    "apogee" AS Apogee,
    "apo_flag" AS Apoflag,
    "range" AS "Range",
    "dest" AS Dest,
    "orb_pay" AS OrbPay,
    "orgs" AS Agency,
    "org_code" AS FirstAgency,
    "_launch_code" AS LaunchCode,
    "fail_code" AS FailCode,
    "category" AS Category,
    "lt_cite" AS LTCite,
    "cite" AS Cite,
    "notes" AS Notes
FROM read_parquet('https://storage.googleapis.com/trilogy_public_models/duckdb/launch_report/launch.parquet');

CREATE OR REPLACE TABLE platform_info AS
SELECT
    "code" AS Code,
    "u_code" AS UCode,
    "state_code" AS StateCode,
    "type" AS "Type",
    "class" AS Class,
    "t_start" AS TStart,
    "t_stop" AS TStop,
    "short_name" AS ShortName,
    "name" AS "Name",
    "location" AS Location,
    "error" AS Error,
    "parent" AS Parent,
    "short_e_name" AS ShortEName,
    "e_name" AS EName,
    "v_class" AS VClass,
    "v_class_id" AS VClassID,
    "v_id" AS VID,
    "u_name" AS UName
FROM read_parquet('https://storage.googleapis.com/trilogy_public_models/duckdb/launch_report/platform_info.parquet');

CREATE OR REPLACE TABLE lv_info AS
SELECT
    "name" AS LV_Name,
    "family" AS LV_Family,
    "manufacturer" AS LV_Manufacturer,
    "variant" AS LV_Variant,
    "alias" AS LV_Alias,
    "min_stage" AS LV_Min_Stage,
    "max_stage" AS LV_Max_Stage,
    "length" AS "Length",
    NULL::VARCHAR AS LFlag,
    "diameter" AS Diameter,
    NULL::VARCHAR AS DFlag,
    "launch_mass" AS Launch_Mass,
    NULL::VARCHAR AS MFlag,
    "leo_capacity" AS LEO_Capacity,
    "gto_capacity" AS GTO_Capacity,
    "to_thrust" AS TO_Thrust,
    "class" AS Class,
    "apogee" AS Apogee,
    "range" AS "Range"
FROM read_parquet('https://storage.googleapis.com/trilogy_public_models/duckdb/launch_report/lv_info.parquet');

CREATE OR REPLACE TABLE lvs_info AS
SELECT
    "name" AS LV_Name,
    "variant" AS LV_Variant,
    "stage_no" AS Stage_No,
    "stage_name" AS Stage_Name,
    "qualifier" AS Qualifier,
    "dummy" AS Dummy,
    "multiplicity" AS Multiplicity,
    "stage_impulse" AS Stage_Impulse,
    "stage_apogee" AS Stage_Apogee,
    "stage_perigee" AS Stage_Perigee,
    "perigee_qual" AS Perigee_Qual
FROM read_parquet('https://storage.googleapis.com/trilogy_public_models/duckdb/launch_report/lvs_info.parquet');

CREATE OR REPLACE TABLE stages AS
SELECT
    "name" AS Stage_Name,
    "family" AS Stage_Family,
    "stage_manufacturer" AS Stage_Manufacturer,
    "stage_alt_name" AS Stage_Alt_Name,
    "length" AS "Length",
    "diameter" AS Diameter,
    "launch_mass" AS Launch_Mass,
    "dry_mass" AS Dry_Mass,
    "thrust" AS Thrust,
    "duration" AS Duration,
    "engine_name" AS Engine,
    "engine_count" AS NEng
FROM read_parquet('https://storage.googleapis.com/trilogy_public_models/duckdb/launch_report/stages.parquet');

CREATE OR REPLACE TABLE engines AS
SELECT
    "name" AS "Name",
    "manufacturer" AS Manufacturer,
    "family" AS Family,
    "alt_name" AS Alt_name,
    COALESCE("oxidizer", '-') AS Oxidizer,
    COALESCE("fuel", '-') AS Fuel,
    "mass" AS Mass,
    NULL::VARCHAR AS MFlag,
    "impulse" AS Impulse,
    NULL::VARCHAR AS ImpFlag,
    "thrust" AS Thrust,
    NULL::VARCHAR AS TFlag,
    "isp" AS Isp,
    NULL::VARCHAR AS IspFlag,
    "duration" AS Duration,
    NULL::VARCHAR AS DurFlag,
    "chambers" AS Chambers,
    "date" AS "Date",
    "usage" AS "Usage",
    "group" AS "Group",
    CASE COALESCE("fuel", '-')
    -- Solid Propellants (Earthy brown → tan ramp)
    WHEN 'PBAN'                     THEN '#6B361E'
    WHEN 'HTPB'                     THEN '#7A421F'
    WHEN 'Solid'                    THEN '#8C5A2B'
    WHEN 'HTPB-2013'                THEN '#9B6A35'
    WHEN 'HTPB BP-204J'             THEN '#AB7740'
    WHEN 'CTPB'                     THEN '#BB844B'
    WHEN 'HTPB/AP/Al'               THEN '#C99357'
    WHEN 'Butalane'                 THEN '#D8A266'
    WHEN 'HTPB 1814'                THEN '#E6B173'
    WHEN 'HTPB/NG/BTTN'            THEN '#F3C081'
    WHEN 'Isolane'                  THEN '#F6C99F'
    WHEN 'HTBP/AP/Al'               THEN '#F9D4B8'
    WHEN 'HTPB BP-205J'             THEN '#FBE6CF'
    WHEN 'Polyurethane/AP'          THEN '#FCECDF'
    WHEN 'Solid T9-BK-6'            THEN '#5B4C43'
    WHEN 'PBAA'                     THEN '#6A564B'
    WHEN 'HTPB BP-201J'             THEN '#7A675D'
    WHEN 'NEPE Polyethane'          THEN '#4B372E'
    WHEN 'Polyurethane'             THEN '#5A463C'
    WHEN 'PBHL/AP/Al'               THEN '#6D584D'
    WHEN 'Solid DB/AP'              THEN '#7F6458'
    WHEN 'Polysulfide'              THEN '#88905C'
    WHEN 'PEG/NG/BTTN'             THEN '#98A86C'
    WHEN 'Solid PS'                 THEN '#7D925F'
    WHEN 'Polysulphide'             THEN '#6F8254'
    WHEN 'Solid T14E1'              THEN '#5E7046'
    WHEN 'CMDB'                     THEN '#7B5CA7'  -- muted purple family (exotic solids)
    WHEN 'Plastolane'               THEN '#8E69B7'
    WHEN 'HTPB 1813'                THEN '#A082C6'
    WHEN 'HTPB/AP/Al TP-H-334'      THEN '#B49AD6'
    WHEN 'Solid?'                   THEN '#C2ACDF'
    WHEN 'AP/Al/Polyurethane'       THEN '#D0BEE8'
    WHEN 'SP Polysulphide'          THEN '#DECEF0'
    WHEN 'UP Polyurethane'          THEN '#E7DBF5'
    WHEN 'BHT'                      THEN '#F0E6F9'
    WHEN 'AP'                       THEN '#F5EFFB'
    WHEN 'Plastolite'               THEN '#FBF7FD'
    WHEN 'Polyurethane-AP'          THEN '#FDFBF7'
    WHEN 'PB'                       THEN '#FEFBFA'
    WHEN 'CTPB 16.12'               THEN '#FFF9F5'
    WHEN 'HEF-20'                   THEN '#F0E68C'  -- khaki-ish (kept as a small neutral)
    WHEN 'Polyurethane?'            THEN '#D2C38A'
    WHEN 'Solid TP-H 1205C'         THEN '#F9F0C9'
    WHEN 'E107M polyurethane'       THEN '#FAF3D2'
    WHEN 'Solid PBAA'               THEN '#FFFADF'
    WHEN 'CTPB/AP/Al'               THEN '#F5F0E1'
    WHEN 'Solid RD2435'             THEN '#FDF8EE'
    WHEN 'Polyvinyl'                THEN '#FBF5EC'
    WHEN 'Solid DB'                 THEN '#FFF7EA'
    WHEN 'CTPB/AP/AlTP-H-3062'      THEN '#FFFBF4'
    WHEN 'Solid ANP2872JM-IV'       THEN '#F0FFF0'
    WHEN 'Polyurethane DREV'        THEN '#F7FFF8'
    WHEN 'EP polyesther'            THEN '#F0FFFF'
    WHEN 'Solid RD2409'             THEN '#F0F8FF'
    WHEN 'Solid RD2410'             THEN '#E8E8FA'
    WHEN 'Solid GALCIT'             THEN '#FFF0F5'
    WHEN 'PBAA/AP'                  THEN '#FFECE9'
    WHEN 'PBAA TPH-3117'            THEN '#FFF5DF'
    WHEN 'Solid APCP'               THEN '#FFE6C9'
    WHEN 'Solid AP/Al E-107M'       THEN '#FFE9BD'
    WHEN 'Solid JPL-131'            THEN '#FFDEAD'
    WHEN 'Solid JPL-132A'           THEN '#F5DEB3'
    WHEN 'E107M'                    THEN '#FFF8E6'
    WHEN 'Solid DB N-4'             THEN '#FFFAF0'
    WHEN 'JPL 136'                  THEN '#FFFBFB'
    WHEN 'AP/Al'                    THEN '#F8F8FF'
    WHEN 'Polyurethane/AP/Al'       THEN '#F5F5F5'
    WHEN 'PS/AP'                    THEN '#EDEDED'
    WHEN 'PB/AP/Al TP-G-3012'       THEN '#DCDCDC'
    WHEN 'Solid DB T6'              THEN '#C0C0C0'
    WHEN 'Epictete'                 THEN '#A9A9A9'
    WHEN 'Solid Arcite'             THEN '#808080'
    WHEN 'Solid T13E1'              THEN '#2F2F2F'
    WHEN 'Sorbitol/Paraf/Al'        THEN '#27304B'  -- dark midnight blue-ish for certain solids
    WHEN 'SBR'                      THEN '#1F2A4A'
    WHEN 'Paraffin'                 THEN '#10203A'
    WHEN 'Polyethylene'             THEN '#003366'
    WHEN 'Polyamide'                THEN '#002244'

    -- Inert/Pressurization (purple/indigo family)
    WHEN 'N2'                       THEN '#6E61C6'
    WHEN 'Xe'                       THEN '#8679D6'
    WHEN 'Kr'                       THEN '#A095E6'

    -- Hypergolic Fuels (Red/orange family)
    WHEN 'Hydyne'                   THEN '#6C1A1A'
    WHEN 'Aniline'                  THEN '#7C2222'
    WHEN 'Aniline/Alcohol'          THEN '#8F2B2B'
    WHEN 'Aniline (ANFA)'           THEN '#9F3434'
    WHEN 'MAF-1'                    THEN '#B43D3D'
    WHEN 'Vinyl Isobutyl ethe'      THEN '#C74646'
    WHEN 'Amine'                    THEN '#D84E4E'
    WHEN 'Amine TG-02'              THEN '#E35656'
    WHEN 'Xylidiene/Trieth.'        THEN '#F04848'

    -- Hydrocarbon Fuels (amber → gold → pale ramp)
    WHEN 'Kerosene?'                THEN '#B8860B'
    WHEN 'Kerosene'                 THEN '#C28710'
    WHEN 'Kerosene JP-4'            THEN '#CFAA1E'
    WHEN 'JPX (JP-4/UDMH)'          THEN '#D9B33A'
    WHEN 'Kerosene TG-02'           THEN '#E4C35A'
    WHEN 'Gasoline/UDMH'            THEN '#EFD77A'
    WHEN 'Kerosene T-1'             THEN '#F7E59A'
    WHEN 'Diesel oil'               THEN '#FAE9B2'
    WHEN 'Turpentine'               THEN '#FCF1C9'
    WHEN 'Kero'                     THEN '#E4CE6F'
    WHEN 'Kero Sintin'              THEN '#E9D488'
    WHEN 'Kerosene RP-1'            THEN '#F0E1A0'
    WHEN 'Kero RP-1'                THEN '#F4E8B6'
    WHEN 'Kero Jet A'               THEN '#F8EFCB'
    WHEN 'Kero Naphthyl'            THEN '#FBF5E0'
    WHEN 'Kero RG-1'                THEN '#FEFBF2'
    WHEN 'Kero RJ-1'                THEN '#FFFDF6'
    WHEN 'Kero T-1'                 THEN '#FFFEFA'
    WHEN 'Kero?'                    THEN '#FFFFFE'

    -- Hypergolic/Hydrazine Compounds (dark → pale red/pink ramp)
    WHEN 'UDMH'                     THEN '#5A0E0E'
    WHEN 'UDMH?'                    THEN '#640F0F'
    WHEN 'UDMH USO'                 THEN '#6E1111'
    WHEN 'Hydrazine'                THEN '#7A1515'
    WHEN 'Hydrazine?'               THEN '#8A1A1A'
    WHEN 'MMH?'                     THEN '#9A2323'
    WHEN 'MMH'                      THEN '#AD2E2E'
    WHEN 'N2H4'                     THEN '#C03F3F'
    WHEN 'N2H4/MMH'                 THEN '#D16666'
    WHEN 'USO'                      THEN '#E79C9C'
    WHEN 'MMH-H2O'                  THEN '#F2CFCF'
    WHEN 'MON-3'                    THEN '#C94A3A'
    WHEN 'AZ-50'                    THEN '#D86B5A'
    WHEN 'UMDH'                     THEN '#E89A96'
    WHEN 'UDMH UH25'                THEN '#F6CECA'

    -- Alcohols (soft aqua family)
    WHEN 'Alcohol'                  THEN '#AEEBE3'
    WHEN 'Ethanol'                  THEN '#87D4CC'

    -- Cryogenic Fuels (cool blue ramp)
    WHEN 'Ammonia'                  THEN '#A8D4F2'
    WHEN 'Methane LNG'              THEN '#6FB8F2'
    WHEN 'Methane'                  THEN '#3F9DEB'
    WHEN 'Propane'                  THEN '#2C7FCE'
    WHEN 'LH2'                      THEN '#1F5FAF'

    -- Green Propellants (greens)
    WHEN 'RL Green Prop'            THEN '#3AAE3A'
    WHEN 'NH3OHNO3 AF-M315E'        THEN '#5CC45C'

    -- Default fallback (shouldn't be used often)
    ELSE printf(
            '#%02x%02x%02x',
            64 + abs(hash(COALESCE("fuel", '-')) % 160), -- R
            64 + abs(hash(COALESCE("fuel", '-')) % 160), -- G
            64 + abs(hash(COALESCE("fuel", '-')) % 160)  -- B
        )
    END AS fuel_hex_color,
CASE COALESCE("group", 'Unspecified')
WHEN 'Hybrid' THEN '#7E57C2' -- purple (mixed-cycle / hybrid)
WHEN 'N2' THEN '#9E9E9E' -- neutral gray (inert / pressurization)
WHEN 'NA/AA' THEN '#A51C1C' -- deep maroon (nitric acid + aromatic amine)
WHEN 'NA/Amine' THEN '#C62828' -- red (nitric acid + amine family)
WHEN 'NA/Kero' THEN '#BF6B00' -- burnt orange (nitric acid + kerosene mix)
WHEN 'NA/Turps' THEN '#D97704' -- orange (nitric acid + turpentine)
WHEN 'NA/UDMH' THEN '#8B0000' -- dark crimson (nitric acid + UDMH)
WHEN 'MonoHyd' THEN '#880E4F' -- deep magenta (monopropellant / hydrazine-like)
WHEN 'NTO/Hyd' THEN '#D84315' -- reddish-orange (NTO + hydrazine family)
WHEN 'NTO/UDMH' THEN '#BF360C' -- brick red (NTO + UDMH)
WHEN 'H2O2/Kero' THEN '#FFB300' -- amber (hydrogen peroxide + kerosene)
WHEN 'LOX/Alcohol' THEN '#42A5F5' -- light blue (LOX + alcohol)
WHEN 'LOX/UDMH' THEN '#1E88E5' -- medium blue (LOX + UDMH)
WHEN 'LOX/NH3' THEN '#26C6DA' -- cyan (LOX + ammonia)
WHEN 'LOX/Kero' THEN '#1976D2' -- royal blue (LOX + kerosene)
WHEN 'LOX/Methane' THEN '#0288D1' -- blue-cyan (LOX + methane)
WHEN 'LOX/Propane' THEN '#039BE5' -- sky blue (LOX + propane)
WHEN 'LOX/LH2' THEN '#0D47A1' -- deep navy (LOX + liquid hydrogen)
WHEN 'Green' THEN '#2E7D32' -- forest green (green propellants)
WHEN 'EP' THEN '#009688' -- teal (electric / electric-propulsion)
-- Fallback: deterministic but distinct hashed color
ELSE printf(
'#%02x%02x%02x',
48 + abs(hash(COALESCE("group", 'Unspecified')) % 200), -- R
48 + abs(hash(COALESCE("group", 'Unspecified')) % 200), -- G
48 + abs((hash(COALESCE("group", 'Unspecified')) + 97) % 200) -- B (offset for variation)
)
END AS group_hex_color
FROM read_parquet('https://storage.googleapis.com/trilogy_public_models/duckdb/launch_report/engine.parquet');

CREATE OR REPLACE TABLE launch_sites AS
SELECT
    "key" AS Site,
    "u_code" AS UCode,
    "type" AS "Type",
    "state_code" AS StateCode,
    "t_start" AS TStart,
    "t_stop" AS TStop,
    "short_name" AS ShortName,
    "name" AS "Name",
    "location" AS Location,
    "longitude",
    "latitude",
    "error" AS Error,
    "parent" AS Parent,
    "e_name" AS EName,
    "u_name" AS UName
FROM read_parquet('https://storage.googleapis.com/trilogy_public_models/duckdb/launch_report/sites.parquet');

CREATE OR REPLACE TABLE organizations AS
SELECT
    "code" AS Code,
    "u_code" AS UCode,
    "state_code" AS StateCode,
    "type" AS "Type",
    "class" AS Class,
    "t_start" AS TStart,
    "t_stop" AS TStop,
    "short_name" AS ShortName,
    "name" AS "Name",
    "location" AS Location,
    "longitude" AS Longitude,
    "latitude" AS Latitude,
    "error" AS Error,
    "parent" AS Parent,
    "short_e_name" AS ShortEName,
    "_e_name" AS EName,
    "u_name" AS UName,
    "color" AS hex_code
FROM read_parquet('https://storage.googleapis.com/trilogy_public_models/duckdb/launch_report/organizations.parquet');

CREATE OR REPLACE TABLE satcat AS
SELECT
    "JCAT",
    "Satcat",
    "Launch_Tag",
    "Piece",
    "Type",
    "Name",
    "PLName",
    "LDate",
    "Parent",
    "status_start_date" AS SDate,
    "Primary",
    "status_end_date" AS DDate,
    "Status",
    "Dest",
    "Owner",
    "State",
    "Manufacturer",
    "Bus",
    "Motor",
    "Mass",
    NULL::VARCHAR AS MassFlag,
    "DryMass",
    NULL::VARCHAR AS DryFlag,
    "TotMass",
    NULL::VARCHAR AS TotFlag,
    "Length",
    NULL::VARCHAR AS LFlag,
    "Diameter",
    NULL::VARCHAR AS DFlag,
    "Span",
    NULL::VARCHAR AS SpanFlag,
    "Shape",
    "orbit_date" AS ODate,
    "Perigee",
    NULL::VARCHAR AS PF,
    "Apogee",
    NULL::VARCHAR AS AF,
    "Inc",
    NULL::VARCHAR AS "IF",
    "OpOrbit",
    "OQUAL",
    "AltNames"
FROM read_parquet('https://storage.googleapis.com/trilogy_public_models/duckdb/launch_report/satcat_three.parquet');


-- OPTIMIZATION

CREATE OR REPLACE TABLE fuel_dashboard_agg as
SELECT
    "launch_info".Launch_Tag as launch_tag,
    "launch_info"."OrbPay" as "orb_pay",
    "vehicle_lvs_info"."LV_Name" as lv_type,
    "vehicle_lvs_info"."LV_Variant" as lv_variant,
    "launch_info"."LaunchCode" as launch_code,
    "org_organizations".code as org_code,
    "org_organizations"."StateCode" as "org_state_code",
    "org_organizations"."hex_code" as "org_hex",
    "vehicle_stage_engine_engines"."Name" as "vehicle_stage_engine_name",
    "vehicle_stage_engine_engines".fuel as vehicle_stage_engine_fuel,
    vehicle_stage_engine_engines."group" as vehicle_stage_engine_group,
    vehicle_stage_engine_engines.oxidizer as vehicle_stage_engine_oxidizer,
    "vehicle_lvs_info"."Stage_No" as "stage_no",
    "vehicle_lvs_info"."Stage_Name" as stage_name,
    date_add(date '1900-01-01', cast((cast("launch_info"."Launch_JD" as float) - 2415021) as int) * INTERVAL 1 day) as launch_date,
    year(date_add(date '1900-01-01', cast((cast("launch_info"."Launch_JD" as float) - 2415021) as int) * INTERVAL 1 day)) as launch_date_year,
    launch_jd
FROM
    "launch_info"
    INNER JOIN "organizations" as "org_organizations" on "launch_info"."FirstAgency" = "org_organizations"."Code"
    FULL JOIN "lvs_info" as "vehicle_lvs_info" on "launch_info"."LV_Type" = "vehicle_lvs_info"."LV_Name" AND "launch_info"."Variant" = "vehicle_lvs_info"."LV_Variant"
    LEFT OUTER JOIN "stages" as "vehicle_stage_stages" on "vehicle_lvs_info"."Stage_Name" = "vehicle_stage_stages"."Stage_Name"
    FULL JOIN "engines" as "vehicle_stage_engine_engines" on "vehicle_stage_stages"."Engine" = "vehicle_stage_engine_engines"."Name"
;
