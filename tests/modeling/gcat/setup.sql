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
FROM read_csv_auto('https://trilogy-data.github.io/trilogy-public-models/trilogy_public_models/duckdb/gcat_space/tsv/tables/launch_cleaned.tsv',
sample_size=-1);

CREATE OR REPLACE TABLE platform_info as
SELECT * 
from read_csv_auto('https://trilogy-data.github.io/trilogy-public-models/trilogy_public_models/duckdb/gcat_space/tsv/tables/platforms.cleaned.tsv',
sample_size=-1);

CREATE OR REPLACE TABLE lv_info as
SELECT * 

from read_csv_auto('https://trilogy-data.github.io/trilogy-public-models/trilogy_public_models/duckdb/gcat_space/tsv/tables/lv.cleaned.tsv',
sample_size=-1);

CREATE OR REPLACE TABLE lvs_info as
SELECT * 
from read_csv_auto('https://trilogy-data.github.io/trilogy-public-models/trilogy_public_models/duckdb/gcat_space/tsv/tables/lvs.cleaned.tsv',
sample_size=-1);

CREATE OR REPLACE TABLE stages as
SELECT * 
from read_csv_auto('https://trilogy-data.github.io/trilogy-public-models/trilogy_public_models/duckdb/gcat_space/tsv/tables/stages.cleaned.tsv',
sample_size=-1);

CREATE OR REPLACE TABLE engines AS
SELECT 
    * EXCLUDE (Fuel, isp, oxidizer), 
    case 
        when RTRIM(oxidizer, '?') ='NA' then '-'
        else RTRIM(oxidizer, '?')
    END as Oxidizer,
    CASE 
        WHEN name = 'Raptor SL'     THEN 350.0
        WHEN name = 'Raptor 2 Vac'  THEN 380.0
        WHEN name = 'Raptor 3 SL'   THEN 350.0
        WHEN name = 'Raptor 3 Vac'  THEN 380.0
        ELSE isp
    END AS isp,
    RTRIM(COALESCE(Fuel, '-'), '?') AS Fuel,
    CASE fuel
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
            64 + abs(hash(fuel) % 160), -- R
            64 + abs(hash(fuel) % 160), -- G
            64 + abs(hash(fuel) % 160)  -- B
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
FROM read_csv_auto(
  'https://trilogy-data.github.io/trilogy-public-models/trilogy_public_models/duckdb/gcat_space/tsv/tables/engines.cleaned.tsv',
  sample_size=-1
);


CREATE OR REPLACE TABLE launch_sites as
SELECT * 
from read_csv_auto('https://trilogy-data.github.io/trilogy-public-models/trilogy_public_models/duckdb/gcat_space/tsv/tables/sites.cleaned.tsv',
sample_size=-1);

CREATE OR REPLACE TABLE organizations as
SELECT *,
CASE statecode
    WHEN 'EARTH' THEN '#1E90FF'
    WHEN 'LUNA' THEN '#C0C0C0'
    WHEN 'SSYS' THEN '#6A0DAD'
    WHEN 'AAT' THEN '#4682B4'
    WHEN 'RU' THEN '#0033A0'
    WHEN 'AF' THEN '#006400'
    WHEN 'AG' THEN '#FF0000'
    WHEN 'AGUK' THEN '#8B0000'
    WHEN 'AM' THEN '#FF9933'
    WHEN 'E' THEN '#FFD700'
    WHEN 'ANTN' THEN '#00CED1'
    WHEN 'AO' THEN '#FF4500'
    WHEN 'AQ' THEN '#F0F8FF'
    WHEN 'AR' THEN '#75AADB'
    WHEN 'ARV' THEN '#6E2E7B'
    WHEN 'AT' THEN '#ED2939'
    WHEN 'AU' THEN '#00843D'
    WHEN 'AZ' THEN '#00B9E4'
    WHEN 'B' THEN '#FFD700'
    WHEN 'BAT' THEN '#A52A2A'
    WHEN 'BB' THEN '#FFB612'
    WHEN 'BBUK' THEN '#003366'
    WHEN 'BD' THEN '#006A4E'
    WHEN 'BG' THEN '#00966E'
    WHEN 'BH' THEN '#FF0000'
    WHEN 'BM' THEN '#003366'
    WHEN 'BO' THEN '#D52B1E'
    WHEN 'BR' THEN '#009739'
    WHEN 'BS' THEN '#0077C8'
    WHEN 'BT' THEN '#FF9933'
    WHEN 'BVI' THEN '#00247D'
    WHEN 'BW' THEN '#007A33'
    WHEN 'BY' THEN '#FF0000'
    WHEN 'CA' THEN '#FF0000'
    WHEN 'ZR' THEN '#006600'
    WHEN 'CH' THEN '#FF0000'
    WHEN 'CI' THEN '#FF7900'
    WHEN 'CK' THEN '#00247D'
    WHEN 'CL' THEN '#D52B1E'
    WHEN 'CM' THEN '#007A5E'
    WHEN 'CN' THEN '#DE2910'
    WHEN 'CO' THEN '#FFD100'
    WHEN 'CR' THEN '#00247D'
    WHEN 'CZ' THEN '#11457E'
    WHEN 'CU' THEN '#002A8F'
    WHEN 'CYM' THEN '#00247D'
    WHEN 'UK' THEN '#00247D'
    WHEN 'D' THEN '#000000'
    WHEN 'DD' THEN '#FF0000'
    WHEN 'DJ' THEN '#FF0000'
    WHEN 'DK' THEN '#C60C30'
    WHEN 'DML' THEN '#008000'
    WHEN 'DR' THEN '#007FFF'
    WHEN 'DX' THEN '#808080'
    WHEN 'DZ' THEN '#006233'
    WHEN 'EC' THEN '#FFCC00'
    WHEN 'EE' THEN '#0072CE'
    WHEN 'EG' THEN '#CE1126'
    WHEN 'ET' THEN '#078930'
    WHEN 'F' THEN '#0055A4'
    WHEN 'FI' THEN '#003580'
    WHEN 'GE' THEN '#D7141A'
    WHEN 'GH' THEN '#006B3F'
    WHEN 'GI' THEN '#C8102E'
    WHEN 'GL' THEN '#FFFFFF'
    WHEN 'GR' THEN '#0D5EAF'
    WHEN 'GRD' THEN '#CE1126'
    WHEN 'GT' THEN '#0073CF'
    WHEN 'GU' THEN '#00247D'
    WHEN 'GUF' THEN '#FFD700'
    WHEN 'HR' THEN '#FF0000'
    WHEN 'HU' THEN '#CE2939'
    WHEN 'I' THEN '#008C45'
    WHEN 'I-ARAB' THEN '#008000'
    WHEN 'I-CSC' THEN '#A52A2A'
    WHEN 'I-CSC1' THEN '#8B0000'
    WHEN 'I-ELDO' THEN '#4682B4'
    WHEN 'I-ESA' THEN '#003366'
    WHEN 'I-ESRO' THEN '#6A0DAD'
    WHEN 'I-EUM' THEN '#FFD700'
    WHEN 'I-EU' THEN '#003399'
    WHEN 'I-EUT' THEN '#FF0000'
    WHEN 'I-INM' THEN '#808080'
    WHEN 'I-INT' THEN '#00CED1'
    WHEN 'I-ISS' THEN '#C0C0C0'
    WHEN 'I-NATO' THEN '#0055A4'
    WHEN 'I-RASC' THEN '#6A0DAD'
    WHEN 'ID' THEN '#DC143C'
    WHEN 'IE' THEN '#169B62'
    WHEN 'IL' THEN '#0033A0'
    WHEN 'IN' THEN '#FF9933'
    WHEN 'IQ' THEN '#CE1126'
    WHEN 'IR' THEN '#239F40'
    WHEN 'IS' THEN '#003897'
    WHEN 'J' THEN '#BC002D'
    WHEN 'JO' THEN '#007A3D'
    WHEN 'KE' THEN '#006600'
    WHEN 'KI' THEN '#FF0000'
    WHEN 'KG' THEN '#D90012'
    WHEN 'KH' THEN '#003DA5'
    WHEN 'KN' THEN '#FFD700'
    WHEN 'KORS' THEN '#C60C30'
    WHEN 'KP' THEN '#ED1C27'
    WHEN 'KR' THEN '#003478'
    WHEN 'KW' THEN '#007E3A'
    WHEN 'KZ' THEN '#00A1DE'
    WHEN 'L' THEN '#FFD700'
    WHEN 'LA' THEN '#DC143C'
    WHEN 'LB' THEN '#ED1C24'
    WHEN 'LK' THEN '#FFB612'
    WHEN 'LT' THEN '#FDB913'
    WHEN 'LV' THEN '#9E3039'
    WHEN 'LY' THEN '#007A3D'
    WHEN 'MA' THEN '#C8102E'
    WHEN 'MC' THEN '#E30613'
    WHEN 'MD' THEN '#002B7F'
    WHEN 'MH' THEN '#FF0000'
    WHEN 'MN' THEN '#0033A0'
    WHEN 'MOCN' THEN '#006600'
    WHEN 'MR' THEN '#006233'
    WHEN 'MT' THEN '#CE1126'
    WHEN 'MU' THEN '#FFCC00'
    WHEN 'MV' THEN '#D21034'
    WHEN 'MX' THEN '#006847'
    WHEN 'MY' THEN '#010066'
    WHEN 'MYM' THEN '#FF0000'
    WHEN 'N' THEN '#FFD700'
    WHEN 'NG' THEN '#008751'
    WHEN 'NI' THEN '#002B7F'
    WHEN 'NL' THEN '#21468B'
    WHEN 'NP' THEN '#DC143C'
    WHEN 'NZ' THEN '#00247D'
    WHEN 'NZRD' THEN '#FF0000'
    WHEN 'OM' THEN '#D71A28'
    WHEN 'P' THEN '#006600'
    WHEN 'PAR' THEN '#0033A0'
    WHEN 'PCZ' THEN '#800080'
    WHEN 'PE' THEN '#D91023'
    WHEN 'PG' THEN '#000000'
    WHEN 'PK' THEN '#01411C'
    WHEN 'PH' THEN '#0038A8'
    WHEN 'PL' THEN '#DC143C'
    WHEN 'PR' THEN '#002868'
    WHEN 'PW' THEN '#0099CC'
    WHEN 'PY' THEN '#D52B1E'
    WHEN 'QA' THEN '#8D1B3D'
    WHEN 'RO' THEN '#002B7F'
    WHEN 'RW' THEN '#00A1DE'
    WHEN 'S' THEN '#FFCD00'
    WHEN 'SA' THEN '#006C35'
    WHEN 'SD' THEN '#D21034'
    WHEN 'SG' THEN '#ED2939'
    WHEN 'SH' THEN '#00247D'
    WHEN 'SI' THEN '#0B4EA2'
    WHEN 'SK' THEN '#0B4EA2'
    WHEN 'SN' THEN '#00853F'
    WHEN 'SR' THEN '#377E3F'
    WHEN 'SU' THEN '#CC0000'
    WHEN 'SY' THEN '#CE1126'
    WHEN 'T' THEN '#D71A28'
    WHEN 'TC' THEN '#00247D'
    WHEN 'TF' THEN '#002395'
    WHEN 'TJ' THEN '#006600'
    WHEN 'TM' THEN '#1C9E4B'
    WHEN 'TN' THEN '#E70013'
    WHEN 'TO' THEN '#C10000'
    WHEN 'TR' THEN '#E30A17'
    WHEN 'TTPI' THEN '#0099FF'
    WHEN 'TW' THEN '#FE0000'
    WHEN 'UA' THEN '#0057B7'
    WHEN 'UAE' THEN '#00732F'
    WHEN 'UG' THEN '#FCDC04'
    WHEN 'UM67' THEN '#3C3B6E'
    WHEN 'UM79' THEN '#3C3B6E'
    WHEN 'US' THEN '#002868'
    WHEN 'UY' THEN '#0038A8'
    WHEN 'UZ' THEN '#1EB53A'
    WHEN 'VA' THEN '#FFD700'
    WHEN 'VE' THEN '#FCE300'
    WHEN 'VN' THEN '#DA251D'
    WHEN 'YE' THEN '#CE1126'
    WHEN 'ZA' THEN '#007847'
    WHEN 'ZW' THEN '#009739'
    WHEN 'X' THEN '#666666'
    WHEN 'HK' THEN '#DE2910'
    WHEN 'BGN' THEN '#005BAC'
    WHEN 'CSSR' THEN '#11457E'
    ELSE '#808080'
END as hex_code
from read_csv_auto('https://trilogy-data.github.io/trilogy-public-models/trilogy_public_models/duckdb/gcat_space/tsv/tables/orgs.cleaned.tsv',
sample_size=-1);

CREATE OR REPLACE TABLE satcat as
SELECT *
from read_csv_auto('https://trilogy-data.github.io/trilogy-public-models/trilogy_public_models/duckdb/gcat_space/tsv/cat/satcat.cleaned.tsv',
sample_size=-1);


-- OPTIMIZATION

CREATE OR REPLACE TABLE fuel_dashboard_agg as 
SELECT
    "launch_info".Launch_Tag as launch_tag,
    "launch_info"."OrbPay" as "orb_pay",
    "launch_info"."LV_Type" as lv_type,
    "launch_info"."Variant" as lv_variant,
    "org_organizations"."StateCode" as "org_state_code",
    "org_organizations"."hex_code" as "org_hex",
    "vehicle_stage_engine_engines"."Name" as "vehicle_stage_engine_name",
    "vehicle_stage_engine_engines".fuel as vehicle_stage_engine_fuel,
    vehicle_stage_engine_engines."group" as vehicle_stage_engine_group,
    vehicle_stage_engine_engines.oxidizer as vehicle_stage_engine_oxidizer,
    "vehicle_lvs_info"."Stage_No" as "stage_no",
    year(date_add(date '1900-01-01', cast((cast("launch_info"."Launch_JD" as float) - 2415021) as int) * INTERVAL 1 day)) as launch_date_year
FROM
    "launch_info"
    INNER JOIN "organizations" as "org_organizations" on "launch_info"."FirstAgency" = "org_organizations"."Code"
    FULL JOIN "lvs_info" as "vehicle_lvs_info" on "launch_info"."LV_Type" = "vehicle_lvs_info"."LV_Name" AND "launch_info"."Variant" = "vehicle_lvs_info"."LV_Variant"
    LEFT OUTER JOIN "stages" as "vehicle_stage_stages" on "vehicle_lvs_info"."Stage_Name" = "vehicle_stage_stages"."Stage_Name"
    FULL JOIN "engines" as "vehicle_stage_engine_engines" on "vehicle_stage_stages"."Engine" = "vehicle_stage_engine_engines"."Name"
;

