# Airline Data

## Data Construction Process

The dataset was constructed by processing quarterly DB1B Market **10% sample** files and merging with auxiliary data on airport characteristics, demographics, and market structure. The DB1B 10% sample is the standard public-use file provided by BTS. The main processing steps include:

1. **Market Definition**: Origin-destination city pairs based on city market IDs
2. **Time Aggregation**: Quarterly data aggregated from individual trip records  
3. **Geographic Scope**: Limited to Continental U.S. (CONUS) airports
4. **Market Size Filter**: Markets with ≥20 passengers/day on average (≥182.5 quarterly passengers in 10% sample)
5. **Price Filter**: Real fares between $25-$2,500 (2019 dollars)
6. **Competition Filter**: Excludes monopoly markets (rival_carriers > 0) Dictionary

## Dataset Overview

**File**: `airline_data.csv` / `airline_data_main.dta`  
**Source**: U.S. Department of Transportation DB1B Market Survey (**10% sample**)  
**Period**: 2005 Q1 - 2019 Q4 (60 quarters)  
**Level**: Product-market-time (carrier-route-quarter)  
**Observations**: 1,677,867 product-market-quarter combinations  
**Markets**: 8,059 unique origin-destination city pairs in Continental United States (CONUS)  

## Data Construction Process

The dataset was constructed by processing quarterly DB1B Market files and merging with auxiliary data on airport characteristics, demographics, and market structure. The main processing steps include:

1. **Market Definition**: Origin-destination city pairs based on city market IDs
2. **Time Aggregation**: Quarterly data aggregated from individual trip records  
3. **Geographic Scope**: Limited to Continental U.S. (CONUS) airports
4. **Market Size Filter**: Markets with ≥20 passengers/day on average (scaled for 10% sample)
5. **Price Filter**: Real fares between $25-$2,500 (2019 dollars)
6. **Competition Filter**: Excludes monopoly markets (rival_carriers > 0)

## Variable Definitions

### Identifiers and Time Variables

| Variable | Type | Description |
|----------|------|-------------|
| `year` | int | Year (2005-2019) |
| `quarter` | int | Quarter (1-4) |
| `time` | string | Time identifier in format YYYY-Q |
| `origin` | int | Origin city market ID (BTS designation) |
| `destination` | int | Destination city market ID (BTS designation) |
| `carrier` | string | Two-letter airline code (IATA/ICAO) |
| `market_code` | string | Origin-destination market identifier |
| `market_name` | string | Human-readable market name |
| `market_airport_city` | string | Full airport and city names for origin-destination pair |

### Core Demand Variables

| Variable | Type | Description |
|----------|------|-------------|
| `total_passengers` | int | Total quarterly passengers for this carrier on this route |
| `average_fare` | float | Average ticket price (2019 real dollars) |
| `share_nonstop` | float | Share of carrier's passengers on nonstop flights (0-1) |
| `average_distance` | float | Average flight distance in miles |
| `average_nonstop_miles` | float | Average nonstop flight distance in miles |
| `average_extra_miles` | float | Additional miles due to connections |

### Market Size and Demographics

| Variable | Type | Description | Construction |
|----------|------|-------------|--------------|
| `origin_pop` | float | Origin metropolitan area population | Merged from Census/BEA data by airport-year |
| `dest_pop` | float | Destination metropolitan area population | Merged from Census/BEA data by airport-year |

**Market Size Variables (created in analysis):**
- `pop_o_d_geo_mean` = √(origin_pop × dest_pop) - **Primary market size measure**
- `pop_sum` = origin_pop + dest_pop  
- `mean_pop` = (origin_pop + dest_pop)/2
- `max_pop` = max(origin_pop, dest_pop)

### Airport and Route Characteristics

| Variable | Type | Description |
|----------|------|-------------|
| `origin_hub` | binary | Origin airport is a major hub (from T_MASTER_CORD) |
| `destination_hub` | binary | Destination airport is a major hub |
| `origin_slot_controlled` | binary | Origin airport has slot restrictions |
| `destination_slot_controlled` | binary | Destination airport has slot restrictions |
| `destination_vacation` | binary | Destination city is a vacation/leisure market |

### Carrier Classification

| Variable | Type | Description | Definition |
|----------|------|-------------|-----------|
| `major` | binary | Major carrier indicator | Large network carriers (AA, DL, UA, etc.) |
| `legacy` | binary | Legacy carrier indicator | Traditional full-service carriers |
| `lcc` | binary | Low-cost carrier indicator | Low-cost/discount carriers (WN, B6, etc.) |
| `fringe` | binary | Fringe carrier indicator | 1 - major |

### Competition Measures

| Variable | Type | Description |
|----------|------|-------------|
| `fringe_carriers` | int | Number of fringe carriers in market |
| `rival_carriers` | int | Number of rival carriers in market (excluding focal carrier) |

### Presence and Network Variables

**Presence Measures** (carrier's market share at origin airport):

| Variable | Type | Description | Construction |
|----------|------|-------------|--------------|
| `presence` | float | Carrier's passenger share at origin airport | carrier_pax_at_origin / total_pax_at_origin |
| `total_presence` | float | Sum of all carriers' presence in market | Sum across carriers in same O-D-time |

**Network Size Measures**:

| Variable | Type | Description | Construction |
|----------|------|-------------|--------------|
| `num_destinations` | int | Number of destinations served by carrier from this origin | Count of unique destinations by origin-carrier-time |
| `num_markets` | int | Number of distinct markets served by carrier | Count of unique O-D pairs by carrier-time |
| `total_num_destinations` | int | Total destinations served by all carriers from origin | Sum across carriers |
| `total_num_markets` | int | Total markets served by all carriers | Sum across carriers |

## Instrumental Variables for Demand Estimation

The following variables serve as **instrumental variables** for price and market shares in demand estimation, based on the assumption that rival carriers' characteristics affect costs but not consumer preferences directly.

### Rival Carrier Instruments

| Variable | Type | Description | Construction | Identification |
|----------|------|-------------|--------------|----------------|
| `average_distance_rival` | float | Average distance flown by rival carriers | (∑ rival_distance - own_distance)/(N-1) | Rival cost shifter |
| `average_presence_rival` | float | Average presence of rival carriers at origin | (∑ rival_presence - own_presence)/(N-1) | Rival market power |
| `average_num_destinations_rival` | float | Average network size of rivals (destinations) | (∑ rival_destinations - own_destinations)/(N-1) | Rival scope economies |
| `average_num_markets_rival` | float | Average network size of rivals (markets) | (∑ rival_markets - own_markets)/(N-1) | Rival network effects |

**Identification Strategy**: These instruments exploit variation in rival carriers' cost structure and network characteristics. The key identifying assumption is that competitors' distance, presence, and network size affect the focal carrier's pricing through strategic interactions, but do not directly enter consumer utility.

**Construction Logic**:
- Calculate total of characteristic across all carriers in market
- Subtract focal carrier's value  
- Divide by number of rivals (N-1)
- Results in leave-one-out means of rival characteristics

### Additional Competition Instruments

| Variable | Type | Use | Description |
|----------|------|-----|-------------|
| `rival_carriers` | int | Nesting parameter IV | Number of competitors (for nested logit σ identification) |
| `rival_carriers_nest` | int | Nest-specific IV | Number of rivals within same nest (created in analysis) |

## Sample Restrictions and Filters

1. **Geographic**: Continental U.S. airports only (excludes Alaska, Hawaii, territories)
2. **Market Size**: ≥20 passengers/day average (scaled for 10% sample: ≥182.5 quarterly passengers)
3. **Price Range**: Real fares between $25-$2,500 (2019 dollars)
4. **Carrier Stability**: Excludes observations where carrier changed during booking (tkcarrierchange=0)
5. **Competition**: Excludes monopoly markets (rival_carriers > 0)
6. **Data Quality**: Non-missing values for core variables (price, distance, passengers)

## Market Share Construction (in analysis files)

Market shares are constructed in the analysis files using alternative market size definitions:

```stata
* Primary specification (geometric mean population)
gen msize = pop_o_d_geo_mean/10  // Market size (scaled)
gen mshare = total_passengers/msize  // Market share

* Outside share construction
bys market_code time: egen s_inside_t = total(mshare)
gen s_0t = 1 - s_inside_t  // Outside option share
```

## Data Quality and Coverage

- **Completeness**: 99%+ coverage for core variables
- **Population Data**: ~1.2% missing (small airports/MSAs)
- **Temporal Coverage**: Complete quarterly coverage 2005-2019
- **Market Coverage**: ~7,000 unique origin-destination pairs
- **Carrier Coverage**: 134 unique airline codes (see crosswalk file)

## Usage Notes for Researchers

1. **Market Size**: Use `pop_o_d_geo_mean` as primary market size (geometric mean of endpoint populations)
2. **Instruments**: Rival-based instruments are valid under standard cost-shifter assumptions
3. **Nesting**: Carrier type variables (legacy, lcc) enable nested logit estimation
4. **Time Effects**: Include quarter-year fixed effects to control for fuel costs, economic conditions
5. **Clustering**: Standard errors should cluster by market (origin-destination) or time
6. **Sample Selection**: Be aware of filters applied - results apply to markets with substantial traffic

## File Dependencies

- **Source**: DB1B Market quarterly files (2005Q1-2019Q4)  
- **Auxiliary**: Airport characteristics, demographics, vacation destinations, slot restrictions
- **Crosswalk**: `crosswalk_airlines_code.csv` maps carrier codes to airline names
- **Processing**: `processing_data.do` contains full data construction code

## Citation

When using this dataset, please cite:
- U.S. Department of Transportation, Bureau of Transportation Statistics, Airline Origin and Destination Survey (DB1B)
- Processing and construction methodology from this project

---

*Last updated: September 2025*  
*Contact: Giselle Labrador-Badia for questions about variable construction or methodology*
