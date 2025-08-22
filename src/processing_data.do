// -----------------------------------------------------------------------------
// * File:    processing_data.do
// * Project: Airlines Merger Simulation
// * Purpose: Preprocess and clean DB1B Market data (Stata version of the R flow)
// * Author:  Giselle Labrador-Badia
// * Date:    August 2025

// * input:  
//         * DB1B Market data (quarterly CSVs)  
//         * auxiliary tables (city market lookup, vacations, population, slot-controlled airports, T_MASTER_CORD)

// * output: airline_data.csv (processed data for analysis)
// -----------------------------------------------------------------------------

// ---------------------- PATHS (EDIT THESE ONLY) -------------------------------
pwd

// if in my laptop:
cd "/Users/gisellelab/Work/airlines_merger_simulation"   // <-- adjust this, then run the do-file , My path is 


global RAW_DB1B      "./data/raw/db1b_market"                 // DB1B quarter CSVs
global RAW_OTHER     "./data/raw/other/"                     // auxiliary raw data
global PROC_DB1B      "./data/processed/db1b_market/"              // processed DB1B Market data
global PROC_OTHER    "./data/processed/other/"                // processed auxiliaries
global OUT_DIR       "./data/processed/combined/"              // output folder
global OUT_FILE      "${OUT_DIR}airline_data.csv"              // final CSV
global CODE_DIR      "./src/"                                   // source code directory

// If the output dir doesn’t exist, Stata will create on first export

log close
log using "${CODE_DIR}logs/processing_data.log", replace


// ---------------------- PARAMETERS -------------------------------------------
global YEARS   "2005/2019"
global QUARTERS "1 2 3 4"
global TYPE     "Market"   // DB1B “Market”


di as yellow "--------------------------------------------------------------------------------------------------"
di as yellow "----------------------------------- Running preprocessing_data.do --------------------------------"
di as yellow "--------------------------------------------------------------------------------------------------"

// ---------------------- LOAD AUXILIARY TABLES --------------------------------
di as yellow "------------------------------------ Loading auxiliary tables ------------------------------------"

// * City market lookup
import delimited using "${PROC_OTHER}city_market_lookup.csv", varnames(1) clear stringcols(_all)
tempfile cities
save `cities'

// * Vacations (city-level flags) (only in the U.S.)
import delimited using "${PROC_OTHER}vacations.csv", varnames(1) clear stringcols(_all)
rename origin_cities description
destring vacation_spot, replace force
tempfile vacations
save `vacations'

// * City lookup + vacations
use `cities', clear
merge 1:1 description using `vacations', nogenerate
keep code vacation_spot

// # make int vacation_spot if float
replace vacation_spot = 0 if vacation_spot==.
// code to long for merger
destring code, replace
// Remove duplicates to ensure unique merge key
duplicates drop code, force
gen long destcitymarketid = code  // for origin merge
drop code
tempfile cities_vac
save `cities_vac'

// * Population by airport-year
import delimited using "${PROC_OTHER}population.csv", varnames(1) clear
// expected cols: airport, msa, year, population
keep airport year population
rename airport airport_id
tempfile pops
save `pops'

// * Slot-controlled airports
import delimited using "${PROC_OTHER}slot_controlled.csv", varnames(1) clear
// expected cols: airport, slot_controlled
// ensure lowercase column names
foreach v of varlist * {
    local newname = lower("`v'")
    if "`v'" != "`newname'" {
        rename `v' `newname'
    }
}
rename airport airport_id
tempfile slots
save `slots'

// * T_MASTER_CORD (airport metadata; filter to CONUS)

import delimited using "${PROC_OTHER}t_master_cord.csv", varnames(1) clear
// ensure lowercase column names
foreach v of varlist * {
    local newname = lower("`v'")
    if "`v'" != "`newname'" {
        rename `v' `newname'
    }
}
keep if airport_country_code_iso == "US" & !inlist(airport_state_code,"PR","VI","TT","HI","AK")

keep airport_id
duplicates drop
rename airport_id originairportid  // for origin merge
tempfile conus_origin
save `conus_origin'

// Create separate copy for destination merge
rename originairportid destairportid
tempfile conus_dest
save `conus_dest'

// * Lookup & Hub (wide) -> long(Code, carrier, hub)
import delimited using "${PROC_OTHER}lookup_and_hub_r.csv", varnames(1) clear stringcols(_all)

// ensure lowercase column names
foreach v of varlist * {
    local newname = lower("`v'")
    if "`v'" != "`newname'" {
        rename `v' `newname'
    }
}
// expected cols: code, description, airport, then 100+ carrier columns (AA, DL, WN, NK, UA, …)
drop description airport
// Prefix all non-code columns with "hub_"
unab allvars : *
local rest : list allvars - code
foreach v of local rest {
    capture confirm variable `v'
    if !_rc rename `v' hub_`v'
    qui destring hub_`v', replace force
}
rename hub_code airport_code

// reshape to long on stub hub_
qui reshape long hub_, i(airport_code) j(carrier) string
rename airport_code airport_id
rename hub_ hub
replace hub = 0 if missing(hub)
tempfile hubs_long
save `hubs_long'

// CPI (already base=2008 index) — if yours is year,index
// If the file has columns like year,index (2008=100), we’ll read and use as deflator map.
import delimited using "${RAW_OTHER}cpi_index.csv", varnames(1) clear
// Expect columns: year,value  (value = 100 in 2008)
keep year value
rename value deflator_index
tempfile cpi
save `cpi'

// ---------------------- INIT OUTPUT ------------------------------------------
capture confirm file "${OUT_FILE}"
local appendflag =  0 // no! _rc==0  // if the file exists, we’ll append

// ---------------------- LOOP OVER YEARS/QUARTERS -----------------------------
di as yellow "------------------------------------ Processing DB1B Market data ------------------------------------"
    forvalues Y = $YEARS {
        // grab the year’s CPI deflator (index with 2008=100)
        use `cpi', clear
        keep if year==`Y'
        qui summarize deflator_index
        local DEF = r(mean)  // year-level deflator index

        foreach Q in $QUARTERS {
            di as yellow "--------------------------------- Processing `Y' Q`Q' --------------------------------------"

            // ---------------- Load one quarter market CSV -------------------
            local fname = "db1b_market_`Y'q`Q'"
            import delimited using "${PROC_DB1B}`fname'.csv", varnames(1) clear
            
            // If the 42nd var is junk "V" in the R flow, mirror that:
            capture ds, has(varnum 42)
            if !_rc {
                local v42 `r(varlist)'
                capture confirm variable `v42'
                if !_rc rename `v42' V
            }

            // ---------------- Recreate key derived fields -------------------
            // Real fare using CPI index (2008=100)
            // If the CPI index is 100 in 2008, deflate by DEF/100:
            gen double mktfare_real = mktfare / (`DEF'/100)

            // Keep "good" tickets, fare band, and CONUS airports
            keep if tkcarrierchange==0
            keep if mktfare_real>=25 & mktfare_real<=2500

            // Limit to CONUS origin/dest
            tempfile current
            save `current'

            use `current', clear
            di as yellow "    ---------  Filtering CONUS airports ---------"

            merge m:1 originairportid using `conus_origin', keep(match master) nogenerate

            merge m:1 destairportid using `conus_dest', keep(match master) nogenerate
            
            // Market passengers per O-D (quarter)
            bysort origincitymarketid destcitymarketid: egen double market_passengers = total(passengers)
            // Keep markets >= 20 pax/day on average, scaled for 10% sample: 20*365/(4*10)
            keep if market_passengers >= 20*365/(4*10)

            // Core variables for merging/aggregation
            gen long   origin      = origincitymarketid
            gen long   destination = destcitymarketid
            gen str2   carrier     = tkcarrier
            gen byte   nonstop     = (mktcoupons==1)

            // ---------------- Merge slot_controlled ------------------------
            di as yellow "    ---------  Filtering slot-controlled airports ---------"

            gen airport_id = originairportid
            merge m:1 airport_id using `slots', keep(match master) nogenerate
            rename slot_controlled origin_slot_controlled
            replace origin_slot_controlled = 0 if missing(origin_slot_controlled)

            replace airport_id = destairportid
            merge m:1 airport_id using `slots', keep(match master) nogenerate
            rename slot_controlled destination_slot_controlled
            replace destination_slot_controlled = 0 if missing(destination_slot_controlled)

            drop airport_id

            // ---------------- Merge destination vacation flag ---------------
            di as yellow "    ---------  Merging destination vacation flags ---------"
            // Cities table keyed by Code (city market id) and vacation_spot
            // cities_vac has: Code, vacation_spot -> now destcitymarketid
            merge m:1 destcitymarketid using `cities_vac', ///
                keep(match master) nogenerate
            rename vacation_spot destination_vacation
            replace destination_vacation = 0 if missing(destination_vacation)

            // ---------------- Merge populations by airport-year -------------
            di as yellow "    ---------  Merging airport populations ---------"
            gen airport_id = originairportid
            merge m:1 airport_id year using `pops', ///
                keep(match master) nogenerate
            rename population origin_pop
            replace origin_pop = . if origin_pop<=0

            replace airport_id = destairportid
            merge m:1 airport_id year using `pops', ///
                keep(match master) nogenerate
            rename population dest_pop
            replace dest_pop = . if dest_pop<=0

            gen double market_size = sqrt(origin_pop * dest_pop)
            drop airport_id
            // drop origin_pop dest_pop // not sure if later needed

            // ---------------- Merge hub indicators (origin/dest) ------------
            di as yellow "    ---------  Merging hub indicators ---------"
            // hubs_long: Code, carrier, hub
            // For airports we need Code==AirportID (lookup_and_hub_r used airport codes in "Code")
            // For origin:

            gen airport_id = originairportid
            merge m:1 airport_id carrier using `hubs_long', keep(match master) nogenerate
            rename hub origin_hub
            replace origin_hub = 0 if missing(origin_hub)

            replace airport_id = destairportid
            merge m:1 airport_id carrier using `hubs_long', keep(match master) nogenerate
            rename hub destination_hub
            replace destination_hub = 0 if missing(destination_hub)
            drop airport_id

            // ---------------- Aggregate to (O-D-carrier-quarter) ------------
            di as yellow "    ---------  Aggregating to (O-D-carrier-quarter) ---------"
            
            // First calculate passenger-weighted averages for distance and fare variables
            gen double weighted_distance = mktdistance * passengers
            gen double weighted_nonstop_miles = nonstopmiles * passengers
            gen double weighted_fare = mktfare_real * passengers
            
            // Collapse with appropriate aggregation methods
            collapse ///
                (sum)   total_passengers = passengers ///
                        weighted_distance weighted_nonstop_miles weighted_fare ///
                (mean)  market_size ///
                        origin_hub destination_hub ///
                        destination_vacation ///
                        origin_slot_controlled destination_slot_controlled ///
                        share_nonstop = nonstop, ///
                by(origin destination carrier year quarter)

            
            // Calculate final weighted averages
            gen double average_distance = weighted_distance / total_passengers
            gen double average_nonstop_miles = weighted_nonstop_miles / total_passengers
            gen double average_fare = weighted_fare / total_passengers
            gen double average_extra_miles = average_distance - average_nonstop_miles
            
            // Clean up temporary variables
            drop weighted_distance weighted_nonstop_miles weighted_fare


            di as yellow "               Data collapsed to (O-D-carrier-quarter) level successfully, obs: " _N 

            di as yellow "    ---------  Creating identifiers ---------"
            // ---------------- Identifiers and carrier types -----------------
            // Use more efficient string creation to avoid "expression too long" error
            tostring origin, gen(origin_str)
            tostring destination, gen(dest_str)
            gen str25 market = origin_str + "-" + dest_str

            tostring year, gen(year_str)
            tostring quarter, gen(quarter_str)  
            gen str25 time = year_str + "-" + quarter_str
            drop year_str quarter_str origin_str dest_str

            di as yellow "    ---------  Creating carrier flags ---------"

            do "src/carrier_flags.do"

            di as yellow "    ---------  Creating rival stats ---------"
            // * Create presence and num_markets variables first (needed for rival calculations)
            
            // Calculate number of fringe firms (non-major carriers) per market
            // Assuming major_carrier_flag is created in carrier_flags.do
            gen fringe = 1 - major
            bysort origin destination year quarter: egen fringe_carriers = total(fringe)
            
            // Presence = carrier's share of passengers at ORIGIN in that year/quarter (across all destinations)
            // First calculate total passengers by origin-carrier-year-quarter
            bysort origin carrier year quarter: egen origin_carrier_pax = total(total_passengers)
            // Then calculate total passengers by origin-year-quarter (all carriers)
            bysort origin year quarter: egen origin_total_pax = total(total_passengers)
            // Presence is the share
            gen presence = origin_carrier_pax / origin_total_pax
            replace presence = 0 if missing(presence)
            
            // num_markets = number of distinct destinations this carrier serves from this origin in year/quarter
            bysort origin carrier year quarter: egen num_destinations = count(destination) // num_destinations
            bysort carrier year quarter: egen num_markets = count(market) // num_markets defined as number of distinct markets served by this carrier in this origin in year/quarter

            drop origin_carrier_pax origin_total_pax
            
            // calculate rival stats using presence and num_markets
            // These are calculated at the O-D level (market level)
            sort origin destination year quarter
            by origin destination year quarter: egen double total_presence = total(presence)
            by origin destination year quarter: egen double total_num_markets = total(num_markets)
            by origin destination year quarter: egen double total_num_destinations = total(num_destinations)
            by origin destination year quarter: egen double total_distance = total(average_distance)

            // Calculate rival average X (for future IV use)
            
            by origin destination year quarter: gen double average_presence_rival = ///
                (total_presence - presence) / ( _N - 1 )
            by origin destination year quarter: gen double average_num_markets_rival = ///
                (total_num_markets - num_markets) / ( _N - 1 )
            by origin destination year quarter: gen double average_num_destinations_rival = ///
                (total_num_destinations - num_destinations) / ( _N - 1 )
            by origin destination year quarter: gen double average_distance_rival = ///
            (total_distance - average_distance) / ( _N - 1 )

            by origin destination year quarter: gen int rival_carriers = _N - 1
            
            drop total_distance 
            
            keep if rival_carriers > 0 // removing monopolies

            // Note: Mkt share creation now moved to demand_estimation.do

            // ---------------- Append to CSV --------------------------------
            di as yellow "    ---------  Writing data to output file ---------"
            
            // Save current quarter data
            tempfile quarter_data
            save `quarter_data', replace

            // make sure types are correct
            // mkt and time to str
            
            // Determine if we need column headers (first time writing)
            if `appendflag' == 0 {
                di as yellow "           Writing headers for first quarter..."
                export delimited using "${OUT_FILE}", replace
                local appendflag = 1
            }
            else {
                di as yellow "           Appending `Y'Q`Q' data to existing file..."
                // Load existing data and append new quarter
                preserve
                import delimited using "${OUT_FILE}", clear
                append using `quarter_data'
                export delimited using "${OUT_FILE}", replace
                restore
            }
            
            di as green "           Successfully wrote `Y'Q`Q' data to ${OUT_FILE}"
        }
    }

di as result "Done. Wrote: ${OUT_FILE}"

log close
