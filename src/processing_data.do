// -----------------------------------------------------------------------------
// * File:    processing_data.do
// * Project: Airlines Merger Simulation
// * Purpose: Preprocess and clean DB1B Market data (Stata version of your R flow)
// * Author:  Giselle Labrador-Badia
// * Date:    August 2025

// * input: DB1B Market data (csvs in proccessed/data_files/)
//         * auxiliary tables (city market lookup, vacations, population, slot-controlled airports, T_MASTER_CORD)

// * output: airline_data.csv (processed data for analysis)
// -----------------------------------------------------------------------------

// ---------------------- PATHS (EDIT THESE ONLY) -------------------------------
pwd
// if in my laptop:
cd "/Users/gisellelab/Work/airlines_merger_simulation"   // <-- adjust this, then run your do-file , My path is 


global RAW_DB1B      "./data/raw/db1b_market"                 // DB1B quarter CSVs
global RAW_OTHER     "./data/raw/other/"                     // auxiliary raw data
global PROC_DB1B      "./data/processed/db1b_market/"              // processed DB1B Market data
global PROC_OTHER    "./data/processed/other/"                // processed auxiliaries
global OUT_DIR       "./data/processed/combined/"              // output folder
global OUT_FILE      "${OUT_DIR}airline_data.csv"              // final CSV

// If the output dir doesn’t exist, Stata will create on first export

// ---------------------- PARAMETERS -------------------------------------------
global YEARS   "2005/2019"
global QUARTERS "1 2 3 4"
global TYPE     "Market"   // DB1B “Market”

// ---------------------- LOAD AUXILIARY TABLES --------------------------------

di as yellow "------------------------------------ Loading auxiliary tables ------------------------------------"

// City market lookup
import delimited using "${PROC_OTHER}city_market_lookup.csv", varnames(1) clear stringcols(_all)
tempfile cities
save `cities'

// Vacations (city-level flags) (only in the U.S.)
import delimited using "${PROC_OTHER}vacations.csv", varnames(1) clear stringcols(_all)
rename origin_cities description
destring vacation_spot, replace force
tempfile vacations
save `vacations'

// City lookup + vacations
use `cities', clear
merge 1:1 description using `vacations', nogenerate
keep code vacation_spot
// # make int vacation_spot if float
replace vacation_spot = 0 if vacation_spot==.
tempfile cities_vac
save `cities_vac'

// Population by airport-year
import delimited using "${PROC_OTHER}population.csv", varnames(1) clear
// expected cols: airport, msa, year, population
keep airport year population
tempfile pops
save `pops'
// Slot-controlled airports
import delimited using "${PROC_OTHER}slot_controlled.csv", varnames(1) clear
// expected cols: airport, slot_controlled
// ensure lowercase column names
foreach v of varlist * {
    local newname = lower("`v'")
    if "`v'" != "`newname'" {
        rename `v' `newname'
    }
}
tempfile slots
save `slots'

// T_MASTER_CORD (airport metadata; filter to CONUS)
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
tempfile conus
save `conus'

// Lookup & Hub (wide) -> long(Code, carrier, hub)
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
    destring hub_`v', replace force
}
rename hub_code airport_code

// reshape to long on stub hub_
qui reshape long hub_, i(airport_code) j(carrier) string
rename hub_ hub
replace hub = 0 if missing(hub)
tempfile hubs_long
save `hubs_long'

// CPI (already base=2008 index) — if yours is year,index
// If your file has columns like year,index (2008=100), we’ll read and use as deflator map.
import delimited using "${RAW_OTHER}cpi_index.csv", varnames(1) clear
// Expect columns: year,value  (value = 100 in 2008)
keep year value
rename value deflator_index
tempfile cpi
save `cpi'

// ---------------------- INIT OUTPUT ------------------------------------------
capture confirm file "${OUT_FILE}"
local appendflag = _rc==0  // if the file exists, we’ll append

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
            
            // If the 42nd var is junk "V" in your R flow, mirror that:
            capture ds, has(varnum 42)
            if !_rc {
                local v42 `r(varlist)'
                capture confirm variable `v42'
                if !_rc rename `v42' V
            }

            // ---------------- Recreate key derived fields -------------------
            // Real fare using CPI index (2008=100)
            // If your CPI index is 100 in 2008, deflate by DEF/100:
            gen double mktfare_real = mktfare / (`DEF'/100)

            // Keep "good" tickets, fare band, and CONUS airports
            keep if tkcarrierchange==0
            keep if mktfare_real>=25 & mktfare_real<=2500

            // Limit to CONUS origin/dest
            tempfile current
            save `current'
            use `conus', clear
            tempfile conus_keep
            save `conus_keep'

            use `current', clear
            merge m:1 originairportid using `conus_keep', nogenerate
            keep if _merge==3 | _merge==.    // in case of merge result; with nogenerate we can just assert matched
            tempfile after_origin
            save `after_origin'

            use `after_origin', clear
            merge m:1 destairportid using `conus_keep', nogenerate
            keep if _merge==3 | _merge==.
            
            // Market passengers per O-D (quarter)
            bysort origincitymarketid destcitymarketid: egen double market_passengers = total(passengers)
            // Keep markets >= 20 pax/day on average, scaled for 10% sample: 20*365/(4*10)
            keep if market_passengers >= 20*365/(4*10)

            // Core variables for merging/aggregation
            gen long   origin      = origincitymarketid
            gen long   destination = destcitymarketid
            gen str2   carrier     = tkcarrier
            gen int    year_q      = year
            gen byte   quarter_q   = quarter
            gen byte   nonstop     = (mktcoupons==1)

            // ---------------- Merge slot_controlled ------------------------
            di as yellow "      Merging slot-controlled airports"
            merge m:1 originairportid using `slots', keep(match master) nogenerate
            rename slot_controlled origin_slot_controlled
            replace origin_slot_controlled = 0 if missing(origin_slot_controlled)

            merge m:1 destairportid using `slots', keep(match master) nogenerate
            rename slot_controlled destination_slot_controlled
            replace destination_slot_controlled = 0 if missing(destination_slot_controlled)

            // // ---------------- Merge destination vacation flag ---------------
            // // Cities table keyed by Code (city market id) and vacation_spot
            // // Our cities_vac has: Code, vacation_spot
            // merge m:1 destcitymarketid using `cities_vac', ///
            //     keep(match master) nogenerate
            // rename vacation_spot destination_vacation
            // replace destination_vacation = 0 if missing(destination_vacation)

            // // ---------------- Merge populations by airport-year -------------
            // merge m:1 originairportid year_q using `pops', ///
            //     keep(match master) nogenerate
            // rename population origin_pop
            // replace origin_pop = . if origin_pop<=0

            // merge m:1 destairportid year_q using `pops', ///
            //     keep(match master) nogenerate
            // rename population dest_pop
            // replace dest_pop = . if dest_pop<=0

            // gen double market_size = sqrt(origin_pop * dest_pop)
            // drop origin_pop dest_pop

            // // ---------------- Merge hub indicators (origin/dest) ------------
            // // hubs_long: Code, carrier, hub
            // // For airports we need Code==AirportID (lookup_and_hub_r used airport codes in "Code")
            // // For origin:
            // tempfile hubs_o
            // use `hubs_long', clear
            // rename code originairportid
            // tempfile hubs_o_long
            // save `hubs_o_long'

            // use `current', clear
            // merge m:1 originairportid carrier using `hubs_o_long', keep(match master) nogenerate
            // rename hub origin_hub
            // replace origin_hub = 0 if missing(origin_hub)

            // // For destination:
            // tempfile hubs_d
            // use `hubs_long', clear
            // rename code destairportid
            // tempfile hubs_d_long
            // save `hubs_d_long'

            // use `current', clear
            // merge m:1 destairportid carrier using `hubs_d_long', keep(match master) nogenerate
            // rename hub destination_hub
            // replace destination_hub = 0 if missing(destination_hub)

            // // ---------------- Aggregate to (O-D-carrier-quarter) ------------
            // // Weighted mean fare by Passengers; sums and means for others
            // collapse ///
            //     (mean)  average_distance = mktdistance ///
            //             average_nonstop_miles = nonstopmiles ///
            //             average_extra_miles   = (mktdistance - nonstopmiles) ///
            //             share_nonstop         = nonstop ///
            //             market_size           = market_size ///
            //             origin_hub            = origin_hub ///
            //             destination_hub       = destination_hub ///
            //             destination_vacation  = destination_vacation ///
            //             origin_slot_controlled      = origin_slot_controlled ///
            //             destination_slot_controlled = destination_slot_controlled ///
            //     (sum)   total_passengers   = passengers ///
            //     (mean)  year_q quarter_q ///
            //     (mean)  _tmp = 0, ///
            //     by(origin destination carrier)
            // drop _tmp

            // // Weighted average fare: do separately to guarantee exact passenger weights
            // // (If you prefer a single step, you can use collapse (mean) with frequency weights.)
            // // We'll recalc with temp merge:
            // tempfile tmp_odc
            // save `tmp_odc'
            // // reload raw for the weight calc
            // use "${PROC_DB1B}`fname'.csv", clear
            // gen double mktfare_real = mktfare / (`DEF'/100)
            // keep if tkcarrierchange==0 & mktfare_real>=25 & mktfare_real<=2500
            // keep origincitymarketid destcitymarketid tkcarrier passengers mktfare_real
            // rename (origincitymarketid destcitymarketid tkcarrier) (origin destination carrier)
            // collapse (sum) wfare_numer = (mktfare_real*passengers) ///
            //          (sum) wfare_denom = passengers, ///
            //          by(origin destination carrier)
            // gen double average_fare = wfare_numer / wfare_denom
            // keep origin destination carrier average_fare

            // merge 1:1 origin destination carrier using `tmp_odc', nogenerate

            // // ---------------- Identifiers and carrier types -----------------
            // gen str market = string(origin) + string(destination)
            // gen str time   = string(year_q) + string(quarter_q)

            // // LCC flag (use your R code’s list)
            // gen byte lcc = inlist(carrier,"TZ","F9","YV","DH","B6","YX","SX","XP","WN","NK","SY","U5","VC")

            // // “Major” flags change by year; you can replicate those long case_when lists
            // // For brevity you can create a separate CSV map if you prefer. Placeholder:
            // gen byte major = .
            // replace major = 1 if year_q==2019 & inlist(carrier,"AS","G4","AA","5Y","DL","MQ","F9","HA","B6","RC","OO","WN","NK","UA")
            // // (Continue for other years as in your R code…)

            // // Legacy
            // gen byte legacy = inlist(carrier,"AS","AQ","AA","CO","DL","HA","NW","TW","UA","US")

            // // Rival stats
            // // Presence = carrier’s share of passengers at origin in that year/quarter
            // // First: total presence by origin-year-quarter
            // by origin year_q quarter_q: egen double total_presence = total(total_passengers)
            // by origin carrier year_q quarter_q: egen double presence = total(total_passengers) / total_presence
            // by origin carrier year_q quarter_q: gen   num_markets = _N

            // // Rival summaries by O-D-year-quarter
            // by origin destination year_q quarter_q: egen double average_distance_rival = ///
            //     (total(average_distance) - average_distance) / ( _N - 1 )
            // by origin destination year_q quarter_q: egen double average_presence_rival = ///
            //     (total(presence) - presence) / ( _N - 1 )
            // by origin destination year_q quarter_q: egen double average_num_markets_rival = ///
            //     (total(num_markets) - num_markets) / ( _N - 1 )
            // by origin destination year_q quarter_q: gen   rival_carriers = _N - 1
            // keep if rival_carriers > 0

            // // Shares
            // by origin destination year_q quarter_q: gen double share = 10 * total_passengers / market_size
            // by origin destination year_q quarter_q: egen double sum_share = total(share)
            // gen double outside_share = 1 - sum_share
            // drop sum_share
            // gen double within_share  = share / ///
            //     (by(origin destination year_q quarter_q): total(share))

            // gen double log_diff_shares = ln(share) - ln(outside_share)

            // // ---------------- Append to CSV --------------------------------
            // tempfile outq
            // save `outq', replace

            // // Decide header write
            // local writeheader = cond(`appendflag', "varnames(0)", "varnames(1)")
            // export delimited using "${OUT_FILE}", `writeheader' append
            // local appendflag = 1
        }
    }

di as result "Done. Wrote: ${OUT_FILE}"
