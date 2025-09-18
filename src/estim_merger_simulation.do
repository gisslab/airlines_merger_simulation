// Final project
// Date : 9-15-2025
// Reference for nested logit :
//  - Train, Kenneth E. (2009). Discrete Choice Methods with Simulation, 2nd ed.
//    Cambridge University Press — Chapter 4 “Nested Logit”.
//  - Helpful note for share derivatives/elasticities:
//     Mansley, R., N. Miller, C. Ryan, and M. Weinberg (2019/2024),
//     “Notes on the Nested Logit Demand Model.” (short technical memo)

// Install as: 
// net from http://www.bjornerstedt.org/stata/mergersim
// net install mergersim, replace

*global mainpath "/Users/kkang57/Dropbox/Teaching/MS-IO/final_project"
clear

// cd "/Users/gisellelab/Work/airlines_merger_simulation"   // <-- adjust this, then run the do-file , My path is 

// global PROC_DATA    "./data/processed/combined/"                // processed data
// global CODE_DIR      "./src/"                         
// global OUT      "./src/output/"              // output folder          // source code directory

// cap log using "${CODE_DIR}/logs/prelim_analysis_merger.log", replace

// use "$mainpath/data/airline_data_main.dta"
// use "$PROC_DATA/airline_data_main.dta", clear

// global mainpath "/Users/karamkang/Library/CloudStorage/Dropbox/Teaching/MS-IO/final_project"
// log using "$mainpath/analysis/prelim_analysis.log", replace

use "$mainpath/data/airline_data_main.dta", clear
*---------------------------
* 1. preparing the data
*---------------------------

// * Checking that legacy and lcc carriers have no overlap
preserve
bys carrier: keep if _n==1
list carrier legacy lcc if !missing(legacy) | !missing(lcc)
restore

* focus on a 5% sub-sample of the data (for computational purposes)
//! this way of randomizing is creating bias towards smaller markets (less observations per market)
// set seed 12345
// gen random_uniform = runiform()
// bys market_code: egen rr = mean(random_uniform)
// quietly sum rr, detail
// keep if rr<r(p5)

// * new addition: sample 15% of markets, sample one market at a time
preserve
bys market_code: keep if _n == 1  // Keep one obs per market
sample 15                          // Sample 15% of markets
keep market_code                  // Keep only market identifier
tempfile sampled_markets
save `sampled_markets'
restore
merge m:1 market_code using `sampled_markets', keep(match) nogenerate

* define markets: time-destination-origin
encode carrier, gen(firmid)
egen marketid = group(year quarter market_code)
egen marketid2 = group(market_code)

* market size: alternatives
gen pop_o_d_geo_mean = sqrt(origin_pop * dest_pop)	// geometric mean 
gen pop_sum = origin_pop + dest_pop			// sum
gen mean_pop = (origin_pop + dest_pop)/2 	// average
gen max_pop = max(origin_pop, dest_pop)		// maximum

* declare market size
gen msize = pop_o_d_geo_mean/10
gen mshare = total_passengers/msize
sum mshare

* nesting groups: alternatives
gen inside = 1
gen nonstop_route = (share_nonstop > 0)  // by non stop vs. connecting

* declare nesting groups
gen nest1 = inside // ! comment
gen nest2 = 3 // ! comment
replace nest2 = 1 if legacy == 1 // ! comment
replace nest2 = 2 if lcc == 1

* market/product attributes
gen dist_k   = average_distance/1000	// distances in thousands
gen dist_k2  = dist_k^2					// distances in thousands, squared
gen lnum_fringe = ln(1 + fringe_carriers)

* declare market/product attributes to control for
local x_exog "share_nonstop dist_k dist_k2 lnum_fringe"

* instruments for market share
bys marketid nest1 nest2: egen rival_carriers_nest = sum(inside)
replace rival_carriers_nest = rival_carriers_nest-1 // number of rivals within the nest

// * new addition: create additional IV for upper level nesting (rival carriers in market)
* declare instruments for price and market share
local inst "average_distance_rival average_num_destinations_rival rival_carriers_nest rival_carriers"

// * new addition: filter small market shares to avoid numerical issues (BEFORE mergersim init)
scalar min_share_filter = 10^-7
keep if mshare > min_share_filter

// * new addition: eliminate singleton markets after share filtering
bys marketid: keep if _N > 1

* set the panel data: product x markets
xtset firmid marketid

*-------------------------------------
* 2. performing a merger simulation
*-------------------------------------

* step 1: initializing the merger simulation settings
mergersim init, nests(nest1 nest2) price(average_fare) quantity(total_passengers) marketsize(msize) firm(firmid)

sum M_ls M_lsjh M_lshg // exploring that log nests are the same than in the demand estimation.do -----> YES

* step 2: nested logit demand model estimation
ivreghdfe M_ls (average_fare M_lsjh M_lshg = `inst') `x_exog', absorb(marketid2) cluster(marketid2)

* step 3: analyzing premerger market conditions
mergersim market if year == 2013

* step 4: simulating the merger effects
*         consider a merger between AA (buyer) and US (seller)
*         note: AA and US merger approved in Dec 2013
mergersim simulate if year == 2014, seller(61) buyer(6) detail

log close
