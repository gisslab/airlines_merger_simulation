// **********************************************************************
// * Project: Airline Demand Estimation & Merger Simulation
// * File   : 02_demand_estimation.do
// * Author : Giselle Labrador-Badia
// * Date   : 8-18-2025
// *
// * PURPOSE
// *   Replicate*-------------------------*
// *   results using the product-market panel created earlier
// *   (airline_data.csv).
// *
// * MODEL (from the paper)
// *   Consumer i’s indirect utility from product j in market t:
// *       u_ijt = α p_jt + x_jt β + ξ_t + ξ_jt + ζ_it + (1-ρ) ε_ijt
// *   where:
// *     - p_jt               : Average Fare (price)
// *     - x_jt includes      : Share Nonstop; Average Distance (1,000’s of miles);
// *                            Average Distance^2 (1,000’s); log(1 + Number of Fringe firms)
// *     - log(S_t)           : included as in Table C2 (market-size/scale control)
// *     - ξ_t                : Origin–Destination (OD) fixed effects
// *     - ε_ijt ~ Type I EV; ζ_it distributed conjugate (for nesting)
// *   Market shares s_jt follow BLP/Logit form; the paper also reports
// *   a nested-logit estimation with a single nesting parameter ρ.
// *
// * VARIABLES USED (as in Table C2 and text)
// *   Outcome (simple logit):  log(s_jt) - log(s_0t)
// *   Regressors x_jt:        Average Fare; log(S_t); Share Nonstop;
// *                           Avg Distance (k miles); Avg Distance^2 (k);
// *                           log(1 + Num Fringe)
// *   Fixed effects:          Origin–Destination (OD) FE
// *
// * INSTRUMENTS FOR DEMAND (from paper)
// *   z^D_jt includes: average rival distance; average # of markets a rival serves;
// *   number of rival carriers (the last is helpful to identify the nesting parameter).
// *
// * NESTING STRUCTURE (single-level nested logit)
// *   One level of groups to capture correlation in ε across products:
// *   Default in this do-file: nests are defined by *carrier* within OD–quarter.
// *
// * OUTPUTS
// *   - LaTeX summary stats for all estimation variables
// *   - OLS logit (informal)
// *   - IV logit (price endogenous)
// *   - IV nested-logit (price and ln(s_gjt) endogenous)
// ***********************************************************************

*-------------------------*
* USER CHOICES & PATHS   *
*-------------------------*
clear all

* Paths

// if in my laptop:
cd "/Users/gisellelab/Work/airlines_merger_simulation"   // <-- adjust this, then run the do-file , My path is 


global PROC_DATA    "./data/processed/combined/"                // processed data
global CODE_DIR      "./src/"                         
global OUT      "./src/output/"              // output folder          // source code directory

cap log close
log using "${CODE_DIR}logs/demand_estimation.log", replace

// ---------------------- PARAMETERS -------------------------------------------
global YEARS   "2005/2019"
global QUARTERS "1 2 3 4"
global TYPE     "Market"   // DB1B “Market”

di as yellow "--------------------------------------------------------------------------------------------------"
di as yellow "----------------------------------- Running demand_estimation.do --------------------------------"
di as yellow "--------------------------------------------------------------------------------------------------"

cap mkdir "$OUT"

di as yellow "    Demand estimation started for years: $YEARS, quarters: $QUARTERS, type: $TYPE"

* Source data from previous step
local source_csv "${PROC_DATA}airline_data.csv"
di as yellow "    Source data: `source_csv'"

* Column mapping in your airline_data.csv (matching processing_data.do output)
* Required identifiers
local id_prod      "product_id"            // product (itinerary/airline) id - we'll create this
local id_market    "market"                // origin–destination market id (fixed effect)
local id_time      "time"                  // time (optional control/cluster)
local id_carrier   "carrier"               // marketing carrier code 
local id_group     "carrier_dummy"          // definition for nesting e.g. major, carrier_dummy(inside vs outside)

* Demand-side quantities (matching processing_data.do variable names)
local var_fare     "average_fare"          // average ticket price
local var_nest     "lns_minus_lng"          // ln(s_jt|g) for nested logit, lng_share or lns_minus_lng
local var_fringe   "fringe_carriers"        // fringe_carriers: number of rival carriers in market // this might supposed to be something else

* Rival-based instruments (matching processing_data.do variable names)
local iv_rivaldist   "average_distance_rival" // average distance rival (for IV)
local iv_rivalpres   "average_presence_rival" // average presence rival (for IV)
local iv_rivalmkts   "average_num_destinations_rival" // options: average_num_destinations_rival average_num_markets_rival
local iv_numrivals   "rival_carriers"

*-------------------------*
* LOCALS FOR ESTIMATION  *
*-------------------------*

di as yellow "    ---------  Setting up locals for estimation ---------"

* Core X’s (match Table C2): price first so α is easy to spot, Other var: logS?
local X_exog  "share_nonstop dist_k dist_k2 lnum_fringe"
local X_core  "`var_fare' `X_exog'"

* Instruments (rival shifters)
local Z_core  "`iv_rivaldist' `iv_rivalmkts' `iv_numrivals'"

* FE and clusters
local FE      "`id_market'"              // OD fixed effects, plus time maybe `id_time'
local CL      "`id_market'"       // cluster by OD (you can switch) O-D clusters with those FE cause collinearity when IVs.

*-------------------------*
* LOAD & PREP            *
*-------------------------*
import delimited using "`source_csv'",  clear

* Make sure key variables are in proper format
* market and time are strings, carrier is string - this is fine for now
* but we need to handle them as strings

* Create product ID (combination of market, carrier, time)
egen long product_id = group(`id_market' `id_carrier' `id_time')

* Make sure time can be used for grouping (keep as string for now)
* market stays as string (it's the market identifier)
* carrier stays as string

* Create proper market shares first (as in processing_data.do commented code)
di as yellow "    ---------  Creating shares and log differences ---------"

tab year quarter, missing

//********** move from here 
// TODO:  Move avd dist  to processing_data.do
// Calculate rival average distance (for future IV use)
// bysort origin destination year quarter: egen double total_distance = total(average_distance)
// bysort origin destination year quarter: gen double average_distance_rival = ///
//     (total_distance - average_distance) / ( _N - 1 )
// drop total_distance

// // TODO:  Move fringe  to processing_data.do
// // Calculate number of fringe firms (non-major carriers) per market
// // Assuming major_carrier_flag is created in carrier_flags.do
// gen fringe = 1 - major
// bysort origin destination year quarter: egen fringe_carriers = total(fringe)
//********** move until here

* Market-level total passengers (sum over products in each market-time)
bys `id_market' `id_time': egen pax_market = total(total_passengers)

// * Market shares for logit
gen s_jt = (10 * total_passengers)/market_size // mkt size is sqrt(orig_pop*dest_pop), so share is scaled by 10
bys `id_market' `id_time': egen s_inside_t = total(s_jt)
gen s_0t = 1 - s_inside_t
replace s_0t = .0000001 if s_0t<=0  // small epsilon to avoid -inf

* log(S_t): create from market_size (which is sqrt(orig_pop*dest_pop))
gen logS = ln(market_size) // not needed for est

* Dependent variable for simple logit
gen lns_minus_lno = ln(s_jt) - ln(s_0t)

// * Nest definition: Group by  nest

// * Possible nests definitions:

// - inside goods
gen carrier_dummy = 1

// - by airline type (major vs. non-major):
// egen nest_g = group(`id_market' `id_time' major)

// - by non stop vs. connecting :
gen nonstop_route = (share_nonstop >= 0.5)  // or some threshold// egen nest_g = group(`id_market' `id_time' nonstop_route)

// - by carrier type (legacy)
// egen nest_g = group(`id_market' `id_time' legacy)  // or major, or lcc

// - by low cost carrier (LCC) vs. legacy:
// egen nest_g = group(`id_market' `id_time' lcc)  // or major, or legacy

// - by hub vs. non-hub:
gen hub_route = (origin_hub == 1 | destination_hub == 1) // egen nest_g = group(`id_market' `id_time' hub_route)

// * Create definition var with defined `var_group'
* This creates meaningful nests where multiple carriers belong to the same nest
egen nest_g = group(`id_market' `id_time' `id_group')  // or legacy, or lcc
bys nest_g: egen s_gjt = total(s_jt)                   // nest share (incl current j)

// replace s_gjt = min(max(s_gjt, .000001), .999999)

* Nested-logit transformed outcome
gen lns_minus_lng = ln(s_jt) - ln(s_gjt)

gen lng_share = ln(s_gjt) // this should be the within cluster share  s_jt // is this necessary?

// let's eliminate singleton mkts to avoid collinearity and issues when ivreghdfe
bys `id_market' `id_time': keep if _N > 1

// -------------------------*
 * Other variables X
*-------------------------*

* Distances in thousands and squared
gen dist_k   = average_distance/1000
gen dist_k2  = dist_k^2

* Log(1 + fringe carriers)
gen lnum_fringe = ln(1 + `var_fringe')

// -------------------------*
* Check variables
*-------------------------*
* Keep a clean estimation sample
di as yellow "    Initial observations: " _N
keep if s_jt>0 & s_0t>0 & s_gjt>0
di as yellow "    After share filters: " _N
foreach v of varlist `X_core' {
    drop if missing(`v')
}
di as yellow "    After X_core missing: " _N
drop if missing(lns_minus_lno) | missing(lns_minus_lng)
di as yellow "    After outcome missing: " _N

* Check instrument availability
foreach v of varlist `Z_core' {
    drop if missing(`v')
}
di as yellow "    After instrument missing: " _N

* Final sample check
di as yellow "    Final estimation sample: " _N
if _N < 50 {
    di as red "WARNING: Very small sample size for estimation!"
    di as red "Consider relaxing sample restrictions"
}

* -------------------------*
* Labels
*-------------------------*

do "$CODE_DIR/labels.do"

*-------------------------*
* SUMMARY STATS → LaTeX  *
*-------------------------*
cap which esttab
if _rc ssc install estout, replace

sum lns_minus_lno lns_minus_lng lng_share `X_core' share s_jt s_0t s_inside_t
sum `Z_core' `var_fare' dist_k dist_k2 lnum_fringe

qui estpost sum s_jt s_0t s_inside_t s_gjt rival_carriers num_markets `X_core', detail
esttab using "$OUT/demand_vars_stats.tex", replace ///
    title("Summary statistics: variables used in demand estimation") ///
    cells("mean(fmt(3)) sd(fmt(3)) min(fmt(4)) p25(fmt(4)) p50(fmt(3)) p75(fmt(3)) max(fmt(3))") ///
    label booktabs nonum

display as text "Saved: $OUT/demand_vars_stats.tex"

tab carrier, sum(legacy)

tab carrier major
*-------------------------*
* OLS (logit form)       *
*-------------------------*
di as yellow "    ---------  Running OLS logit (informal) ---------"

cap which reghdfe
if _rc ssc install reghdfe, replace
reghdfe lns_minus_lno `X_core', absorb(`FE') vce(robust) // vce(cluster `CL')
estimates store OLS

*-------------------------*
* IV-LOGIT (price endog) *
*-------------------------*
di as yellow "    ---------  Running IV logit (price endogenous) ---------"

cap which ivreghdfe
if _rc ssc install ivreghdfe, replace
ivreghdfe lns_minus_lno (`var_fare' = `Z_core') `X_exog', ///
    absorb(`FE') first savefirst savefp(fsiv_)  vce(robust) // vce(cluster `CL')

* Store F-statistic from first stage
scalar F_stat_IV = e(widstat)
di as yellow "First-stage F-statistic (IV): " F_stat_IV

estimates store IV

*-------------------------*
* NESTED-LOGIT (no IV)   *
*   ln(s_jt) - ln(s_gjt) = α p_jt + x_jt β + σ ln(s_jt|g) + FE + error
*   No endogeneity correction - treats price and within-group share as exogenous
*-------------------------*
di as yellow "    ---------  Running Nested-logit (no IV) ---------"

reghdfe lns_minus_lno `var_fare' `X_exog' `var_nest', ///
    absorb(`FE') vce(robust)

estimates store NL_OLS

*-------------------------*
* IV NESTED-LOGIT        *
*   ln(s_jt) - ln(s_gjt) = α p_jt + x_jt β + σ ln(s_jt|g) + FE + error
*   Endogenous: p_jt and ln(s_jt|g). Instruments: Z_core plus functions of rivalry.
*   We also include the exogenous X's as their own instruments by default.
*-------------------------*
di as yellow "    ---------  Running IV nested-logit (price and ln(s_jt|g) endogenous) ---------"

ivreghdfe lns_minus_lno (`var_nest' `var_fare' = `Z_core') `X_exog', ///
    absorb(`FE')  first savefirst savefp(fsivn_)  vce(robust)

* Store F-statistic from first stage
scalar F_stat_IV_NL = e(widstat)
di as yellow "First-stage F-statistic (IV Nested-Logit): " F_stat_IV_NL

estimates store IV_NL


*-------------------------*
* COLLECT & EXPORT TABLE *
*-------------------------*

// export also first stage results for IV nested-logit

esttab  fsivn_`var_fare' using "$OUT/demand_results_fs_fare.tex", replace ///
    b(4) se(4) label booktabs se star(* 0.10 ** 0.05 *** 0.01)

esttab fsivn_`var_nest' using "$OUT/demand_results_fs_nest.tex", replace ///
    b(4) se(4) label booktabs se star(* 0.10 ** 0.05 *** 0.01)

cap which esttab
esttab OLS IV NL_OLS IV_NL using "$OUT/demand_results.tex", replace ///
    b(4) se(4) ar2  label booktabs se star(* 0.10 ** 0.05 *** 0.01) ///
    keep(`var_fare' share_nonstop dist_k dist_k2 lnum_fringe `var_nest') ///
    order(`var_fare' share_nonstop dist_k dist_k2 lnum_fringe `var_nest') ///
    scalars("N Observations" "widstat F-statistic (IV)") ///
    title("Demand Estimates (Logit and Nested-Logit)") nonotes
        // stats(N r2, fmt(0 3) labels("Observations" "R^2")) ///

display as text "Saved: $OUT/demand_results.tex"

*-------------------------*
* ELASTICITIES 
*   Own-price elasticity for logit: ε_j = α * p_jt * (1 - s_jt)
*   For nested-logit, adjust by 1 - σ*(1 - s_gjt)
*-------------------------*
di as yellow "    ---------  Calculating elasticities ---------"
tempvar elast_logit elast_nlogit
scalar alpha = _b[`var_fare']  // from last model in memory; switch if needed

* Using OLS as quick check:
est restore OLS
scalar alpha_ols = _b[`var_fare']

gen `elast_logit'  = alpha_ols * `var_fare' * (1 - s_jt)

* Using nested-logit IV estimate:
est restore IV_NL
scalar alpha_nl = _b[`var_fare']
scalar sigma    = _b[`var_nest']

gen `elast_nlogit' = alpha_nl * `var_fare' * (1 - s_jt) / (1 - sigma*(1 - s_gjt))

sum `elast_logit' `elast_nlogit'

// Export elasticities
qui estpost sum `elast_logit' `elast_nlogit', detail
esttab using "$OUT/demand_elasticities.tex", replace ///
    title("Elasticities: Own-price (Logit and Nested-Logit)") ///
    cells("count mean(fmt(3)) sd(fmt(3)) min(fmt(3)) p25(fmt(3)) p50(fmt(3)) p75(fmt(3)) max(fmt(3))") ///
    label booktabs nonum ///
    varlabels(`elast_logit' "Logit Elasticity" `elast_nlogit' "Nested-Logit Elasticity")

display as text "Saved: $OUT/demand_elasticities.tex"
// export histogram of elasticities
preserve

    keep if `elast_nlogit' < 1 & `elast_nlogit' > -10 // filter for better histogram
    histogram `elast_nlogit', ///
        title("Own-Price Elasticities (Nested-Logit)") ///
        xtitle("Elasticity") ytitle("Frequency") ///
        graphregion(color(white)) plotregion(color(white)) ///
        bin(100) fcolor(navy%60) lcolor(white) lwidth(thin) ///
        xscale(range(-10 1)) xlabel(-10(2)1)

    // # use graph export
    graph export "$OUT/elasticity_logit_histogram.pdf", replace

display as text "Saved: $OUT/elasticity_logit_histogram.pdf"


log close

// ***********************************************************************