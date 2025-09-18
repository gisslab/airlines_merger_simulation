// *********************************************************************************
// * Project: Airline Demand Estimation & Merger Simulation
// * File   : 02_demand_estimation.do
// * Author : Giselle Labrador-Badia
// * Date   : 8-18-2025
// *
// * PURPOSE
// *   Replicate and improve demand estimation from
// *    the reference paper using the product-market panel created earlier
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
// * Estimating equation for two-level nested logit (GEV) structure
// * Let mean utility be δ_j = x_j'β - αp_j + ξ_j
// * Under the two-level nested logit structure, the inversion/estimating equation is:
// * ln(s_j) - ln(s_0) = x_j'β - αp_j + σ_2 ln(s_j|h) + σ_1 ln(s_h|g) + ξ_j
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
// *   - IV nested-logit (price and ln(s_h) endogenous)
// *
// * REFERENCE (nested logit, incl. two-level):
// * - Train, Kenneth E. (2009). Discrete Choice Methods with Simulation, 2nd ed.
// * Cambridge University Press — Chapter 4 “Nested Logit”.
// * - Helpful note for share derivatives/elasticities:
// * Mansley, R., N. Miller, C. Ryan, and M. Weinberg (2019/2024),
// * “Notes on the Nested Logit Demand Model.” (short technical memo)
// *********************************************************************************

// *----------------------------------------------------------*
// * USER CHOICES & PATHS   *
// *----------------------------------------------------------*
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

{
    di as yellow "--------------------------------------------------------------------------------------------------"
    di as yellow "----------------------------------- Running demand_estimation.do ---------------------------------"
    di as yellow "--------------------------------------------------------------------------------------------------"
}
cap mkdir "$OUT"

di as yellow "    Demand estimation started for years: $YEARS, quarters: $QUARTERS, type: $TYPE"

* Source data from previous step
local source_csv "${PROC_DATA}airline_data.csv"
di as yellow "    Source data: `source_csv'"

// *----------------------------------------------------------*
// * COLUMN MAPPING                                           *
// * Column mapping in airline_data.csv (matching processing_data.do output)
// * Set up configuration for column mapping below
// *----------------------------------------------------------*

* Required identifiers
local id_prod      "product_id"            // product (itinerary/airline) id - we'll create this
local id_market    "market_code"                // origin–destination market id (fixed effect)
                                            // possibles (same meaning) market_airport_city mkt_code mkt_name
local id_time      "time"                  // time (optional control/cluster)
local id_carrier   "carrier"               // marketing carrier code 
local id_group_2     "lcc"                  // definition for LEVEL 2 of the nest e.g. lcc, legacy
local id_group_1     "carrier_dummy"          // definition for LEVEL 1 of the nest e.g. carrier_dummy(inside vs outside),  (CREATED LATER)
                                                //  if only one nest `id_group_1' leave empty, and fill `id_group_2' with the nest definition                      

* Demand-side quantities (matching processing_data.do variable names)
local market_size_var "pop_o_d_geo_mean"      // market size variable (sqrt(orig_pop*dest_pop)), (CREATED LATER)
            // other options generated are pop_o_d_geo_mean, pop_sum, mean_pop, max_pop 
                                                // choose one of these three alternatives if you want a different market size definition
                                                // adjust demand scale accordingly, right now set to 10
local var_fare     "average_fare"          // average ticket price 
local var_nest     "lns_minus_ln_g lns_minus_ln_h"          // ln(s_jt|h) ln(s_ht|g) for nested logit, s_minus_ln_g (CREATED LATER)
                                                // if 2-level: lns_minus_ln_g lns_minus_ln_h, if single-level: lns_minus_ln_h
local var_fringe   "fringe_carriers"        // fringe_carriers: number of rival carriers in market 

* Rival-based instruments (matching processing_data.do variable names)
local iv_rivaldist   "average_distance_rival" // average distance rival (for IV)
local iv_rivalpres   "average_presence_rival" // average presence rival (for IV)
local iv_rivalmkts   "average_num_destinations_rival" // options: average_num_destinations_rival average_num_markets_rival
local iv_numrivals   "rival_carriers"

// *----------------------------------------------------------*
// * LOCALS FOR ESTIMATION                                    *
// * CONFIG locals for estimation below (X, Z, nests, FE etc.)
// *----------------------------------------------------------*

di as yellow "    ---------  Setting up locals for estimation ---------"

* Core X’s (match Table C2): price first so α is easy to spot, 
local X_exog  "share_nonstop dist_k dist_k2 lnum_fringe"
local X_core  "`var_fare' `X_exog'"

* Instruments (rival shifters)
local Z_price  "`iv_rivaldist' `iv_rivalmkts'"
local Z_core  "`Z_price' `iv_numrivals'"
local Z_nest2 "`iv_numrivals'_nest2"

* FE and clusters
local FE      "`id_market'"              // OD fixed effects, plus time maybe `id_time'
local CL      "`id_market'"       // cluster by OD (you can switch) O-D clusters with those FE cause collinearity when IVs.

local clustering_config "vce(robust)"  // leave empty or vce(robust) for robust, or add "cluster(`CL')" or "vce(cluster <clustering_group>)" for clustered SEs

scalar scale_share = 10  // scale factor for shares, if market size is pop_o_d_geo_mean (sqrt(orig_pop*dest_pop))

scalar min_share = 10^-7  // minimum share to keep an observation (to avoid -inf in log)

local est_models "OLS IV NL_OLS IV_NL" // list of models estimates to report

// *----------------------------------------------------------*
// * LOAD & PREP                                              *
// *----------------------------------------------------------*
import delimited using "`source_csv'",  clear

* Make sure key variables are in proper format
tostring `id_market' `id_carrier' `id_time', replace

* Create product ID (combination of market, carrier, time)
egen long product_id = group(`id_market' `id_carrier' `id_time')

* Create proper market shares first (as in processing_data.do commented code)
di as yellow "    ---------  Creating shares and log differences ---------"

// * Share definitions:
* s_jt     : product share s_j
* s_j_h    : conditional share s_{j|h}
* s_h_g    : conditional share s_{h|g}
* s_g      : group share (sum of s_j over all products in group g)
* s_h      : sub-group share (sum of s_j over all products in sub-group h)
* price    : product price (your `var_fare')
* sigma1   : coefficient on ln(s_{h|g}) from your 2-level NL regression
* sigma2   : coefficient on ln(s_{j|h}) from your 2-level NL regression
* alpha_nl : the regression coefficient on price

tab year quarter, missing

// market definition: geometric mean 
cap gen double pop_o_d_geo_mean = sqrt(origin_pop * dest_pop)

// alternative market size definitions
cap gen double pop_sum = origin_pop + dest_pop
cap gen double mean_pop = (origin_pop + dest_pop)/2
cap gen double max_pop = max(origin_pop, dest_pop)

* Market-level total passengers (sum over products in each market-time), can be used for an alternative market definition
bys `id_market' `id_time': egen pax_market = total(total_passengers)

// * Market shares for logit
gen s_jt = (scale_share * total_passengers)/`market_size_var' // mkt size is sqrt(orig_pop*dest_pop), so share is scaled by 10
bys `id_market' `id_time': egen s_inside_t = total(s_jt)
gen s_0t = 1 - s_inside_t
replace s_0t = .0000001 if s_0t<=0  // small epsilon to avoid -inf

sum s_jt s_0t s_inside_t

//     Variable |        Obs        Mean    Std. dev.       Min        Max
// -------------+---------------------------------------------------------
//         s_jt |  1,636,916    .0013417    .0058076   6.12e-07   .4734702
//         s_0t |  1,677,867    .9920027     .015648   .3364701          1
//   s_inside_t |  1,677,867    .0079973     .015648          0   .6635299


* log(S_t): create from `market_size' (which is sqrt(orig_pop*dest_pop))
gen logS = ln(`market_size_var') // not needed for estimation

* Dependent variable for logit
gen lns_minus_lno = ln(s_jt) - ln(s_0t)

// * Nest definition: Group by  nest

// Possible nests definitions below:
// - by inside goods (inside nest vs outside)
gen carrier_dummy = 1
// - by airline type (major vs. non-major), though is has time variation
// - by carrier type (legacy)
// - by low cost carrier (LCC) vs. legacy: 
// - by non stop vs. connecting :
gen nonstop_route = (share_nonstop >= 0.5)  // or some threshold
// - by hub vs. non-hub:
gen hub_route = (origin_hub == 1 | destination_hub == 1) 

// if group_2 is empty and group_1 is not, use group_1 only swap
if "`id_group_2'" == "" & "`id_group_1'" != "" {
    di as yellow "    Note: nest 2 var `id_group_2' is empty, using only `id_group_1' for nest definition"
    local id_group_2 "`id_group_1'"
    local id_group_1 ""
}

// * Create definition var with defined `var_group'
* This creates meaningful nests where multiple carriers belong to the same nest
egen nest_h = group(`id_market' `id_time' `id_group_2')  // or legacy, or lcc
bys nest_h: egen s_h = total(s_jt)                   // nest share (incl current j)
gen s_j_h = s_j / s_h             // Share within sub-nest h

// replace s_h = min(max(s_h, .000001), .999999) // avoid 0 or 1

if "`id_group_1'" != "" {
    egen nest_g = group(`id_market' `id_time' `id_group_1')  // or legacy, or lcc
    bys nest_g: egen s_g = total(s_jt)                   // nest share (incl current j)
    gen s_j_g = s_jt / s_g             // Share within sub-nest g
    local nest_lv1 "lns_minus_ln_jg"
} 
else {
    di as yellow "    Note: using single-level nesting with `id_group_2' only"
    local nest_lv1 "lns_minus_ln_h"
    gen s_g = s_h  // if only one nest, then s_g =
}
// Define shares for nested logit structure
gen s_h_g = s_h / s_g             // Share of sub-nest h within top nest g

* Nested-logit transformed nest shares
gen lns_minus_ln_h = ln(s_j_h)           // Share within sub-nest h// same as ln(s_jt) - ln(s_h)
gen lns_minus_ln_g = ln(s_h_g)           // Share of sub-nest h within top nest g// same as ln(s_h) - ln(s_g) //! it's zero if single-level nest
gen lns_minus_ln_jg = ln(s_j_g)           // Share of top nest g within overall nest j// same as ln(s_g) - ln(s_j)

sum lns_minus_lno lns_minus_ln_h lns_minus_ln_g

// *----------------------------------------------------------*
//  * Other variables X
// *----------------------------------------------------------*

* Distances in thousands and squared
gen dist_k   = average_distance/1000
gen dist_k2  = dist_k^2

* Log(1 + fringe carriers)
gen lnum_fringe = ln(1 + `var_fringe')

// generate num_rivals for nested `id_market' `id_time': egen num_rivals = total(rival_carriers)
bys nest_h: gen int rival_carriers_nest2 = _N - 1

// *----------------------------------------------------------*
// * Check variables
// *----------------------------------------------------------*
* Keep a clean estimation sample
di as yellow "    Initial observations: " _N
* First filter small shares to avoid numerical issues
keep if s_jt>min_share & s_0t>min_share& s_h>min_share
di as yellow "    After share filters: " _N
* Then eliminate singleton markets (after share filtering may have created new singletons)
bys `id_market' `id_time': keep if _N > 1
di as yellow "    After dropping singletons: " _N
foreach v of varlist `X_core' {
    drop if missing(`v')
}
di as yellow "    After X_core missing: " _N
drop if missing(lns_minus_lno) | missing(lns_minus_ln_h) 
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

// *----------------------------------------------------------*
// * Labels
// *----------------------------------------------------------*

do "$CODE_DIR/labels.do"

// *----------------------------------------------------------*
// * SUMMARY STATS → LaTeX                                    *
// *----------------------------------------------------------*
cap which esttab
if _rc ssc install estout, replace

// print for log
sum lns_minus_lno lns_minus_ln_g lns_minus_ln_h s_g s_h share s_jt s_0t s_h_g s_j_h
sum `X_core' `Z_core' `Z_nest2'

qui estpost sum s_jt s_0t s_inside_t s_g s_h rival_carriers num_markets `X_core' `Z_core' `Z_nest2', detail
esttab using "$OUT/demand_vars_stats_`id_group_2'.tex", replace ///
    substitute(\_ _ ) ///
    title("Summary statistics: variables used in demand estimation") ///
    cells("mean(fmt(3)) sd(fmt(3)) min(fmt(4)) p25(fmt(4)) p50(fmt(3)) p75(fmt(3)) max(fmt(3))") ///
    label booktabs nonum

display as text "Saved: $OUT/demand_vars_stats_`id_group_2'.tex"

// *----------------------------------------------------------*
// * OLS (logit form).                                        *
// *----------------------------------------------------------*
di as yellow "    ---------  Running OLS logit  ---------"

cap which reghdfe
if _rc ssc install reghdfe, replace
reghdfe lns_minus_lno `X_core', absorb(`FE') `clustering_config' // vce(cluster `CL')
estimates store OLS

// *----------------------------------------------------------*
// * IV-LOGIT (price endog)                                   *
// *----------------------------------------------------------*
di as yellow "    ---------  Running IV logit (price endogenous) ---------"

cap which ivreghdfe
if _rc ssc install ivreghdfe, replace
ivreghdfe lns_minus_lno (`var_fare' = `Z_price') `X_exog', ///
    absorb(`FE') first savefirst savefp(fsiv_)  `clustering_config' // vce(cluster `CL')
estimates store IV

// *----------------------------------------------------------*
// * NESTED-LOGIT (no IV)                                     *
// *   ln(s_jt) - ln(s_0) = α p_jt + x_jt β + σ2 ln(s_jt|h) + σ1 ln(s_h|g) + FE + error
// *   No endogeneity correction - treats price and within-group share as exogenous
// *----------------------------------------------------------*
di as yellow "    ---------  Running Nested-logit (no IV) ---------"

reghdfe lns_minus_lno `var_fare' `X_exog' `nest_lv1', ///
    absorb(`FE') `clustering_config'

estimates store NL_OLS

// *----------------------------------------------------------*
// * IV NESTED-LOGIT - ONE-LEVEL                              *
// *   ln(s_jt) - ln(s_0) = α p_jt + x_jt β + σ ln(s_jt|h) + FE + error
// *   Endogenous: p_jt and ln(s_jt|g). Instruments: Z_core plus functions of rivalr nest.
// *   We also include the exogenous X's as their own instruments by default.
// *----------------------------------------------------------*
di as yellow "    ---------  Running IV nested-logit (price and ln(s_jt|g) endogenous) ---------"

ivreghdfe lns_minus_lno (`var_fare' `nest_lv1' = `Z_core') `X_exog', ///
    absorb(`FE')  first savefirst savefp(fsivn_)  `clustering_config'

estimates store IV_NL

// *----------------------------------------------------------*
// * IV NESTED-LOGIT   - TWO-LEVEL       *
// *   ln(s_jt) - ln(s_0) = α p_jt + x_jt β + σ2 ln(s_jt|h) + σ1 ln(s_h|g) + FE + error
// *   Endogenous: p_jt and ln(s_jt|g), ln(s_jt|h).
// *   Instruments: Z_core plus functions of rivalry.
// *   We also include the exogenous X's as their own instruments by default.
// *----------------------------------------------------------*

if "`id_group_1'" != "" {

    di as yellow "    ---------  Running IV nested-logit (price, ln(s_ht|g), ln(s_jt|h) endogenous) ---------"

    ivreghdfe lns_minus_lno (`var_fare' `var_nest' = `Z_core' `Z_nest2') `X_exog', ///
        absorb(`FE')  first savefirst savefp(fsivn_)  `clustering_config'

    estimates store IV_NL2
    local est_models "`est_models' IV_NL2"
}
else {
    di as yellow "    Note: skipping two-level IV nested-logit since `id_group_1' is empty"
}

// *----------------------------------------------------------*
// * COLLECT & EXPORT TABLE                                   *
// *----------------------------------------------------------*

di as yellow "    ---------  Exporting results to LaTeX ---------"

// export also first stage results for IV nested-logit

esttab  fsivn_`var_fare' using "$OUT/demand_results_fs_fare.tex", replace ///
        substitute(\_ _) b(4) se(4) label booktabs se star(* 0.10 ** 0.05 *** 0.01) 

local i = 1
foreach n in `var_nest' {
    esttab fsivn_`n' using "$OUT/demand_results_fs_`n'_`id_group_`i''.tex", replace ///
        substitute(\_ _) b(4) se(4) label booktabs se star(* 0.10 ** 0.05 *** 0.01) 
    local ++i
}

// print for log

esttab `est_models', b(4) se(4) ar2  label booktabs se star(* 0.10 ** 0.05 *** 0.01) ///
    keep(`var_fare' `X_exog' `nest_lv1' `var_nest') ///
    order(`var_fare' `X_exog' `nest_lv1' `var_nest') ///
    scalars("N Observations" "widstat F-statistic (IV)")

// export all other models
    
esttab `est_models' using "$OUT/demand_results_all.tex", replace ///
    b(4) se(4) ar2  label booktabs se star(* 0.10 ** 0.05 *** 0.01) ///
    keep(`var_fare' `X_exog' `nest_lv1' `var_nest') ///
    order(`var_fare' `X_exog' `nest_lv1' `var_nest') ///
    substitute(\_ _) ///
    scalars("N Observations" "widstat F-statistic (IV)") ///
    title("Demand Estimates (Logit and Nested-Logit)") nonotes
        // stats(N r2, fmt(0 3) labels("Observations" "R^2")) ///

esttab OLS IV NL_OLS IV_NL  using "$OUT/demand_results_basic.tex", replace ///
    b(4) se(4) ar2  label booktabs se star(* 0.10 ** 0.05 *** 0.01) ///
    keep(`var_fare' `X_exog' `nest_lv1') ///
    order(`var_fare' `X_exog' `nest_lv1') ///
    scalars("N Observations" "widstat F-statistic (IV)") ///
    substitute(\_ _) ///
    title("Demand Estimates (Logit and Nested-Logit)") nonotes
        // stats(N r2, fmt(0 3) labels("Observations" "R^2")) ///

esttab IV_NL IV_NL2 using "$OUT/demand_results_nested_2lv_`id_group_2'.tex", replace ///
    b(4) se(4) ar2  label booktabs se star(* 0.10 ** 0.05 *** 0.01) ///
    keep(`var_fare' `X_exog' `nest_lv1' `var_nest') ///
    order(`var_fare' `X_exog' `nest_lv1' `var_nest') ///
    substitute(\_ _) ///
    scalars("N Observations" "widstat F-statistic (IV)") ///
    title("Demand Estimates (Logit and Nested-Logit)") nonotes
        // stats(N r2, fmt(0 3) labels("Observations" "R^2")) ///

display as text "Saved: $OUT/demand_results_all.tex, $OUT/demand_results_basic.tex, $OUT/demand_results_nested_2lv_`id_group_2'.tex"

// *----------------------------------------------------------*
// * ELASTICITIES 
// *   Own-price elasticity for logit: ε_j = α * p_jt * (1 - s_jt)
// *   For nested-logit, adjust by 1 - σ*(1 - s_gjt)
// *----------------------------------------------------------*

di as yellow "    ---------  Calculating elasticities ---------"

scalar alpha = _b[`var_fare']  // from last model in memory; switch if needed

* Using OLS as quick check:

est restore OLS
scalar alpha_ols = _b[`var_fare']

gen own_el_ols  = alpha_ols * `var_fare' * (1 - s_jt)

// * Using nested-logit IV estimate:

if "`id_group_1'" != "" {

    est restore IV_NL2

    scalar sigma1    = _b[lns_minus_ln_g] 
    scalar sigma2    = _b[lns_minus_ln_h]
    di as yellow "Nesting parameters: σ1 = " sigma1 ", σ2 = " sigma2

    scalar alpha_nl = _b[`var_fare']
    di as yellow "Price coefficient: α = " alpha_nl

    scalar lambda1 = 1 - sigma1   // = 1 - σ1
    scalar lambda2 = 1 - sigma2   // = 1 - σ2

    * Two-level nested-logit own-price elasticity
    /* ε_jt =  α * p_jt * [ (1/λ1) * (1 - s_j|h) + (1/λ2) * s_j|h * (1 - s_h|g) + s_j|h * s_h|g * (1 - s_g) ]
    This line calculates the nested logit probabilities and their derivatives based on the given parameters.
    The nested logit model is an extension of the  multinomial logit model, allowing for correlation within
    groups (nests) of alternatives.

    Special cases:
    1. If σ1 = σ2 = 0 (multinomial logit), the formula simplifies to:
        ε_jj = α * p_j * (1 - s_j).
    2. If there is only one nest level (e.g., σ1 = 0), the formula reduces to:
        ε_jj = α * p_j *  [1 - σ2 * s_j|h - (1 - σ2) * s_j]/(1 - σ2) 
    */
    gen double own_el_lv2 =  alpha_nl * `var_fare' * ( (1/lambda1) * (1 - s_j_h) ///
        + (1/lambda2) * s_j_h * (1 - s_h_g) ///
        + s_j_h * s_h_g * (1 - s_g) )
}

// * Single-level nested logit elasticity

est restore IV_NL

scalar alpha_nl = _b[`var_fare']
scalar sigma    = _b[`nest_lv1']
di as yellow "Single-level nesting parameter: σ = " sigma
di as yellow "Price coefficient: α = " alpha_nl

* Single-level nested logit elasticity: 
*  ε_jj = α * p_j * [1 - σ s_{j|g} - (1 - σ) s_j] / (1 - σ)
gen double own_el_lv1 = alpha_nl * `var_fare' * ( (1 - sigma*s_j_h - (1 - sigma)*s_jt) / (1 - sigma) )

// sum for log
di as yellow "    Summary of own-price elasticities:"
sum own_el_ols own_el_lv1 own_el_lv2

// *----------------------------------------------------------*
// * Export ELASTICITIES to LaTeX
// *----------------------------------------------------------*
di as yellow "    ---------  Exporting elasticities to LaTeX ---------"

qui estpost sum own_el_ols own_el_lv1 own_el_lv2, detail
esttab using "$OUT/demand_elasticities_`id_group_2'.tex", replace ///
    title("Elasticities: Own-price (Logit and Nested-Logit)") ///
    cells("count mean(fmt(3)) sd(fmt(3)) min(fmt(3)) p25(fmt(3)) p50(fmt(3)) p75(fmt(3)) max(fmt(3))") ///
    label booktabs nonum ///
    varlabels(own_el_ols "Logit Elasticity OLS" own_el_lv1 "Nested-Logit Elasticity (1 Level)" own_el_lv2 "Nested-Logit Elasticity (2 Levels)")

display as text "Saved: $OUT/demand_elasticities.tex"

// export histogram of elasticities
di as yellow "    ---------  Exporting elasticity histogram ---------"
preserve

    * Calculate 1% and 99% percentiles for filtering
    summarize own_el_lv2, detail
    local p1 = floor(r(p1))
    // local p99 = ceil(r(p99))
    local p99 = 0

    keep if own_el_lv2 >= `p1' & own_el_lv2 <= `p99' // filter based on percentiles
    histogram own_el_lv2, ///
        title("Own-Price Elasticities (Nested-Logit, 2 Levels)") ///
        ytitle("Density") ///
        xtitle("Elasticity") ///
        graphregion(color(white)) plotregion(color(white)) ///
        bin(60) fcolor(navy%60) lcolor(white) lwidth(thin)
        // xlabel(`p1'(2)`p99') 

    graph export "$OUT/elasticity_logit_histogram_nests_lv2_`id_group_2'.pdf", replace

    display as text "Saved: $OUT/elasticity_logit_histogram_nests_lv2_`id_group_2'.pdf"

restore
preserve

    * Calculate 1% and 99% percentiles for filtering
    summarize own_el_lv1, detail
    local p1 = floor(r(p1))
    // local p99 = ceil(r(p99))
    local p99 = 0

    keep if own_el_lv1 >= `p1' & own_el_lv1 <= `p99' // filter based on percentiles
    histogram own_el_lv1, ///
        title("Own-Price Elasticities (Nested-Logit)") ///
        ytitle("Density") ///
        xtitle("Elasticity") ///
        graphregion(color(white)) plotregion(color(white)) ///
        bin(60) fcolor(navy%60) lcolor(white) lwidth(thin)
        // xlabel(`p1'(2)`p99') 

    graph export "$OUT/elasticity_logit_histogram_nests_lv1.pdf", replace

    display as text "Saved: $OUT/elasticity_logit_histogram_nests_lv1.pdf"

restore

di as yellow "    Demand estimation completed. --------------------------------------------------"
log close

// ***********************************************************************