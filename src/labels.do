// -----------------------------------------------------------------------------
// * File:    labels.do
// * Purpose: Labels for variables in the dataset
// -----------------------------------------------------------------------------

label var time "Year/Quarter"
label var market_code "Airports (origin-destination)"
label var market_name "Airports Name (origin-destination)"
label var lns_minus_lno "\$\ln s_{jt}/s_{0t}\$"
label var lns_minus_ln_h "\$\ln s_{jt|h}\$"
label var lns_minus_ln_g "\$\ln s_{ht|g}\$"
label var legacy "Legacy carrier"
label var lcc "Low-cost carrier"
label var major "Major carrier"
label var average_fare "Average fare (dollars)"
label var logS "Log of market size"
label var share_nonstop "Share nonstop flights "
label var dist_k "Average Distance (000s miles)"
label var dist_k2 "Average Distance sqr (000s miles)"
label var lnum_fringe "Log(1 + fringe carriers)"
label var fringe_carriers "Number of fringe carriers"
label var num_markets "Number of destinations served"
label var rival_carriers "Number of rival carriers"
label var rival_carriers_nest2 "Number of rival carriers lower nest"
label var s_jt "Market share"
label var s_0t "Outside share"
label var s_inside_t "Inside share sum"
label var s_g "Nest share (Upper level)"
label var s_h "Nest share (Lower level)"
label var s_j_h "\$ s_{jt|h}\$ conditional share (Firm | Lower)"
label var s_h_g "\$ s_{ht|g}\$ conditional share (Lower | Upper)"
label var nest_h "Lower level nest coefficient"
label var nest_g "Upper level nest coefficient"
label var average_num_markets_rival "Average number of rival markets"
label var average_distance_rival "Average distance to rival markets"
label var average_presence_rival "Average presence of rival carriers"
label var average_num_destinations_rival "Average number of rival destinations"

// -----------------------------------------------------------------------------
