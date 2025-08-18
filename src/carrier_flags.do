// -----------------------------------------------------------------------------
// * File:    processing_data.do
// * Purpose: Contains logit to create major carrier flags
// *          and carrier type classifications
// -----------------------------------------------------------------------------

gen major = 0

// Use inlist()  does not allowed more than 10 itemrms
replace major = 1 if year==2019 & inlist(carrier,"AS","G4","AA","5Y","DL","MQ","F9","HA")
replace major = 1 if year==2019 & inlist(carrier,"B6","RC","OO","WN","NK","UA")
replace major = 1 if year==2018 & inlist(carrier,"AS","G4","AA","5Y","DL","MQ","EV","F9")
replace major = 1 if year==2018 & inlist(carrier,"HA","B6","OO","WN","NK","UA","VX")
replace major = 1 if year==2017 & inlist(carrier,"AS","G4","AA","5Y","DL","F9","HA","B6")
replace major = 1 if year==2017 & inlist(carrier,"OO","WN","NK","UA","VX")
replace major = 1 if year==2016 & inlist(carrier,"AS","AA","5Y","DL","MQ","EV","F9","HA")
replace major = 1 if year==2016 & inlist(carrier,"B6","OO","WN","NK","UA","VX")
replace major = 1 if year==2015 & inlist(carrier,"AS","AA","5Y","DL","MQ","EV","F9","HA")
replace major = 1 if year==2015 & inlist(carrier,"B6","OO","WN","NK","UA","US","VX")
replace major = 1 if year==2014 & inlist(carrier,"AS","AA","5Y","DL","MQ","EV","F9","HA")
replace major = 1 if year==2014 & inlist(carrier,"B6","OO","WN","NK","UA","US","VX")
replace major = 1 if year==2013 & inlist(carrier,"8C","AS","AA","MQ","5Y","DL","EV","F9")
replace major = 1 if year==2013 & inlist(carrier,"HA","B6","K4","OO","WN","NK","UA","US")
replace major = 1 if year==2013 & inlist(carrier,"VX","WO")
replace major = 1 if year==2012 & inlist(carrier,"8C","AS","AA","MQ","5Y","DL","F9","HA")
replace major = 1 if year==2012 & inlist(carrier,"B6","K4","OO","WN","UA","US","WO")
replace major = 1 if year==2011 & inlist(carrier,"8C","AS","AA","MQ","5Y","CO","CS","DL")
replace major = 1 if year==2011 & inlist(carrier,"F9","HA","B6","OO","WN","UA","US","WO")
replace major = 1 if year==2010 & inlist(carrier,"GB","8C","AS","AA","MQ","EV","5Y","OH")
replace major = 1 if year==2010 & inlist(carrier,"CO","DO","F9","HA","B6","NW","OO","WN")
replace major = 1 if year==2010 & inlist(carrier,"UA","US","WO")
replace major = 1 if year==2009 & inlist(carrier,"GB","8C","AS","AA","MQ","EV","5Y","OH")
replace major = 1 if year==2009 & inlist(carrier,"CO","DL","F9","HA","B6","YV","NW","OO")
replace major = 1 if year==2009 & inlist(carrier,"WN","UA","US","WO")
replace major = 1 if year==2008 & inlist(carrier,"GB","8C","AS","AA","MQ","EV","5Y","OH")
replace major = 1 if year==2008 & inlist(carrier,"CO","DL","F9","B6","YV","NW","OO","WN")
replace major = 1 if year==2008 & inlist(carrier,"UA","US","WO")
replace major = 1 if year==2007 & inlist(carrier,"GB","8C","AS","AA","MQ","HP","EV","OH")
replace major = 1 if year==2007 & inlist(carrier,"CO","DL","F9","B6","YV","NW","OO","WN")
replace major = 1 if year==2007 & inlist(carrier,"UA","US","WO")
replace major = 1 if year==2006 & inlist(carrier,"GB","8C","AS","AA","MQ","TZ","HP","EV")
replace major = 1 if year==2006 & inlist(carrier,"OH","CO","DL","B6","NW","OO","WN","UA")
replace major = 1 if year==2006 & inlist(carrier,"US","WO")
replace major = 1 if year==2005 & inlist(carrier,"AS","AA","MQ","TZ","HP","OH","CO","EV")
replace major = 1 if year==2005 & inlist(carrier,"DL","B6","NW","WN","UA","US","WO")

// Create carrier type classifications
gen legacy = 0
gen lcc = 0

// Legacy carriers (traditional full-service carriers)
replace legacy = 1 if inlist(carrier,"AS","AQ","AA","CO","DL")
replace legacy = 1 if inlist(carrier,"HA","NW","TW","UA","US")

// Low-cost carriers
replace lcc = 1 if inlist(carrier,"TZ","F9","YV","DH","B6","YX","SX","XP","WN")
replace lcc = 1 if inlist(carrier,"NK","SY","U5","VC")


di as text "    Major carrier flags and carrier type classifications created successfully."
