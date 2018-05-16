/*
Copyright 2016-17, Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package de.hpi.ingestion.dataimport.frsbanks.models

import java.util.Date

case class FRSEntity(
    rssd_id: Int,
    date_start: Date,
    date_end: Option[Date],
    bank_holding_company_indicator: Option[Int],
    broad_regulatory_code: Option[Int],
    authority_charter: Option[Int],
    charter_type: Option[Int],
    fbo4c9_qualification_indicator: Option[Int],
    financial_holding_company_indicator: Option[Int],
    functional_regulator: Option[Int],
    primary_insurer: Option[Int],
    fhlbs_membership_indicator: Option[Int],
    frs_membership_indicator: Option[Int],
    sec_reporting_status: Option[Int],
    establishment_type_code: Option[Int],
    banktype_analysis_code: Option[Int],
    date_start_existence: Option[Date],
    date_end_existence: Option[Date],
    fiscal_year_end: Option[Int],
    date_insured: Option[Date],
    date_opening: Option[Date],
    financial_subsidiary_holder: Option[Int],
    financial_subsidiary_indicator: Option[Int],
    iba_grandfather_indicator: Option[Int],
    ibf_indicator: Option[Int],
    head_office_rssd_id: Option[Int],
    majority_owned_by_minorities_women: Option[Int],
    legal_name: String,
    short_name: Option[String],
    numeric_search_code: Option[Int],
    organization_type: Option[Int],
    reason_termination_code: Option[Int],
    conservatorship_code: Option[Int],
    entity_type: Option[String],
    federal_reserve_regulatory_district_code: Option[Int],
    primary_activity_code: Option[String],
    city: Option[String],
    country: Option[String],
    cusip_id: Option[String],
    state: Option[String],
    place_code: Option[Int],
    state_code: Option[Int],
    home_state: Option[Int],
    street_line1: Option[String],
    street_line2: Option[String],
    zip: Option[String],
    thrift_id: Option[Int],
    thrift_holding_company_id: Option[String],
    domestic_indicator: Option[String],
    primary_aba_id: Option[Int],
    fdic_certificated: Option[Int],
    ncua_charter_id: Option[Int],
    county_code: Option[Int],
    federal_reserve_district_code: Option[Int],
    occ_charter_id: Option[Int],
    country_code: Option[Int],
    tax_id: Option[Int],
    province_region: Option[String],
    url: Option[String],
    slhc_indicator: Option[Int], // savings and loan holding company
    slhc_type_indicator: Option[Int],
    primary_federal_regulator: Option[String],
    state_incorporation_code: Option[Int],
    country_incorporation_code: Option[Int],
    state_incorporation: Option[String],
    country_incorporation: Option[String],
    lei_id: Option[String],
    bank_count: Option[Int]
)
