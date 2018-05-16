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

package de.hpi.ingestion.dataimport.frsbanks

import java.text.SimpleDateFormat
import java.util.Date

import com.databricks.spark.xml.XmlInputFormat
import de.hpi.ingestion.dataimport.frsbanks.models.FRSEntity
import de.hpi.ingestion.framework.SparkJob
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._

import scala.xml.{NodeSeq, XML}

class BankImport extends SparkJob {
    appName = "FRS Bank Import"
    configFile = "frs_import.xml"

    var inputXML: RDD[String] = _
    var parsedEntities: RDD[FRSEntity] = _

    // $COVERAGE-OFF$
    override def load(sc: SparkContext): Unit = {
        val inputFile = settings("inputEntityFile")
        sc.hadoopConfiguration.set(XmlInputFormat.START_TAG_KEY, "<ATTRIBUTES>")
        sc.hadoopConfiguration.set(XmlInputFormat.END_TAG_KEY, "</ATTRIBUTES>")
        sc.hadoopConfiguration.set(XmlInputFormat.ENCODING_KEY, "UTF-8")
        inputXML = sc
            .newAPIHadoopFile(inputFile, classOf[XmlInputFormat], classOf[LongWritable], classOf[Text])
            .map(pair => new String(pair._2.getBytes, 0, pair._2.getLength, "UTF-8"))
    }

    override def save(sc: SparkContext): Unit = {
        parsedEntities.saveToCassandra(settings("frsKeyspace"), settings("rawEntityTable"))
    }
    // $COVERAGE-ON$

    override def run(sc: SparkContext): Unit = {
        parsedEntities = inputXML.map(parseXML)
    }

    def getString(doc: NodeSeq, xmlTag: String): Option[String] = {
        (doc \ xmlTag).headOption.map(_.text.trim)
    }

    def getInt(doc: NodeSeq, xmlTag: String): Option[Int] = {
        getString(doc, xmlTag).map(_.toInt)
//        try {
//            getString(doc, xmlTag).map(_.toInt)
//        } catch {
//            case e: Exception =>
//                println(s"Error parsing tag $xmlTag to int in doc:\n${doc.toString()}")
//                e.printStackTrace()
//                None
//        }
    }

    def getDate(doc: NodeSeq, xmlTag: String, formatString: String = "yyyy-MM-dd'T'hh:mm:ss"): Option[Date] = {
        val dateFormat = new SimpleDateFormat(formatString)
        getString(doc, xmlTag).map(dateFormat.parse)
//        try {
//            val dateFormat = new SimpleDateFormat(formatString)
//            getString(doc, xmlTag).map(dateFormat.parse)
//        } catch {
//            case e: Exception =>
//                println(s"Error parsing tag $xmlTag to date in doc:\n${doc.toString()}")
//                e.printStackTrace()
//                None
//        }
    }

    // scalastyle:off method.length
    def parseXML(xmlString: String): FRSEntity = {
        val doc = XML.loadString(xmlString)
        FRSEntity(
            rssd_id = getInt(doc, "ID_RSSD").get,
            date_start = getDate(doc, "D_DT_START").get,
            date_end = getDate(doc, "D_DT_END"),
            bank_holding_company_indicator = getInt(doc, "BHC_IND"),
            broad_regulatory_code = getInt(doc, "BROAD_REG_CD"),
            authority_charter = getInt(doc, "CHTR_AUTH_CD"),
            charter_type = getInt(doc, "CHTR_TYPE_CD"),
            fbo4c9_qualification_indicator = getInt(doc, "FBO_4C9_IND"),
            financial_holding_company_indicator = getInt(doc, "FHC_IND"),
            functional_regulator = getInt(doc, "FUNC_REG"),
            primary_insurer = getInt(doc, "INSUR_PRI_CD"),
            fhlbs_membership_indicator = getInt(doc, "MBR_FHLBS_IND"),
            frs_membership_indicator = getInt(doc, "MBR_FRS_IND"),
            sec_reporting_status = getInt(doc, "SEC_RPTG_STATUS"),
            establishment_type_code = getInt(doc, "EST_TYPE_CD"),
            banktype_analysis_code = getInt(doc, "BNK_TYPE_ANALYS_CD"),
            date_start_existence = getDate(doc, "D_DT_EXIST_CMNC", "yyyy-MM-dd"),
            date_end_existence = getDate(doc, "D_DT_EXIST_TERM"),
            fiscal_year_end = getInt(doc, "FISC_YREND_MMDD"),
            date_insured = getDate(doc, "D_DT_INSUR"),
            date_opening = getDate(doc, "D_DT_OPEN", "yyyy-MM-dd"),
            financial_subsidiary_holder = getInt(doc, "FNCL_SUB_HOLDER"),
            financial_subsidiary_indicator = getInt(doc, "FNCL_SUB_IND"),
            iba_grandfather_indicator = getInt(doc, "IBA_GRNDFTHR_IND"),
            ibf_indicator = getInt(doc, "IBF_IND"),
            head_office_rssd_id = getInt(doc, "ID_RSSD_HD_OFF"),
            majority_owned_by_minorities_women = getInt(doc, "MJR_OWN_MNRTY"),
            legal_name = getString(doc, "NM_LGL").get,
            short_name = getString(doc, "NM_SHORT"),
            numeric_search_code = getInt(doc, "NM_SRCH_CD"),
            organization_type = getInt(doc, "ORG_TYPE_CD"),
            reason_termination_code = getInt(doc, "REASON_TERM_CD"),
            conservatorship_code = getInt(doc, "CNSRVTR_CD"),
            entity_type = getString(doc, "ENTITY_TYPE"),
            federal_reserve_regulatory_district_code = getInt(doc, "AUTH_REG_DIST_FRS"),
            primary_activity_code = getString(doc, "ACT_PRIM_CD"),
            city = getString(doc, "CITY"),
            country = getString(doc, "CNTRY_NM"),
            cusip_id = getString(doc, "ID_CUSIP"),
            state = getString(doc, "STATE_ABBR_NM"),
            place_code = getInt(doc, "PLACE_CD"),
            state_code = getInt(doc, "STATE_CD"),
            home_state = getInt(doc, "STATE_HOME_CD"),
            street_line1 = getString(doc, "STREET_LINE1"),
            street_line2 = getString(doc, "STREET_LINE2"),
            zip = getString(doc, "ZIP_CD"),
            thrift_id = getInt(doc, "ID_THRIFT"),
            thrift_holding_company_id = getString(doc, "ID_THRIFT_HC"),
            domestic_indicator = getString(doc, "DOMESTIC_IND"),
            primary_aba_id = getInt(doc, "ID_ABA_PRIM"),
            fdic_certificated = getInt(doc, "ID_FDIC_CERT"),
            ncua_charter_id = getInt(doc, "ID_NCUA"),
            county_code = getInt(doc, "COUNTY_CD"),
            federal_reserve_district_code = getInt(doc, "DIST_FRS"),
            occ_charter_id = getInt(doc, "ID_OCC"),
            country_code = getInt(doc, "CNTRY_CD"),
            tax_id = getInt(doc, "ID_TAX"),
            province_region = getString(doc, "PROV_REGION"),
            url = getString(doc, "URL"),
            slhc_indicator = getInt(doc, "SLHC_IND"),
            slhc_type_indicator = getInt(doc, "SLHC_TYPE_IND"),
            primary_federal_regulator = getString(doc, "PRIM_FED_REG"),
            state_incorporation_code = getInt(doc, "STATE_INC_CD"),
            country_incorporation_code = getInt(doc, "CNTRY_INC_CD"),
            state_incorporation = getString(doc, "STATE_INC_ABBR_NM"),
            country_incorporation = getString(doc, "CNTRY_INC_NM"),
            lei_id = getString(doc, "ID_LEI"),
            bank_count = getInt(doc, "BANK_CNT")
        )
    }
    // scalastyle:on method.length

}
