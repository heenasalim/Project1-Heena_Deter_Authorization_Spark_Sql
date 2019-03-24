package Heena

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions
import org.apache.spark.sql.types;
import org.apache.spark.sql.SparkSession

case class smith__idrp_vend_pack_dc_combined_data_schema()
case class smith__idrp_inbound_vendor_package_dc_driver_data_schema()
case class work__idrp_kmart_vendor_package_location_store_level_data_schema()
case class smith__idrp_dc_location_current_data_schema();

object vendor {
  
  def main(args:Array[String])
  {
    
   //Reading the files in dataframe forms
   //The spark.read.csv function is dataframe 
   //when we pass case class/type to this data frame like sparkread.csv.as[Person]/csv.as[String]
   //it becomes dataset  which can be handle exacty as method 1 project1 ie select and filter
    
     val sparksession = SparkSession.builder().master("local").
     appName("kmart_vendor_pakage_location").getOrCreate();
     import sparksession.implicits._
    
     var gold__item_aprk_current_data
     = sparksession.read.format("csv")
     .option("header","true")
     .option("delimiter","|")
     .option("inferSchema","true")
     .load("C:\\Users\\jabin\\Desktop\\project_files\\gold_item_aprk_current_data.txt")
         
    var work__store_level_vend_pack_loc_final_data =
      
      sparksession.read.format("csv")
     .option("header", "true")
     .option("delimiter", "|")
     .option("inferSchema", "true")
     .load("C:\\Users\\jabin\\Desktop\\project_files\\work__store_level_vend_pack_loc_final_data.txt");
     

      work__store_level_vend_pack_loc_final_data.registerTempTable("work__store_level_vend_pack_loc_final_data_table")
     
    
    val work__idrp_vp_dc_stat =  sparksession.sqlContext.sql(
     "SELECT shc_item_id ,'K' as source_owner_cd,item_purchase_status_cd, vendor_package_id,vendor_package_purchase_status_cd,flow_type_cd as vendor_package_flow_type_cd,vendor_carton_qty,vendor_stock_nbr,ksn_package_id,ksn_purchase_status_cd,import_ind,sears_divission_nbr,sears_item_nbr,sears_sku_nbr,scan_based_trading_ind,cross_merchandising_cd,retail_carton_vendor_package_id,vendor_package_owner_cd,can_carry_model_id,'' AS days_to_check_begin_day_qty,'' as days_to_check_end_day_qty ,dotcom_allocation_ind ,retail_carton_internal_package_qty,allocation_replenishment_cd,shc_item_type_cd,idrp_order_method_cd,source_package_qty as store_source_package_qty,order_duns_nbr FROM ${work__idrp_vp_dc_start} WHERE flow_type_cd === 'JIT' OR servicing_dc_nbr > '0' "
      );
 
 work__idrp_vp_dc_stat.collect().foreach(println);

//sparksession.sqlContext.sql(
 
 //"""SELECT * FROM ${work__idrp_vp_dc_start}""")
 
 
 //.collect().foreach(println)
 
  
  
  

    var smith__idrp_eligible_loc_data =
      
    sparksession.read.format("csv")
    .option("delimiter", "|")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("C:\\Users\\jabin\\Desktop\\project_files\\SMITH_IDRP_ELIGIBLE_LOC_LOCATION.txt")
  
     
     var smith__idrp_vend_pack_dc_combined_data =
     sparksession.read.format("csv")
     .option("delimiter", "|")
     .load("C:\\Users\\jabin\\Desktop\\project_files\\smith__idrp_vend_pack_dc_combined_data.txt")
     .as[smith__idrp_vend_pack_dc_combined_data_schema]
   
           
     var smith__idrp_inbound_vendor_package_dc_driver_data =
    
     sparksession.read.format("csv") 
    .option("delimiter", "|")
    .load("C:\\Users\\jabin\\Desktop\\project_files\\smith__idrp_inbound_vendor_package_dc_driver_data.txt")
    .as[smith__idrp_inbound_vendor_package_dc_driver_data_schema]
  
     var  work__idrp_kmart_vendor_package_location_store_level_data =
     sparksession.read.format("csv")
    .option("delimiter", "|")
    .load("C:\\Users\\jabin\\Desktop\\project_files\\work__idrp_kmart_vendor_package_location_store_level_data.txt")
    .as[work__idrp_kmart_vendor_package_location_store_level_data_schema]
    
    
     var smith__idrp_dc_location_current_data =
     sparksession.read.format("csv").
     option("delimiter","|")
     .load("C:\\Users\\jabin\\Desktop\\project_files\\smith__idrp_dc_location_current_data.txt")
     .as[smith__idrp_dc_location_current_data_schema]
     
     var smith__idrp_ie_batchdate_data =
 
     sparksession.read.format("csv")
     .option("header", "true")
     .option("delimiter", "|")
     .option("inferSchema" ,"true")
     .load("C:\\Users\\jabin\\Desktop\\project_files\\smith__idrp_ie_batchdate_data .txt")
      
     
     

      
  }
  
}