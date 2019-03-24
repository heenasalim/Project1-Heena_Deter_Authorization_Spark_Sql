package Heena

import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._ 
import org.apache.spark.SparkConf
case class smith_idrp_vend_pack_combined_data_schema(load_ts:String,vendor_package_id:String,activity_point_id:String,activity_point_nm:String,address_role_type_cd:String,aprk_id:String,aprk_type_cd:String,carton_per_layer_qty:String,cost_amt:String,cost_model_cd:String,cost_uom_cd:String,cost_uom_desc:String,duns_origin_cd:String,duns_owner_cd:String,eas_tag_ind:String,effective_dt:String,effective_ts:String,expration_ts:String,flow_type_cd:String,flo_type_desc:String,gtin_usage_cd:String,gtin_usage_desc:String,hierachy_instance_id:String,
    import_cd:String,import_des:String,ksn_id:String,ksn_id_effective_ts:String,ksn_id_expiration_ts:String,ksn_last_cange_user_id:String,ksn_package_id:String,land_cost_analysisi_ind:String,last_change_user_id:String,layer_per_pallet_qty:String,order_duns_import_ind:String,order_duns_nbr:String,order_ind:String,order_multiply_qty:String,order_uom_cd:String,package_id:String,projected_first_in_store_dt:String,pojected_store_sale_qty:String,projected_unit_sale_qty:String,purchase_status_cd:String,puchase_status_desc:String,puchase_status_dt:String,
    sears_buisness_desc:String,sears_buisness_nbr:String,seras_category_desc:String,sears_category_nbr:String,seras_class_desc:String,sears_class_nbr:String,sears_dc_store_order_multiply_qty:String,seras_devision_desc:String,seras_devision_nbr:String,sears_group_desc:String,seras_group_nbr:String,sears_hierarchy_exception_ind:String,sears_item_color_cd:String,sears_item_color_desc:String,seras_item_last_change_user_id:String,sears_item_nbr:String,sears_item_size_desc:String,seras_item_size_ratio_cd:String,seras_item_size_sub_desc:String,
    seras_line_desc:String,sears_line_nbr:String,sears_sku_desc:String,seras_sku_nbr:String,sears_sub_line_desc:String,seras_sub_line_nbr:String,service_area_restriction_model_id:String, shc_business_desc:String,shc_business_id:String,shc_business_level_id:String,shc_business_nbr:String,shc_business_unit_dsc:String,shc_business_unit_id:String,shc_business_unit_level_id:String,shc_business_unit_nbr:String,shc_category_desc:String,shc_category_group_desc:String,shc_category_group_id:String,shc_category_group_level_id:String,shc_category_group_level_nbr:String,shc_category_id:String,shc_category_level_id:String,shc_category_nbr:String,shc_corporate_descshc_corporate_level_id:String,shc_corporte_nbr:String,shc_deprtment_desc:String,shc_department_idshc_departement_level_id:String,shc_department_nbr:String,shc_devision_desc:String,shc_devision_id:String,shc_devision_leel_id:String,shc_devsion_nbr:String,shc_item_id:String,shc_item_type_cd:String,ship_gtin_exempy_cd:String,ship_gtin_exempt_desc:String,
    suplier_aprk_id:String,vendor_carton_ty:String,vendor_cost_format_cd:String,vendor_package_alternate_id:String,vendor_stock_desc:String,vendor_stock_nbr:String,ksn_purchase_status_cd:String,dotcom_allocation_ind:String,idrp_batch_id:String)
case class smith__idrp_shc_item_combined_data_schema(H_ITEM_ID:String,H_DIV_NBR:String,H_CATG_NBR:String,H_DEPT_NBR:String)
case class rev_dtc_file_schema(REV_ITEM:String,REV_STORE:String,REV_DTC_NUM:String,REV_DTCE_NUM:String, REV_TOTPLANS:String,
                           REV_CUR_FACE:String,REV_CUR_PRES:String,REV_CUR_FILL:String,REV_CUR_CAP:String, REV_CHKOUT:String,
                            REV_REC_CRDTE:String,REV_REC_LUDTE:String,REV_PLNBUS:String,REV_CUR_DISP_FACE:String
   )
case class replitems_rec_file_schema(load_ts:String,shc_item_id:String,shc_item_desc:String,shc_division_nbr:String,shc_division_desc:String,shc_department_nbr:String,shc_department_desc:String)
//case class smith_idrp_ksn_attribute_current_schema()
case class sbt_vend_packs_schema(VEND_PACK_ID:String,FLOWTYPE_CD:String,KSN_ID:String,ITEM_ID:String,DUNS_NBR:String)
case class sbt_ord_pt_schema(ORD_DUNS_NBR:String,LAUNCH_ID:String,VEND_ID:String,MUL_SIDTRIB_IND:String,DC_VEND_IND:String,EDI_852_IND:String,EDI_861_IND:String,ROLL_VEND:String,CLOSE_LOCN_EXCP:String,VEND_STAT:String,DR5_VEND_IND:String);
case class gold_geographic_model_str_data_schema(load_ts:String,model_nbr:String,location_nbr:String,facility_nbr:String,model_desc:String)
case class smith_idrp_eligible_loc_data_schema(loc:String,srs_loc:String,loc_ste_cd:String,srs_vndr_nbr:String,descr:String,loc_cty:String,loc_fmt_typ_cd:String)
case class sbt_launch_str_schema(LAUNCH_ID:String,LOCN_NBR:String,EFF_DT:String,TSR_PROD_CD:String)
    
object deter  {
  
   def main(args:Array[String])
 {
 val sparkconf = new SparkConf().setAppName("Test").setMaster("local[*]")
 sparkconf.set("spark.sql.crossJoin.enabled", "true")
  //var sparksession = SparkSession.builder().appName("The SparkSession").master("local")
   val sparksession = SparkSession.builder().config(sparkconf).getOrCreate()
   import sparksession.implicits._
   import sparksession.implicits.StringToColumn
   
   var load_smith_idrp_vend_pack_combined_data = sparksession.sparkContext.textFile("C:\\Users\\jabin\\Desktop\\project_files\\SMITH_IDRP_VEND_PACK_COMBINED_LOCATION.txt",6)  
   var header_smith_idrp_vend_pack_combined_data  = load_smith_idrp_vend_pack_combined_data.first;
   var smith_idrp_vend_pack_combined_data = load_smith_idrp_vend_pack_combined_data.filter( lines => lines != header_smith_idrp_vend_pack_combined_data)
   .map(line => line.split(','))
   .map( rows => smith_idrp_vend_pack_combined_data_schema(
    rows(0),rows(1),rows(2),rows(3),rows(4),rows(5),rows(6),rows(7),rows(8),rows(9),rows(10),
    rows(11),rows(12),rows(13),rows(14), rows(15),rows(16),rows(17),rows(18),rows(19),rows(20),
		rows(21),rows(22),rows(23),rows(24), rows(25),rows(26),rows(27),rows(28),rows(29),rows(30),
		rows(31),rows(32),rows(33),rows(34), rows(35),rows(36),rows(37),rows(38),rows(39),rows(40),
		rows(41),rows(42),rows(43),rows(44),rows(45),rows(46),rows(47),rows(48),rows(49),rows(50),
		rows(51), rows(52),rows(53),rows(54),rows(55),rows(56),rows(57),rows(58),rows(59),rows(60),
		rows(61),rows(62),rows(63),rows(64),rows(65),rows(66),rows(67),rows(68),rows(69),rows(70),
		rows(71),rows(72),rows(73),rows(74),rows(75),rows(76),rows(77),rows(78),rows(79),rows(80),
		rows(81),rows(82),rows(83),rows(84), rows(85),rows(86),rows(87),rows(88),rows(89),rows(90),
		rows(91),rows(92),rows(93),rows(94),rows(95),rows(96),rows(97),rows(98),rows(99),rows(100),
		rows(101),rows(102),rows(103),rows(104),rows(105),rows(106),rows(107),rows(108)))
		.toDF().cache()
   
		//smith_idrp_vend_pack_combined_data.collect().foreach(println);
   
  // var filtered_vend_packs = smith_idrp_vend_pack_combined_data.select("'ksn_purchase_status_cd' != 'U'" )
   var filtered_vend_packs = smith_idrp_vend_pack_combined_data.filter( $"ksn_purchase_status_cd" === 'U')
   //.filter( a => !a(93).trim.contains('U'))
   
   //we cant use a(93).toString == "heena" here need to use contains
   //.collect().flatten.foreach(println)
   //Array[Array[T]]  flatten is used to expand array of array
   //  If we can see we cannot provide columns name 
   // Here we need  to use index 93 a(93) for accecssing ksn_purchase_id column 
	 //we cannot directly refer ksn_purchase_id we need to calculate index each time .
	 //In this case, we have to provide  schema to file which is possible by only converting rdd to dataframe and 
	 //we can provide schema by following two ways:-
	
	 //1:Inferschema -StructFields
	 //2:case classess
   //Case classes are more benificial as they are cheapest and StructFields are exepensive.
   
   //var smith__idrp_shc_item_combined_data = sc.textFile("file://C:/Users/jabin/Desktop/SMITH_IDRP_SHC_ITEM_COMBINE_LOCATION.txt");
   // here I can see rdd is not useful with sparkcore
   //we have to use rdd with sparksql
   //again import sparkcontext of sparksession  which can allow dataframe and rdd together
   //var sparksession = SparkSession.Builder
 
   
   //************************************************************//
  //* loading rev_dtc_file file
   var load_rev_dtc_file  = sparksession.sparkContext.
   textFile("file:///C:/Users/jabin/Desktop/project_files/SPACE_PLANNING_SG_GDTU100_DTCITEM_LOCATION.txt",5);
   var header_load_rev_dtc_file = load_rev_dtc_file.first
   var rev_dtc_file = load_rev_dtc_file.filter(rows => rows != header_load_rev_dtc_file )
   .map(rows =>rows.split('|'))
   .map( row => rev_dtc_file_schema(row(0).toString(),row(1).toString,row(2).toString,row(3).toString,row(4).toString,row(5).toString,
   row(6).toString,row(7).toString,row(8).toString,row(9).toString,row(10).toString,row(11).toString,row(12).toString,row(13).toString));
   var rev_dtc_file_df = rev_dtc_file.toDF()
   
   //* loading smith__idrp_shc_item_combined_data file
   var load_smith__idrp_shc_item_combined_data  = sparksession.sparkContext.textFile("file:///C:/Users/jabin/Desktop/project_files/SMITH_IDRP_SHC_ITEM_COMBINE_LOCATION.txt",6);
   var header_smith__idrp_shc_item_combined_data  = load_smith__idrp_shc_item_combined_data.first;
   var smith__idrp_shc_item_combined_data  = load_smith__idrp_shc_item_combined_data.
   filter(rows => rows != header_smith__idrp_shc_item_combined_data).map( rows => rows.split('|'));
  
   //* loading only 4 columns from smith__idrp_shc_item_combined_data
   var hier_item_data_required = smith__idrp_shc_item_combined_data
   .map( row => smith__idrp_shc_item_combined_data_schema(row(1).toString ,row(2).toString, row(3).toString, row(4).toString))
   var hier_item_data_required_df = hier_item_data_required.toDF()
   
   //* for performing join ins aprk we need to convert rdd into dataframe
   var rev_dtc_file12= hier_item_data_required_df.join(rev_dtc_file_df ,
   hier_item_data_required_df("H_ITEM_ID").equalTo(rev_dtc_file_df("REV_ITEM")),"inner")
   .selectExpr("REV_ITEM","REV_STORE","REV_DTC_NUM","REV_DTCE_NUM","REV_TOTPLANS",
   "REV_CUR_FACE","REV_CUR_PRES","REV_CUR_FILL","REV_CUR_CAP","REV_CHKOUT","H_DIV_NBR as REV_DEPT",
   "H_CATG_NBR as REV_CATG","REV_REC_CRDTE","REV_REC_LUDTE","REV_PLNBUS",
   "REV_CUR_DISP_FACE")
   
   //collecting results
   
  
    
    
   //hier_item_data_required.collect().foreach(println)
  // rev_dtc_file.collect().foreach(println)
   //var load_smith_idrp_ksn_attribute_current = sc.textFile("file:///C:/Users/jabin/Desktop/SMITH_IDRP_KSN_ATTRIBUTE_CURRENT_LOCATION.txt");
   //var header_smith_idrp_ksn_attribute_current  = load_smith_idrp_ksn_attribute_current.first();
   //var smith_idrp_ksn_attribute_current  = load_smith_idrp_ksn_attribute_current.filter(rows => rows != header_smith_idrp_ksn_attribute_current);
  // map(rows =>rows.split("|"))
   
  
    var load_replitems_rec_file = sparksession.sparkContext.textFile("file:///C:/Users/jabin/Desktop/project_files/WORK_IDRP_ITEMS_VEND_PACKS_ARRY_LOCATION.txt",5);
    var header_replitems_rec_file = load_replitems_rec_file.first()
      //sparksession.sparkContext.textFile("file:///C:/Users/jabin/desktop/WORK_IDRP_ITEMS_VEND_PACKS_CAN_CARRY_LOCATION")
    var temp = load_replitems_rec_file.filter(row => row != header_replitems_rec_file)
    .map(rows => rows.split('|')).map( rows => replitems_rec_file_schema(rows(0), rows(1),rows(2),rows(3),rows(4),rows(5),rows(6)))
        .toDF()
    
  //  now we have to select only one column from list of 6 columns and case classes is the common schema
  //  files to the enterprise projects hence we cannot modify the case classes to sleect only one column
    //hence do following approach  two ways
   // 1>
   temp.registerTempTable("replitems_rec_file_table");
   sparksession.sqlContext.sql("select DISTINCT cast(shc_item_id as String) from replitems_rec_file_table")
   //.collect().foreach(println)
     // or  2>
   
   //again cast can be done with two ways
    var replitems_rec_file = temp.selectExpr("cast(shc_item_id as String)  ITEM").distinct()
    //.collect().foreach(println)
  //not possible string conversion replitems_rec_file.withColumn("ITEM", toString(replitems_rec_file("shc_item_id"))).select("shc_item_id");
   
    
     var load_SBT_VEND_PACKS =sparksession.sparkContext.textFile("file:///C:/Users/jabin/Desktop/project_files/SPACE_PLANNING_SBT_VEND_PACK_LOCATION.txt",5) 
     var head_SBT_VEND_PACKS = load_SBT_VEND_PACKS.first();
     var SBT_VEND_PACKS = load_SBT_VEND_PACKS.filter(rows => rows != head_SBT_VEND_PACKS )
     .map(rows=> rows.split('|')).
     map( row =>  sbt_vend_packs_schema(row(0),row(1),row(2),row(3),row(4)))
     .toDF()
    
     var load_SBT_ORD_PT = sparksession.sparkContext.
     textFile("file:///C:/Users/jabin/Desktop/project_files/SPACE_PLANNING_SBT_ORD_PT_LOCATION.txt",5)
  // var sqlcontext =  new SQLContext(sc);
     var head_SBT_ORD_PT = load_SBT_ORD_PT.first();
     var SBT_ORD_PT = load_SBT_ORD_PT.filter( rows => rows != head_SBT_ORD_PT)
     .map(rows=> rows.split('|'))
     .map(row => sbt_ord_pt_schema(row(0),row(1),row(2),row(3),row(4),row(5),row(6),row(7),row(8),row(9),row(10)))
   .toDF()
     var load_gold_geographic_model_str_data=
       
     
     sparksession.sparkContext.textFile("file:///C:/Users/jabin/Desktop/project_files/GOLD_GEOGRAPHIC_MODEL_STORE_LOCATION.txt",5)
     var head_gold_geographic_model_str_data = load_gold_geographic_model_str_data.first()
     var gold_geographic_model_str_data =  load_gold_geographic_model_str_data.filter(rows => rows != head_gold_geographic_model_str_data)
     .map( r => r.split('|'))
     .map( row => gold_geographic_model_str_data_schema(row(0), row(1),row(2),row(3),row(4)) )
     .toDF();
     
    var  MDL_STR = gold_geographic_model_str_data.selectExpr("model_nbr as MDL_NBR ", "location_nbr as LOCN_NBR");
    
    var load_smith__idrp_eligible_loc_data = sparksession.sparkContext.textFile("file:///C:/Users/jabin/Desktop/project_files/SMITH_IDRP_ELIGIBLE_LOC_LOCATION.txt",6);
    var head_smith__idrp_eligible_loc_data = load_smith__idrp_eligible_loc_data.first()
    var smith_idrp_eligible_loc_data =   load_smith__idrp_eligible_loc_data
     .filter(rows => rows != head_smith__idrp_eligible_loc_data)
     .map( r => r.split('|'))
     .map( row => smith_idrp_eligible_loc_data_schema(row(0),row(1),row(2),row(3),row(4),row(5),row(6))).toDF()
    
//smith_idrp_eligible_loc_data.filter(loc => loc <= 9000);
    //we cant do above as it is not row
var store_stat = smith_idrp_eligible_loc_data.selectExpr("cast(loc as Int) LOCN_NBR" , "trim(loc_ste_cd) as M_L_ST_CD", "loc_fmt_typ_cd")
.filter($"loc"  <= 9999).selectExpr("cast(LOCN_NBR as String)" , " M_L_ST_CD", "loc_fmt_typ_cd")

 
    
 var op1_req_mstr = rev_dtc_file12.join(store_stat, rev_dtc_file12("REV_STORE").equalTo(store_stat("LOCN_NBR")),"inner")
    .selectExpr("REV_ITEM","REV_STORE","REV_DTC_NUM","REV_DTCE_NUM","REV_TOTPLANS",
   "REV_CUR_FACE","REV_CUR_PRES","REV_CUR_FILL","REV_CUR_CAP","REV_CHKOUT","REV_DEPT","REV_CATG","REV_REC_CRDTE","REV_REC_LUDTE","REV_PLNBUS",
   "REV_CUR_DISP_FACE","M_L_ST_CD as REV_ST_CD","loc_fmt_typ_cd")
   
 // rev_dtc_file12.printSchema()//collect().foreach(println);
   // store_stat.printSchema()//collect().foreach(println);
    op1_req_mstr.printSchema()//.collect().foreach(println);
    var dept_match = op1_req_mstr.filter($"REV_DEPT" === "57")
    var dept_unmatch = op1_req_mstr.filter($"REV_DEPT" =!= "57");

   var load_SBT_LAUNCH_STR = sparksession.sparkContext.textFile("file:///C:/Users/jabin/Desktop/project_files/SPACE_PLANNING_SBT_LAUNC_STR_LOCATION.TXT",5);
   var head_SBT_LAUNCH_STR = load_SBT_LAUNCH_STR.first()
   var SBT_LAUNCH_STR =   load_SBT_LAUNCH_STR
     .filter(rows => rows != head_SBT_LAUNCH_STR)
     .map( r => r.split(','))
     .map( row => sbt_launch_str_schema(row(0),row(1),row(2),row(3)))
.toDF();
   var locn_filtered = SBT_LAUNCH_STR.selectExpr("LAUNCH_ID","LOCN_NBR","EFF_DT","TSR_PROD_CD")
  .filter($"EFF_DT".leq(date_add(current_date(),1)));
  // .collect().foreach(println);
 

  
var splitted_rep_join =    dept_unmatch.join(replitems_rec_file, dept_unmatch("REV_ITEM")===replitems_rec_file("ITEM"),"inner")
.selectExpr("REV_ITEM","REV_STORE","REV_REC_CRDTE","REV_REC_LUDTE","REV_DTC_NUM","REV_DTCE_NUM","REV_TOTPLANS",
   "REV_CUR_FACE","REV_CUR_PRES","REV_CUR_FILL","REV_CUR_CAP","REV_CHKOUT","REV_DEPT","REV_CATG","REV_PLNBUS",
   "REV_CUR_DISP_FACE","REV_ST_CD","loc_fmt_typ_cd");

var union_data = dept_match.union(splitted_rep_join)
.selectExpr("REV_ITEM","REV_STORE","REV_REC_CRDTE","REV_REC_LUDTE","REV_DTC_NUM"
    ,"REV_DTCE_NUM","REV_TOTPLANS","REV_CUR_FACE","REV_CUR_PRES","REV_CUR_FILL","REV_CUR_CAP",
    "REV_CHKOUT","REV_DEPT","REV_CATG","REV_ST_CD","loc_fmt_typ_cd");
var join2 = smith_idrp_vend_pack_combined_data.join(union_data,smith_idrp_vend_pack_combined_data("shc_item_id").equalTo(union_data("REV_ITEM")),"inner")
var join3 = join2.as('j).join(SBT_VEND_PACKS.as('s),$"j.vendor_package_id"===$"s.VEND_PACK_ID","inner")
.selectExpr("j.ksn_id as KSN_NBR","j.*","s.*");
///set org.apache.spark.sparksession.sql.crossJoin.enabled = true;
var join4 = join3.as('a).join(SBT_ORD_PT.as('b),$"a.DUNS_NBR"==="$b.ORD_DUNS_NBR","inner")
   .select($"a.ITEM_ID",
       $"a.REV_STORE" ,
       $"a.KSN_NBR" ,
       $"a.dotcom_allocation_ind".alias("KSN_DTCOM_ORDER_IND"),
       $"a.ksn_purchase_status_cd".alias("KSN_PURCH_STAT_CD"),
       $"a.vendor_package_id".alias("VEND_PACK_NBR"),
       $"a.service_area_restriction_model_id".alias("SARM_NBR"),
       $"b.LAUNCH_ID".alias("SBT_LAUNCH_ID"),
       $"a.REV_REC_CRDTE",$"a.REV_REC_LUDTE",$"a.REV_DTC_NUM",$"a.REV_DTCE_NUM",$"a.REV_TOTPLANS",
       $"a.REV_CUR_FACE",$"a.REV_CUR_PRES",$"a.REV_CUR_FILL",$"a.REV_CUR_CAP",
       $"a.REV_CHKOUT",$"a.REV_DEPT",$"a.REV_CATG",$"a.REV_ST_CD",$"a.loc_fmt_typ_cd", 
       $"a.purchase_status_cd".alias("PURCH_STAT_CD") 
 )//.drop($"a.ksn_id").drop($"b.KSN_ID");
.withColumn("SARM_NBR",when($"SARM_NBR" === ' ' ,'0')
  .otherwise($"SARM_NBR"))
.withColumn("SBT_LAUNCH_ID", when($"SBT_LAUNCH_ID".isNull ,'0').otherwise($"SBT_LAUNCH_ID"))
//join4.collect().foreach(println);
//join4.printSchema();
import org.apache.spark.sql.types.IntegerType
 var input_req_fltr= join4
    .select($"ITEM_ID",$"REV_STORE",$"KSN_NBR",$"KSN_DTCOM_ORDER_IND",$"KSN_PURCH_STAT_CD",
      $"VEND_PACK_NBR",$"SARM_NBR",$"SBT_LAUNCH_ID",
      $"REV_REC_CRDTE",$"REV_REC_LUDTE",$"REV_DTC_NUM",$"REV_DTCE_NUM",$"REV_TOTPLANS",
      $"REV_CUR_FACE",$"REV_CUR_PRES",$"REV_CUR_FILL",$"REV_CUR_CAP",
      $"REV_CHKOUT",$"REV_DEPT",$"REV_CATG",$"REV_ST_CD",$"loc_fmt_typ_cd",$"PURCH_STAT_CD")
      .filter($"SBT_LAUNCH_ID".cast(IntegerType) === 0)
   // .
   /*
 var input_req_un_fltr =  join4.select($"ITEM_ID",$"REV_STORE",$"KSN_NBR",$"KSN_DTCOM_ORDER_IND",$"KSN_PURCH_STAT_CD",
      $"VEND_PACK_NBR",$"SARM_NBR",$"SBT_LAUNCH_ID",
      $"REV_REC_CRDTE",$"REV_REC_LUDTE",$"REV_DTC_NUM",$"REV_DTCE_NUM",$"REV_TOTPLANS",
      $"REV_CUR_FACE",$"REV_CUR_PRES",$"REV_CUR_FILL",$"REV_CUR_CAP",
      $"REV_CHKOUT",$"REV_DEPT",$"REV_CATG",$"REV_ST_CD",$"loc_fmt_typ_cd",$"PURCH_STAT_CD")
      .filter($"SBT_LAUNCH_ID" === '0')
 */
//.collect().foreach(println);
 
 sparksession.conf.set("spark.sql.shuffle.partitions", 2);

//sparksession.sqlContext.sql("set spark.sql.shuffle.partition = 10”);
var sbt_vend_join1 = filtered_vend_packs.join(SBT_VEND_PACKS,filtered_vend_packs("vendor_package_id")===SBT_VEND_PACKS("DUNS_NBR"),"inner");
var sbt_ord_join1 = sbt_vend_join1.alias('c).join(SBT_ORD_PT.alias('d),$"c.DUNS_NBR"===$"d.ORD_DUNS_NBR","inner")
.selectExpr("c.ITEM_ID as ITEM_ID_1","d.LAUNCH_ID as SBT_LAUNCH_ID_1","c.*","d.*")
var input_join = input_req_fltr.alias('a).join(sbt_ord_join1.alias('b),$"a.ITEM_ID"==="b.shc_item_id","inner")
    .select( $"b.SBT_LAUNCH_ID_1",$"a.ITEM_ID",$"a.REV_STORE",
    $"a.KSN_NBR",$"a.KSN_DTCOM_ORDER_IND",$"a.KSN_PURCH_STAT_CD",
    $"a.VEND_PACK_NBR",$"a.SARM_NBR",$"a.SBT_LAUNCH_ID",
    $"a.REV_REC_CRDTE",$"a.REV_REC_LUDTE",$"a.REV_DTC_NUM",$"a.REV_DTCE_NUM",$"a.REV_TOTPLANS",
    $"a.REV_CUR_FACE",$"a.REV_CUR_PRES",$"a.REV_CUR_FILL",$"a.REV_CUR_CAP",
    $"a.REV_CHKOUT",$"a.REV_DEPT",$"a.REV_CATG",$"a.REV_ST_CD",$"loc_fmt_typ_cd"
    )
//input_req_fltr
  
 
   var input_sbt_join = input_join.
   join(locn_filtered , input_join("SBT_LAUNCH_ID_1") <=> locn_filtered("LAUNCH_ID")
   && input_join("REV_STORE") <=> locn_filtered("LOCN_NBR")  ,"inner").
       select(
   $"ITEM_ID",$"REV_STORE",$"KSN_NBR",$"KSN_DTCOM_ORDER_IND",$"KSN_PURCH_STAT_CD"
    ,$"VEND_PACK_NBR",$"SARM_NBR",$"SBT_LAUNCH_ID",
    $"REV_REC_CRDTE",$"REV_REC_LUDTE",$"REV_DTC_NUM",$"REV_DTCE_NUM",$"REV_TOTPLANS",
    $"REV_CUR_FACE",$"REV_CUR_PRES",$"REV_CUR_FILL",$"REV_CUR_CAP",
    $"REV_CHKOUT",$"REV_DEPT",$"REV_CATG",$"REV_ST_CD",$"loc_fmt_typ_cd"
    ).distinct();
 //aim is completed heena   won 
 var input_req_un_fltr =  join4.select($"ITEM_ID",$"REV_STORE",$"KSN_NBR",$"KSN_DTCOM_ORDER_IND",$"KSN_PURCH_STAT_CD",
      $"VEND_PACK_NBR",$"SARM_NBR",$"SBT_LAUNCH_ID",
      $"REV_REC_CRDTE",$"REV_REC_LUDTE",$"REV_DTC_NUM",$"REV_DTCE_NUM",$"REV_TOTPLANS",
      $"REV_CUR_FACE",$"REV_CUR_PRES",$"REV_CUR_FILL",$"REV_CUR_CAP",
      $"REV_CHKOUT",$"REV_DEPT",$"REV_CATG",$"REV_ST_CD",$"loc_fmt_typ_cd")
      .filter($"SBT_LAUNCH_ID" === '0')
      
var op1_req_prev = input_sbt_join.union(input_req_un_fltr);
var grp_11 = op1_req_prev.filter( $"SBT_LAUNCH_ID".cast(IntegerType) > 0)
var grp_22 = op1_req_prev.filter($"SBT_LAUNCH_ID" === '0');

var op1_req_new_123= grp_11.as('a).join(grp_22.as('b), $"a.ITEM_ID" <=> $"b.ITEM_ID" &&
    $"a.REV_STORE" <=> "b.REV_STORE","fullouter")
    .select(
 $"a.ITEM_ID".alias("grp11_ITEM_ID"),
 $"b.ITEM_ID".alias("grp12_ITEM_ID"),
 $"a.REV_STORE".alias("grp11_REV_STORE"),
 $"b.REV_STORE".alias("grp12_REV_STORE"),
 $"a.SBT_LAUNCH_ID".alias("grp11_SBT_LAUNCH_ID"),
 $"b.SBT_LAUNCH_ID".alias("grp12_SBT_LAUNCH_ID"),
 $"b.KSN_NBR".alias("grp12_KSN_NBR"),
 $"b.KSN_DTCOM_ORDER_IND".alias("grp12_KSN_DTCOM_ORDER_IND"),
 $"b.KSN_PURCH_STAT_CD".alias("grp12_KSN_PURCH_STAT_CD"),
 $"b.VEND_PACK_NBR".alias("grp12_VEND_PACK_NBR"),
 $"b.SARM_NBR".alias("grp12_SARM_NBR")
,$"b.SBT_LAUNCH_ID".alias("grp12_SBT_LAUNCH_ID")
,$"b.REV_REC_CRDTE".alias("grp12_REV_REC_CRDTE")
,$"b.REV_REC_LUDTE".alias("grp12_REV_REC_LUDTE")
,$"b.REV_DTC_NUM".alias("grp12_REV_DTC_NUM")
,$"b.REV_DTCE_NUM".alias("grp12_REV_DTCE_NUM")
,$"b.REV_TOTPLANS".alias("grp12_REV_TOTPLANS")
,$"b.REV_CUR_FACE".alias("grp12_REV_CUR_FACE")
,$"b.REV_CUR_PRES".alias("grp12_REV_CUR_PRES")
,$"b.REV_CUR_FILL".alias("grp12_REV_CUR_FILL")
,$"b.REV_CUR_CAP".alias("grp12_REV_CUR_CAP")
,$"b.REV_CHKOUT".alias("grp12_REV_CHKOUT")
,$"b.REV_DEPT".alias("grp12_REV_DEPT")
,$"b.REV_CATG".alias("grp12_REV_CATG")
,$"b.REV_ST_CD".alias("grp12_REV_ST_CD")
,$"b.loc_fmt_typ_cd".alias("grp12_loc_fmt_typ_cd")
,$"a.KSN_NBR".alias("grp11_KSN_NBR")
,$"a.KSN_DTCOM_ORDER_IND".alias("grp11_KSN_DTCOM_ORDER_IND")
,$"a.KSN_PURCH_STAT_CD".alias("grp11_KSN_PURCH_STAT_CD")
,$"a.VEND_PACK_NBR".alias("grp11_VEND_PACK_NBR")
,$"a.SARM_NBR".alias("grp11_SARM_NBR")
,$"a.SBT_LAUNCH_ID".alias("grp11_SBT_LAUNCH_ID")
,$"a.REV_REC_CRDTE".alias("grp11_REV_REC_CRDTE")
,$"a.REV_REC_LUDTE".alias("grp11_REV_REC_LUDTE")
,$"a.REV_DTC_NUM".alias("grp11_REV_DTC_NUM")
,$"a.REV_DTCE_NUM".alias("grp11_REV_DTCE_NUM")
,$"a.REV_TOTPLANS".alias("grp11_REV_TOTPLANS")
,$"a.REV_CUR_FACE".alias("grp11_REV_CUR_FACE")
,$"a.REV_CUR_PRES".alias("grp11_REV_CUR_PRES")
,$"a.REV_CUR_FILL".alias("grp11_REV_CUR_FILL")
,$"a.REV_CUR_CAP".alias("grp11_REV_CUR_CAP")
,$"a.REV_CHKOUT".alias("grp11_REV_CHKOUT")
,$"a.REV_DEPT".alias("grp11_REV_DEPT")
,$"a.REV_CATG".alias("grp11_REV_CATG")
,$"a.REV_ST_CD".alias("grp11_REV_ST_CD")
,$"a.loc_fmt_typ_cd".alias("grp11_loc_fmt_typ_cd")

    );

var t_data_2 = op1_req_new_123.filter( !($"grp11_REV_STORE".isNull
    and $"grp11_ITEM_ID" === "null" 
    and $"grp12_ITEM_ID".isNull)
    )
 //.filter($"grp11_REV_STORE".isNull && $"grp12_REV_STORE".isNull)
    .collect().foreach(println)
    

 }
 
   
}