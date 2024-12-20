from typing import List
from copy import deepcopy
from datetime import datetime, timedelta
from pathlib import Path
from pathlib import Path as _Path_, _windows_flavour, _posix_flavour
import os
import pprint

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Window
from pyspark.sql import DataFrame as SparkDataFrame

class DBPath(_Path_):
    
    _flavour = _windows_flavour if os.name == 'nt' else _posix_flavour
    
    def __init__(self, input_file):
        super().__init__()
        pass
        
    def __repr__(self):
        return f"DBPath class : {self.as_posix()}"
    
    def file_api(self):
        rm_first_5_str = str(self.as_posix())[5:]
        return str("/dbfs"+rm_first_5_str)
    
    def spark_api(self):
        rm_first_5_str = str(self.as_posix())[5:]
        return str("dbfs:"+rm_first_5_str)

class txnItem:

    def __init__(self,
                 end_wk_id: str = "",
                 range_n_week: int = 0,
                 str_wk_id: str = "",
                 customer_data: str = "EPOS",
                 manuf_name: bool = False,
                 store_format: str = "all",
                 division: List = [1,2,3,4,9,10,13],
                 head_col_select: List = ["transaction_uid", "date_id", "store_id", "channel"],
                 item_col_select: List = ["transaction_uid", "store_id", "date_id", "week_id",
                                          "upc_id", "net_spend_amt",
                                          "unit", "customer_id"],
                 prod_col_select: List = ["upc_id", "division_name", "department_name",
                                          "section_name", "section_id", "class_name", "class_id"]
                 ) -> None:
        """Transaction Item Class
        Params
        ----
        end_wk_id : 
            data ending week-id
        range_n_week :
            data range in number of weeks, inclusive
        str_wk_id : optional
            start of data week_id, overriding range_n_week if defined
        customer_data :
            "CC" for only MyLo customer, "EPOS" or any string for MyLo+NonMyLo
        manuf_name :
            To include "mfr_name", "mfr_name_orig" in the output DataFrame
        store_format :
            "hde", "hdet", "talad", "gofresh", "all" => "hdet + gofresh"
        division :
            List of division_id
        head_col_select :
            List of column from transaction_head
        item_col_select :
            List of column from transaction_item
        prod_col_select :
            List of column from product_table
        """

        self.spark = SparkSession.builder.appName("txnItem").getOrCreate()

        self._TBL_DATE = "tdm.v_th_date_dim"
        self._TBL_ITEM = "tdm.v_transaction_item"
        self._TBL_BASK = "tdm.v_transaction_head"
        self._TBL_PROD = "tdm.v_prod_dim_c"
        self._TBL_CUST = "tdm.v_customer_dim"
        self._TBL_MANUF = "tdm.v_mfr_dim"
        self._TBL_STORE = "tdm.v_store_dim"
        self._TBL_SM_LKP = "tdm.srai_shopping_missions_lookup"

        self._LKP_FACTS_DESC = self.spark.createDataFrame(
            [(-1,'Not available'), (1, 'Primary'), (2, 'Secondary'), (3, 'Tertiary')],
            ('facts_seg', 'description'))
        self._LKP_FACTS_LV2_DESC = self.spark.createDataFrame(
            [(-1, 'Not available'), (1, 'Primary High'), (2, 'Primary Standard'),
             (3, 'Secondary Grow Frequency'), (4, 'Secondary Grow Breadth'),
             (5, 'Tertiary Standard'), (6, 'Tertiary (OTB)')],
            ('facts_level2_seg', 'description'))
        self._LKP_TRUPRICE_DESC = self.spark.createDataFrame(
            [(1, 'Most Price Driven'), (2, 'Price Driven'), (3, 'Price Neutral'),
             (4, 'Price Insensitive'), (5, 'Most Price Insensitive')],
            ('truprice_seg', 'description'))

        self._LKP_LIFE_CYC_DESC = self.spark.read.csv('dbfs:/mnt/pvtdmbobazc01/edminput/filestore/user/thanawat_asa/lifecycle_segmentation_prod/lifecycle_name_def.csv',
                                                          header=True, inferSchema=True)
        
        self._MPPNG_LYT_DESC = {"1":'premium', "2":'valuable', "3":'potential', "4":'uncommitted'}

        self._LKP_LIFE_CYC_LV2_DESC = self.spark.read.csv('dbfs:/mnt/pvtdmbobazc01/edminput/filestore/user/thanawat_asa/lifecycle_segmentation_prod/lifecycle_detailed_name_def.csv',
                                                          header=True, inferSchema=True)

        self.wk_id_type = "fis_week_id"
        self.period_n_week = range_n_week

        self.customer_data = customer_data.lower()
        self.manuf_name = manuf_name
        self.store_format = store_format.lower()
        self.division = division
        self.head_col_select = head_col_select
        self.item_col_select = item_col_select
        self.prod_col_select = prod_col_select

        self.run_date = datetime.today().strftime("%Y-%m-%d")
        self.date_dim = self.spark.table(self._TBL_DATE).select("date_id", "week_id", "period_id", "quarter_id", "week_num_sequence").drop_duplicates().persist()

        def _get_wk_nm_sq(self, wk_id: str) -> str:
            """Get from fis week_id get week_number_sequence
            """
            return self.date_dim.where(F.col("week_id")==wk_id).select("week_num_sequence").drop_duplicates().collect()[0][0]

        # If not defined end_wk_id, use current week as end_wk_id
        if end_wk_id == "":
            self.end_wk_id = self.date_dim.where(F.col("date_id")==self.run_date).select("week_id").drop_duplicates().collect()[0][0]
            print(f"No definded end week, use today {self.run_date} with week id {self.end_wk_id}")
        else:
            self.end_wk_id = end_wk_id

        self.end_wk_nm_se = _get_wk_nm_sq(self, self.end_wk_id)
        self.end_period_id = self.date_dim.where(F.col("week_num_sequence")==self.end_wk_nm_se).select("period_id").drop_duplicates().collect()[0][0]
        self.end_qtr_id = self.date_dim.where(F.col("week_num_sequence")==self.end_wk_nm_se).select("quarter_id").drop_duplicates().collect()[0][0]

        # If not defined str_wk_id, and range_n_week = 0, then adjust period_n_week to 1 to calculate start week id
        if (str_wk_id == "") & (range_n_week == 0):
            print("Undefined str_wk_id & period_n_week -> Adjust period_n_week = 1, calculate str_wk_id")
            self.period_n_week = 1
            self.str_wk_id = \
                (self.date_dim.where(F.col("week_num_sequence")==(int(self.end_wk_nm_se) - int(self.period_n_week) + 1))
                    .select("week_id").drop_duplicates()
                ).collect()[0][0]

        # If not definded str_wk_id, use range_n_week (period_n_week) to calculate str_wk_id
        elif (str_wk_id == ""):
            print("Undefined str_wk_id use range_n_week to calculate start week id (inclusive)")
            self.str_wk_id = \
                (self.date_dim.where(F.col("week_num_sequence")==(int(self.end_wk_nm_se) - int(self.period_n_week) + 1))
                    .select("week_id").drop_duplicates()
                ).collect()[0][0]
        else:
            self.str_wk_id = str_wk_id

        self.str_wk_nm_se = _get_wk_nm_sq(self, self.str_wk_id)
        self.period_n_week = self.end_wk_nm_se - self.str_wk_nm_se + 1 # Inclusive
        self.str_date_id = self.date_dim.where(F.col("week_id")==self.str_wk_id).agg(F.min(F.col("date_id"))).collect()[0][0]
        self.end_date_id = self.date_dim.where(F.col("week_id")==self.end_wk_id).agg(F.max(F.col("date_id"))).collect()[0][0]

        self.init_txn()

    #---- Instantiate Class attribute ----
    def __repr__(self) -> str:
        return f"txnItem ({self.run_date})"

    def __str__(self) -> str:
        return f"Transaction item Object, run date {self.run_date}"

    #---- Get mapped transaction SparkDataFrame
    def get_txn(self) -> SparkDataFrame:
        """Get txn with all mapped details
        """
        return self.txn

    #---- Initial transaction SparkDataFrame
    def init_txn(self) -> SparkDataFrame:
        """Get Txn
        """
        def _map_format_channel(txn: SparkDataFrame) -> SparkDataFrame:
            """Helper function use column 'channel', from head, and 'store_format_name', from mapping between head - store
            to create offline_online_other_channel, store_format_online_subchannel_other
            """
            mapped_chan_txn = \
            (txn
                .withColumn("channel", F.trim(F.col("channel")))
                .withColumn("channel_group_forlotuss",
                            F.when(F.col("channel")=="OFFLINE", "OFFLINE")
                            .when(F.col("channel").isin(["Click and Collect", "Scheduled CC"]), "Click & Collect")
                            .when(F.col("channel").isin(["HATO"]), "Line HATO")
                            .when(F.col('channel').isin(['GHS 1','GHS 2','GHS APP','Scheduled HD']), 'Scheduled HD')
                            .when(F.col('channel').isin(['HLE','Scheduled HLE']), 'Electronic Mall')
                            .when( (F.col('channel').isin(['OnDemand HD']) ) & (F.col('store_format_name').isin(['Hypermarket', "HDE"])) & (F.col('store_id')!=5185), 'OnDemand Hypermarket') 
                            .when( (F.col('channel').isin(['OnDemand HD'])) & (F.col('store_format_name').isin(['Supermarket','Mini Super', "Talad", "GoFresh"])), 'OnDemand')
                            .when( F.col('channel').isin(['Light Delivery']), 'OnDemand')
                            .when( (F.col('channel').isin(['OnDemand HD'])) & (F.col('store_id')==5185) & (F.col('date_id')<='2023-04-06'), 'OnDemand')
                            .when( (F.col('channel').isin(['OnDemand HD'])) & (F.col('store_id')==5185) & (F.col('date_id')>='2023-04-07'), 'OnDemand Hypermarket')
                            .when(F.col("channel").isin(["Shopee", "Lazada", "O2O Lazada", "O2O Shopee"]), "Marketplace")
                            .when(F.col("channel").isin(["Ant Delivery", "Food Panda", "Grabmart",
                                                    "Happy Fresh", "Robinhood", "We Fresh", "7 MARKET"]), "Aggregator")
                            .when(F.col("channel").isin(["TRUE SMART QR"]), "Others")
                            .otherwise(F.lit("OFFLINE")))

            .withColumn("store_format_online_subchannel_other",
                        F.when(F.col("channel_group_forlotuss").isin(["Scheduled HD","Electronic Mall","OnDemand","OnDemand Hypermarket"
                                                                      ,"Click & Collect","Line HATO","Marketplace","Aggregator","Others"]), F.lit("ONLINE"))
                        .otherwise(F.col("store_format_name")))

            .withColumn("offline_online_other_channel",
                        F.when(F.col("store_format_online_subchannel_other").isin(['Hypermarket','Supermarket','Mini Super', "HDE", "Talad", "GoFresh"]), F.lit("OFFLINE"))
                        .otherwise(F.lit("ONLINE")))
            )
            return mapped_chan_txn

        item:SparkDataFrame = self.spark.table(self._TBL_ITEM).where(F.col("week_id").between(self.str_wk_id, self.end_wk_id))
        bask:SparkDataFrame = self.spark.table(self._TBL_BASK).where(F.col("week_id").between(self.str_wk_id, self.end_wk_id))

        if self.customer_data == "cc":
            item = item.where(F.col("cc_flag").isin(["cc"]))
            bask = bask.where(F.col("cc_flag").isin(["cc"]))

        #---- Item lv,
        item = (item
                #.where(F.col("net_spend_amt")>0)
                #.where(F.col("product_qty")>0)

                .withColumnRenamed("counted_qty", "count_qty")
                .withColumnRenamed("measured_qty", "measure_qty")

                .withColumn("unit", F.when(F.col("count_qty").isNotNull(), F.col("product_qty")).otherwise(F.col("measure_qty")))
            )

        #---- Basket lv,
        self.head_col_select = list(set(self.head_col_select + ["transaction_uid", "date_id", "store_id", "channel"]))
        bask = (bask
                #.where(F.col("net_spend_amt")>0)
                #.where(F.col("total_qty")>0)
                .select(self.head_col_select)
                .drop_duplicates()
                )

        # ---- Store
        if self.store_format == "hde":
            self._STR_FMT_CODE = [1,2,3]
        elif self.store_format == "hdet":
            self._STR_FMT_CODE = [1,2,3,4]
        elif self.store_format == "talad":
            self._STR_FMT_CODE = [4]
        elif self.store_format == "gofresh":
            self._STR_FMT_CODE = [5]
        elif self.store_format == "all":
            self._STR_FMT_CODE = [1,2,3,4,5]

        store:SparkDataFrame = (
            self.spark.table(self._TBL_STORE)
            .where(F.col("country")=="th")
            .where(F.col("source")=="rms")
            .where(F.col("format_id").isin(self._STR_FMT_CODE))

            .where(~F.col("store_id").like("8%"))

            .withColumn("store_format_name", F.when(F.col("format_id").isin([1,2,3]),"HDE")
                        .when(F.col("format_id")==4,"Talad")
                        .when(F.col("format_id")==5,"GoFresh")
                        .when(F.col("format_id")==6,"B2B")
                        .when(F.col("format_id")==7,"Cafe")
                        .when(F.col("format_id")==8,"Wholesale")
                        )
            .withColumnRenamed("region", "store_region")
            .select("store_id", "store_format_name", "store_region")
            .drop_duplicates()
        )

        #---- Product
        prod:SparkDataFrame = (
            self.spark.table(self._TBL_PROD)
            .where(F.col("country")=="th")
            .where(F.col("source")=="rms")
            .where(F.col("division_id").isin(self.division))
            .withColumn("department_name", F.trim(F.col("department_name")))
            .withColumn("section_name", F.trim(F.col("section_name")))
            .withColumn("class_name", F.trim(F.col("class_name")))
            .withColumn("subclass_name", F.trim(F.col("subclass_name")))
            .withColumn("brand_name", F.trim(F.col("brand_name")))
            .drop_duplicates()
        )
        filter_division_prod_id = prod.select("upc_id").drop_duplicates()

        #---- Manufacturer
        mfr:SparkDataFrame = (
            self.spark.table(self._TBL_MANUF)
            .withColumn("len_mfr_name", F.length(F.col("mfr_name")))
            .withColumn("len_mfr_id", F.length(F.regexp_extract("mfr_name", r"(-\d+)", 1)))
            .withColumn("mfr_only_name", F.expr("substring(mfr_name, 1, len_mfr_name - len_mfr_id)"))
            .withColumnRenamed("mfr_name", "mfr_name_orig")
            .withColumnRenamed("mfr_only_name", "mfr_name")
            .select("mfr_id", "mfr_name", "mfr_name_orig")
            .drop_duplicates()
        )
        #---- If need manufacturer name, add output columns, add manuf name in prod sparkDataFrame
        if self.manuf_name:
            self.prod_col_select = list(set(self.prod_col_select + ["mfr_id", "mfr_name", "mfr_name_orig"])) # Dedup add columns
            prod = prod.join(mfr, "mfr_id", "left")

        #---- Result spark dataframe
        sf = \
        (item
            .select(self.item_col_select)
            .join(F.broadcast(filter_division_prod_id), "upc_id", "inner")
            .join(F.broadcast(store), "store_id")
            .join(bask, ["transaction_uid", "store_id", "date_id"], how="left")
            .join(prod.select(self.prod_col_select), "upc_id")
        )

        #---- Mapping household_id
        party_mapping = \
        (self.spark.table(self._TBL_CUST)
            .select("customer_id", "household_id")
            .drop_duplicates()
        )

        #---- Filter out only Clubcard data
        if self.customer_data == "cc":
            sf = sf.filter(F.col("customer_id").isNotNull()).join(F.broadcast(party_mapping), "customer_id")

        else:
            cc = sf.filter(F.col("customer_id").isNotNull()).join(F.broadcast(party_mapping), "customer_id")
            non_cc = sf.filter(F.col("customer_id").isNull())

            sf = cc.unionByName(non_cc, allowMissingColumns=True)

        #---- mapping channel
        sf = _map_format_channel(sf)

        #---- print result column
        print("-"*30)
        print("Transaction details")
        print("-"*30)
        print(f"Week id range : {self.str_wk_id} - {self.end_wk_id}, total {self.period_n_week} weeks (inclusive)")
        print(f"Ending week id in period : {self.end_period_id} , quarter : {self.end_qtr_id}")
        print(f"Store format : {self.store_format} , id {self._STR_FMT_CODE}")
        print(f"Product division : {self.division}")
        print(f"Shopper scope CC or EPOS : {self.customer_data}")
        print("-"*30)

        #---- Special fix
        # create unique transaction_uid from combined key transaction_uid_store_id_date_id
        sf = (sf
              .withColumnRenamed("transaction_uid", "transaction_uid_orig")
              .withColumn("transaction_uid", F.concat_ws("_", "transaction_uid_orig", "store_id", "date_id"))
              )

        self.txn = sf

        return self.txn

    def map_cust_seg(self) -> None:
        """Map customer segment based on available segment data
        """
        @F.udf(returnType=T.StringType())
        def get_leding_wk_id(wk_id: str, leding_num: int = 1):
            """Get leading week_id
            """
            wk_num = int(wk_id)
            added_wk_num = wk_num + leding_num
            if added_wk_num % 100 > 52:
                leding_wk_num = wk_num + 100 - (52 - leding_num)
            else:
                leding_wk_num = added_wk_num

            return leding_wk_num

        def get_back_date_dim(date_id: str, bck_days: int = 180):
            """Get move back 160 days and get date_id, week_id, week_num_sequence, period_id, quarter_id
            """
            bck_date = (datetime.strptime(date_id, "%Y-%m-%d") - timedelta(days=bck_days)).strftime("%Y-%m-%d")
            bck_date_df = self.date_dim.where(F.col("date_id")==bck_date)
            bck_date_id = bck_date_df.select("date_id").drop_duplicates().collect()[0][0]
            bck_wk_id = bck_date_df.select("week_id").drop_duplicates().collect()[0][0]
            bck_p_id = bck_date_df.select("period_id").drop_duplicates().collect()[0][0]
            bck_qrt_id = bck_date_df.select("quarter_id").drop_duplicates().collect()[0][0]

            return bck_date_id, bck_wk_id, bck_p_id, bck_qrt_id

        self.spark.sparkContext.setCheckpointDir('dbfs:/mnt/pvtdmbobazc01/edminput/filestore/thanakrit/temp/checkpoint')

        # use back date 6 mth to decreaese max seg data scan
        _, bck_wk_id, bck_p_id, bck_qrt_id = get_back_date_dim(self.run_date)

        truprice = self.spark.table("tdm_seg.srai_truprice_full_history").where(F.col("period_id")>=bck_p_id).select("household_id", "truprice_seg_desc", "period_id").drop_duplicates()
        facts = self.spark.table("tdm_seg.srai_facts_full_history").where(F.col("week_id")>=bck_wk_id).select("household_id", "facts_seg_desc", "week_id").drop_duplicates()
        prfr_store = \
            (self.spark.table("tdm_seg.srai_prefstore_full_history")
             .where(F.col("period_id")>=bck_p_id)
             .select("household_id", "pref_store_id", "pref_store_format", "pref_store_region", "period_id")
             ).drop_duplicates()
        lfstg = self.spark.table('tdm.edm_lifestage2023_full_v3').where(F.col("mapping_quarter_id")>=bck_qrt_id).select("household_id", "lifestage_seg_name", "mapping_quarter_id").drop_duplicates()
        
        lfcycl = \
            (self.spark.table("tdm_seg.lifecycle_with_rfm_rev2")
             .where(F.col("week_id")>=bck_wk_id)
             .join(self._LKP_LIFE_CYC_LV2_DESC, "lifecycle_detailed_code")
             .withColumn("add_1_week_id", F.col("week_id").astype(T.IntegerType()) + 1)
             .withColumn("add_1_mod52", F.col("add_1_week_id") % 52)
             .withColumn("mapping_week_num", F.when(F.col("add_1_mod52")>52, F.col("add_1_week_id") + 100 - (52-1) ).otherwise(F.col("add_1_week_id")))
             .withColumn("mapping_week_id", F.col("mapping_week_num").astype(T.StringType()))
            #  .select("household_id", "lifecycle_name", get_leding_wk_id_udf(F.col("week_id")).alias("mapping_week_id"))
             .select("household_id", "lifecycle_name", "mapping_week_id")
             )

        lylty  = \
            (self.spark.table("tdm.srai_loyalty_history")
             .where(F.col("week_id")>=bck_wk_id)
             .replace(self._MPPNG_LYT_DESC, subset=["hde_loyalty_code"]).withColumnRenamed("hde_loyalty_code", "hde_loyalty_desc")  # type: ignore
             .replace(self._MPPNG_LYT_DESC, subset=["talad_loyalty_code"]).withColumnRenamed("talad_loyalty_code", "talad_loyalty_desc")  # type: ignore
             .replace(self._MPPNG_LYT_DESC, subset=["express_loyalty_code"]).withColumnRenamed("express_loyalty_code", "express_loyalty_desc")  # type: ignore
             .replace(self._MPPNG_LYT_DESC, subset=["online_loyalty_code"]).withColumnRenamed("online_loyalty_code", "online_loyalty_desc")  # type: ignore
             .replace(self._MPPNG_LYT_DESC, subset=["lotus_loyalty_code"]).withColumnRenamed("lotus_loyalty_code", "lotus_loyalty_desc")  # type: ignore
             .withColumn("add_1_week_id", F.col("week_id").astype(T.IntegerType()) + 1)
             .withColumn("add_1_mod52", F.col("add_1_week_id") % 52)
             .withColumn("mapping_week_num", F.when(F.col("add_1_mod52")>52, F.col("add_1_week_id") + 100 - (52-1) ).otherwise(F.col("add_1_week_id")))
             .withColumn("mapping_week_id", F.col("mapping_week_num").astype(T.StringType()))
            #  .select("household_id", "lifecycle_name", get_leding_wk_id_udf(F.col("week_id")).alias("mapping_week_id"))
             .drop("golden_record_external_id_hash", "period_id_ForHistoryOnly","add_1_week_id", "add_1_mod52", "mapping_week_num", "week_id")
             )

        # truprice = self.spark.table("tdm_seg.srai_truprice_full_history").select("household_id", "truprice_seg_desc", "period_id").drop_duplicates()
        # facts = self.spark.table("tdm_seg.srai_facts_full_history").select("household_id", "facts_seg_desc", "week_id").drop_duplicates()
        # prfr_store = \
        #     (self.spark.table("tdm_seg.srai_prefstore_full_history")
        #      .select("household_id", "pref_store_id", "pref_store_format", "pref_store_region", "period_id")
        #      ).drop_duplicates()
        # lfstg = self.spark.table('tdm.edm_lifestage_full').select("household_id", "lifestage_seg_name", "mapping_quarter_id").drop_duplicates()
        # lfcycl_desc = self.spark.read.csv('dbfs:/FileStore/thanakrit/utils/lifecycle_detailed_name_mapping.csv', header=True, inferSchema=True)
        # lfcycl = \
        #     (self.spark.table("tdm.srai_lifecycle_history")
        #      .join(lfcycl_desc, "lifecycle_detailed_code").select("household_id", "lifecycle_name", get_leding_wk_id_udf(F.col("week_id")).alias("mapping_week_id"))
        #      )

        max_trprc_p_id = truprice.agg(F.max("period_id")).drop_duplicates().collect()[0][0]
        max_facts_wk_id = facts.agg(F.max("week_id")).drop_duplicates().collect()[0][0]
        max_prfr_str_p_id = prfr_store.agg(F.max("period_id")).drop_duplicates().collect()[0][0]
        max_lfstg_mp_qrt_id = lfstg.agg(F.max("mapping_quarter_id")).drop_duplicates().collect()[0][0]
        max_lfcycl_mp_wk_id = lfcycl.agg(F.max("mapping_week_id")).drop_duplicates().collect()[0][0]
        max_lylty_mp_wk_id = lylty.agg(F.max("mapping_week_id")).drop_duplicates().collect()[0][0]

        self.map_trprc_p_id = max_trprc_p_id if int(self.end_period_id) > int(max_trprc_p_id) else self.end_period_id
        self.map_facts_wk_id = max_facts_wk_id if int(self.end_wk_id) > int(max_facts_wk_id) else self.end_wk_id
        self.map_prfr_str_p_id = max_prfr_str_p_id if int(self.end_period_id) > int(max_prfr_str_p_id) else self.end_period_id
        self.map_lfstg_mp_qrt_id = max_lfstg_mp_qrt_id if int(self.end_qtr_id) > int(max_lfstg_mp_qrt_id) else self.end_qtr_id
        self.map_lfcycl_mp_wk_id = max_lfcycl_mp_wk_id if int(self.end_wk_id) > int(max_lfcycl_mp_wk_id) else self.end_wk_id
        self.map_lylty_mp_wk_id = max_lylty_mp_wk_id if int(self.end_wk_id) > int(max_lylty_mp_wk_id) else self.end_wk_id

        print("-"*30)
        print("Mapping customer segment")
        print("-"*30)
        print(f"Data ending week : {self.end_wk_id}, period : {self.end_period_id}, quarter : {self.end_qtr_id}")
        print(f"Facts week id : {self.map_facts_wk_id}")
        print(f"Life cycle mapping week id : {self.map_lfcycl_mp_wk_id}")
        print(f"Loyalty mapping week id : {self.map_lylty_mp_wk_id}")
        print(f"Truprice period id : {self.map_trprc_p_id}")
        print(f"Preferred store period id : {self.map_prfr_str_p_id}")
        print(f"Life stage mapping quarter id : {self.map_lfstg_mp_qrt_id}")
        print("-"*30)

        cc = self.txn.where(F.col("customer_id").isNotNull())
        non_cc = self.txn.where(F.col("customer_id").isNull())

        cc_seg = \
            (cc
            .join(truprice.where(F.col("period_id")==self.map_trprc_p_id).select("household_id", "truprice_seg_desc"), "household_id", "left")
            .join(facts.where(F.col("week_id")==self.map_facts_wk_id).select("household_id", "facts_seg_desc"), "household_id", "left")
            .join(prfr_store.where(F.col("period_id")==self.map_prfr_str_p_id).select("household_id", "pref_store_id", "pref_store_format", "pref_store_region"), "household_id", "left")
            .join(lfstg.where(F.col("mapping_quarter_id")==self.map_lfstg_mp_qrt_id).select("household_id", "lifestage_seg_name"),  "household_id", "left")
            .join(lfcycl.where(F.col("mapping_week_id")==self.map_lfcycl_mp_wk_id).select("household_id", "lifecycle_name"), "household_id", "left")
            .join(lylty.where(F.col("mapping_week_id")==self.map_lylty_mp_wk_id), "household_id", "left").drop("mapping_week_id")
            .fillna(value="Unclassified", subset=["truprice_seg_desc", "facts_seg_desc",
                                                  "pref_store_id", "pref_store_format", "pref_store_region",
                                                  "lifestage_seg_name", "lifecycle_name",
                                                  "hde_loyalty_desc", "talad_loyalty_desc", "express_loyalty_desc",
                                                  "online_loyalty_desc", "lotus_loyalty_desc"])
            )
        txn_seg = cc_seg.unionByName(non_cc, allowMissingColumns=True)
        self.txn = txn_seg

    def map_shms(self) -> None:
        """Map shopping mission, for trader basket stamp "trader"
        """
        print("-"*30)
        print("Mapping Basket Shopping mission")
        print("-"*30)
        print(f"Shopping mission data week : {self.str_wk_id}  -  {self.end_wk_id}")
        print(f"Trader customer not include in shopping mission model; stamp mission as 'trader'")
        print("-"*30)

        shp_ms = \
        (self.spark
        .table("tdm_seg.shopping_missions_full_internal")
        .where(F.col("country")=="th")
        .where(F.col("week_id").between(self.str_wk_id, self.end_wk_id))
        # Fix mapping key - transaction_uid_orig
        .select(F.col("transaction_uid").alias("transaction_uid_orig"), "shopping_missions_id", "week_id")
        )

        sm_lkp = self.spark.table(self._TBL_SM_LKP)
        # Fix mapping key - transaction_uid_orig
        txn_shms = (self.txn
                    .join(shp_ms, on=["transaction_uid_orig", "week_id"], how="left")
                    .join(sm_lkp, "shopping_missions_id", "left")
                    .fillna(value="trader", subset=["shopping_missions"])
                    .drop("shopping_missions_id")
        )

        self.txn = txn_shms

    # def map_sngl_tndr(self) -> None:
    #     """Mapping single tender type, for multiple tender type stamp "multi"
    #     """
    #     BUFFER_END_DATE = 7
    #     print("-"*30)
    #     print("Mapping Basket Tender Type (single)")
    #     print("-"*30)
    #     print("Mulitiple tender type basket; stamp  as 'MULTI'")
    #     print(f"Use tender table with dp_data_dt +{BUFFER_END_DATE} days")
    #     print("-"*30)

    #     end_date_tndr = self.end_date_id + timedelta(days=BUFFER_END_DATE)

    #     tndr = \
    #     (self.spark.table('tdm.v_resa_group_resa_tran_tender')
    #      .where(F.col("country")=="th")
    #      .where(F.col("source")=="resa")
    #      .where(F.col("dp_data_dt").between(self.str_date_id, end_date_tndr))
    #     )

    #     sngl_tndr_typ = \
    #     (tndr
    #      .withColumn("tender_type_group", F.trim(F.col("tender_type_group")))
    #      .withColumn("set_tndr_type", F.array_distinct(F.collect_list(F.col("tender_type_group")).over(Window.partitionBy(["tran_seq_no", "store", "day"]))))
    #      # .withColumn("set_tndr_type", F.collect_set(F.col("tender_type_group")).over(Window.partitionBy("tran_seq_no")))
    #      .withColumn("n_tndr_type", F.size(F.col("set_tndr_type")))
    #      .select("tran_seq_no", "store", "day", "dp_data_dt", "n_tndr_type", "tender_type_group")
    #      .withColumn("sngl_tndr_type", F.when(F.col("n_tndr_type")==1, F.col("tender_type_group")).otherwise(F.lit("MULTI")))
    #      # Adjust to support new unique txn_uid from surrogate key
    #      .withColumnRenamed("tran_seq_no", "transaction_uid_orig")
    #      .withColumnRenamed("store", "store_id")
    #      .withColumnRenamed("dp_data_dt", "date_id")
    #      .select("transaction_uid_orig", "store_id", "day", "date_id", "sngl_tndr_type")
    #      .drop_duplicates()
    #     )

    #     # to-do : optimized with ragne join
    #     # txn_tndr = self.txn.join(sngl_tndr_typ, ["transaction_uid", "date_id"], "left")
    #     txn_tndr = (self.txn
        #             .withColumn("day", F.date_format(F.col("date_id"),'dd'))
        #             .join(sngl_tndr_typ.drop("date_id"), ["transaction_uid_orig", "store_id", "day"], "left")
        #             .drop("day")
        # )
        # self.txn = txn_tndr