# Databricks notebook source
import os
import sys

from pyspark.sql import functions as F

# COMMAND ----------

from utils.DBPath import DBPath, save_PandasDataFrame_to_csv_FileStore
from utils.campaign_config import CampaignConfigFile, CampaignEval
from utils.helper import to_pandas

# COMMAND ----------

# conf = CampaignConfigFile("/dbfs/FileStore/media/campaign_eval/01_hde/00_cmp_inputs/cmp_list_hde_than_2023_07.csv")
conf = CampaignConfigFile("/dbfs/FileStore/media/campaign_eval/01_hde/00_cmp_inputs/cmp_list_pakc_multi.csv")

# COMMAND ----------

conf.display_details()

# COMMAND ----------

cmp = CampaignEval(conf, cmp_row_no=73)

# COMMAND ----------

from exposure import exposed

cmp.txn = cmp.txn.withColumnRenamed("store_format_group", "store_format_name")

exposure_all, exposure_reg, exposure_mech = exposed.get_exposure(cmp)

# COMMAND ----------

cmp.load_aisle(aisle_mode="target_store_config")

# COMMAND ----------

from utils import load_txn
load_txn.load_txn(cmp, txn_mode="stored_campaign_txn")

# COMMAND ----------

from exposure import exposed

# COMMAND ----------

cmp.aisle_target_store_conf

# COMMAND ----------

exposure_all, exposure_reg, exposure_mech = exposed.get_exposure(cmp)

# COMMAND ----------

x = uplift.get_cust_uplift_by_mech(cmp, cmp.feat_sku, "sku")

# COMMAND ----------

x.display()

# COMMAND ----------

from cross_cate import asso_basket

# COMMAND ----------

c, l, u = asso_basket.get_asso_kpi(cmp, cmp.feat_sku)

# COMMAND ----------

c.display()

# COMMAND ----------

l.display()

# COMMAND ----------

u.display()

# COMMAND ----------

sf = spark.createDataFrame([("test", "pre", 10), ("test", "dur", 12),
                            ("ctrl", "pre", 10), ("ctrl", "dur", 12)]
                            , ["store", "period", "uplift"])

sf.withColumn("store_period", F.concat_ws("_", "store", "period")).drop("store", "period").withColumn("dummy",F.lit("x")).groupBy("dummy").pivot("store_period").agg(F.first("uplift")).display()

# COMMAND ----------

ul = uplift.get_cust_uplift_by_mech(cmp, cmp.feat_brand_sku , "brand")

# COMMAND ----------

ul.display()

# COMMAND ----------

tbl.display()

# COMMAND ----------

tbl.groupBy("customer_micro_flag").agg(F.count("*")).display()

# COMMAND ----------

from activate import switching
sw = switching.get_cust_brand_switching_and_penetration(cmp)

# COMMAND ----------

sw.display()

# COMMAND ----------

exp_all.display()

# COMMAND ----------

exp_reg.display()

# COMMAND ----------

exp_mech.display()

# COMMAND ----------

from activate import activated

# COMMAND ----------

activated.get_cust_by_mach_activated(cmp)

# COMMAND ----------

cmp.str_mech_exposure_cmp.printSchema()

# COMMAND ----------

cmp.str_mech_exposure_cmp.display()

# COMMAND ----------

# DBTITLE 1,Dev
# MAGIC %run /Repos/thanakrit.boonquarmdee@lotuss.com/edm_media_dev/notebook/dev_notebook_fn

# COMMAND ----------

feat_list = cmp.feat_sku.toPandas()["upc_id"].to_numpy().tolist()
# cmp.txn = cmp.txn.withColumn("pkg_weight_unit", F.col("unit"))
# cmp.txn = cmp.txn.replace({"dur":"cmp"}, subset=['period_fis_wk', 'period_promo_wk', 'period_promo_mv_wk'])

matching_df = to_pandas(cmp.matched_store)
from pyspark.sql.functions import broadcast

info, tab, _, _, _, _ = sales_uplift_reg_mech(cmp.txn, "sku", cmp.feat_brand_sku, feat_list, matching_df)

# COMMAND ----------

info.display()

# COMMAND ----------

tab.display()
