from typing import List
from copy import deepcopy
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Window
from pyspark.sql import DataFrame as SparkDataFrame


def get_cust_exposed_activated(
    txn: SparkDataFrame,  
    cp_start_date: str, 
    cp_end_date: str, 
    wk_type: str,
    switching_lv: str, 
    brand_df: str,
    test_store_sf: SparkDataFrame, 
    ctr_store_list: List,
    adj_prod_sf: SparkDataFrame, 
    feat_list: List) -> SparkDataFrame:
    """Get customer exposed & unexposed / shopped, not shop
    
    Parameters
    ----------
    txn:
        Snapped transaction of ppp + pre + cmp period
    cp_start_date
    cp_end_date
    wk_type:
        "fis_week" or "promo_week"
    """
    
    

def cust_movement_2(txn: SparkDataFrame, 
                    switching_lv: str, 
                    cp_start_date: str, 
                    cp_end_date: str, 
                    wk_type: str,
                    brand_df: str,
                    test_store_sf: SparkDataFrame, 
                    adj_prod_sf: SparkDataFrame, 
                    feat_list: List
                    ):
    """Media evaluation solution, Customer movement and switching v3
    - Exposure based on each store media period
    - 
    
    """
    spark.sparkContext.setCheckpointDir('dbfs:/FileStore/thanakrit/temp/checkpoint')
    
    print('Customer movement for "OFFLINE" + "ONLINE"')
    
    #---- Get scope for brand in class / brand in subclass
    # Get section id - class id of feature products
    sec_id_class_id_feature_product = \
    (spark.table('tdm.v_prod_dim_c')
     .filter(F.col('division_id').isin([1,2,3,4,9,10,13]))
     .filter(F.col('upc_id').isin(feat_list))
     .select('section_id', 'class_id')
     .drop_duplicates()
    )
    # Get section id - class id - subclass id of feature products
    sec_id_class_id_subclass_id_feature_product = \
    (spark.table('tdm.v_prod_dim_c')
     .filter(F.col('division_id').isin([1,2,3,4,9,10,13]))
     .filter(F.col('upc_id').isin(feat_list))
     .select('section_id', 'class_id', 'subclass_id')
     .drop_duplicates()
    )
    # Get list of feature brand name
    brand_of_feature_product = \
    (spark.table('tdm.v_prod_dim_c')
     .filter(F.col('division_id').isin([1,2,3,4,9,10,13]))
     .filter(F.col('upc_id').isin(feat_list))
     .select('brand_name')
     .drop_duplicates()
    )
        
    #---- During camapign - exposed customer, 
    dur_campaign_exposed_cust = \
    (txn
     .filter(F.col('channel')=='OFFLINE') # for offline media     
     .join(test_store_sf, 'store_id','inner') # Mapping cmp_start, cmp_end, mech_count by store
     .join(adj_prod_sf, 'upc_id', 'inner')
     .fillna(str(cp_start_date), subset='c_start')
     .fillna(str(cp_end_date), subset='c_end')
     .filter(F.col('date_id').between(F.col('c_start'), F.col('c_end'))) # Filter only period in each mechanics
     .filter(F.col('household_id').isNotNull())
     
     .groupBy('household_id')
     .agg(F.min('date_id').alias('first_exposed_date'))
    )
    
    #---- During campaign - Exposed & Feature Brand buyer
    dur_campaign_brand_shopper = \
    (txn
     .filter(F.col('date_id').between(cp_start_date, cp_end_date))
     .filter(F.col('household_id').isNotNull())
     .join(brand_df, 'upc_id')
     .groupBy('household_id')
     .agg(F.min('date_id').alias('first_brand_buy_date'))
     .drop_duplicates()
    )
    
    dur_campaign_exposed_cust_and_brand_shopper = \
    (dur_campaign_exposed_cust
     .join(dur_campaign_brand_shopper, 'household_id', 'inner')
     .filter(F.col('first_exposed_date').isNotNull())
     .filter(F.col('first_brand_buy_date').isNotNull())
     .filter(F.col('first_exposed_date') <= F.col('first_brand_buy_date'))
     .select('household_id')
    )
    
    activated_brand = dur_campaign_exposed_cust_and_brand_shopper.count()
    print(f'Total exposed and brand shopper (Activated Brand) : {activated_brand}')    
    
    #---- During campaign - Exposed & Features SKU shopper
    dur_campaign_sku_shopper = \
    (txn
     .filter(F.col('date_id').between(cp_start_date, cp_end_date))
     .filter(F.col('household_id').isNotNull())
     .filter(F.col('upc_id').isin(feat_list))
     .groupBy('household_id')
     .agg(F.min('date_id').alias('first_sku_buy_date'))
     .drop_duplicates()
    )
    
    dur_campaign_exposed_cust_and_sku_shopper = \
    (dur_campaign_exposed_cust
     .join(dur_campaign_sku_shopper, 'household_id', 'inner')
     .filter(F.col('first_exposed_date').isNotNull())
     .filter(F.col('first_sku_buy_date').isNotNull())
     .filter(F.col('first_exposed_date') <= F.col('first_sku_buy_date'))
     .select('household_id')
    )
    
    activated_sku = dur_campaign_exposed_cust_and_sku_shopper.count()
    print(f'Total exposed and sku shopper (Activated SKU) : {activated_sku}')    
    
    activated_df = pd.DataFrame({'customer_exposed_brand_activated':[activated_brand], 'customer_exposed_sku_activated':[activated_sku]})
    
    #---- Find Customer movement from (PPP+PRE) -> CMP period
    
    # Existing and New SKU buyer (movement at micro level)
    prior_pre_sku_shopper = \
    (txn
     .filter(F.col('period_fis_wk').isin(['pre', 'ppp']))
     .filter(F.col('household_id').isNotNull())
     .filter(F.col('upc_id').isin(feat_list))
     .select('household_id')
     .drop_duplicates()
    )
    
    existing_exposed_cust_and_sku_shopper = \
    (dur_campaign_exposed_cust_and_sku_shopper
     .join(prior_pre_sku_shopper, 'household_id', 'inner')
     .withColumn('customer_macro_flag', F.lit('existing'))
     .withColumn('customer_micro_flag', F.lit('existing_sku'))
    )
    
    existing_exposed_cust_and_sku_shopper = existing_exposed_cust_and_sku_shopper.checkpoint()
    
    new_exposed_cust_and_sku_shopper = \
    (dur_campaign_exposed_cust_and_sku_shopper
     .join(existing_exposed_cust_and_sku_shopper, 'household_id', 'leftanti')
     .withColumn('customer_macro_flag', F.lit('new'))
    )
    new_exposed_cust_and_sku_shopper = new_exposed_cust_and_sku_shopper.checkpoint()
        
    #---- Movement macro level for New to SKU
    prior_pre_cc_txn = \
    (txn
     .filter(F.col('household_id').isNotNull())
     .filter(F.col('period_fis_wk').isin(['pre', 'ppp']))
    )

    prior_pre_store_shopper = prior_pre_cc_txn.select('household_id').drop_duplicates()

    prior_pre_class_shopper = \
    (prior_pre_cc_txn
     .join(sec_id_class_id_feature_product, ['section_id', 'class_id'])
     .select('household_id')
    ).drop_duplicates()
    
    prior_pre_subclass_shopper = \
    (prior_pre_cc_txn
     .join(sec_id_class_id_subclass_id_feature_product, ['section_id', 'class_id', 'subclass_id'])
     .select('household_id')
    ).drop_duplicates()
        
    #---- Grouping, flag customer macro flag
    new_sku_new_store = \
    (new_exposed_cust_and_sku_shopper
     .join(prior_pre_store_shopper, 'household_id', 'leftanti')
     .select('household_id', 'customer_macro_flag')
     .withColumn('customer_micro_flag', F.lit('new_to_lotus'))
    )
    
    new_sku_new_class = \
    (new_exposed_cust_and_sku_shopper
     .join(prior_pre_store_shopper, 'household_id', 'inner')
     .join(prior_pre_class_shopper, 'household_id', 'leftanti')
     .select('household_id', 'customer_macro_flag')
     .withColumn('customer_micro_flag', F.lit('new_to_class'))
    )
    
    if switching_lv == 'subclass':
        new_sku_new_subclass = \
        (new_exposed_cust_and_sku_shopper
         .join(prior_pre_store_shopper, 'household_id', 'inner')
         .join(prior_pre_class_shopper, 'household_id', 'inner')
         .join(prior_pre_subclass_shopper, 'household_id', 'leftanti')
         .select('household_id', 'customer_macro_flag')
         .withColumn('customer_micro_flag', F.lit('new_to_subclass'))
        )
        
        prior_pre_brand_in_subclass_shopper = \
        (prior_pre_cc_txn
         .join(sec_id_class_id_subclass_id_feature_product, ['section_id', 'class_id', 'subclass_id'])
         .join(brand_of_feature_product, ['brand_name'])
         .select('household_id')
        ).drop_duplicates()
        
        #---- Current subclass shopper , new to brand : brand switcher within sublass
        new_sku_new_brand_shopper = \
        (new_exposed_cust_and_sku_shopper
         .join(prior_pre_store_shopper, 'household_id', 'inner')
         .join(prior_pre_class_shopper, 'household_id', 'inner')
         .join(prior_pre_subclass_shopper, 'household_id', 'inner')
         .join(prior_pre_brand_in_subclass_shopper, 'household_id', 'leftanti')
         .select('household_id', 'customer_macro_flag')
         .withColumn('customer_micro_flag', F.lit('new_to_brand'))
        )
        
        new_sku_within_brand_shopper = \
        (new_exposed_cust_and_sku_shopper
         .join(prior_pre_store_shopper, 'household_id', 'inner')
         .join(prior_pre_class_shopper, 'household_id', 'inner')
         .join(prior_pre_subclass_shopper, 'household_id', 'inner')
         .join(prior_pre_brand_in_subclass_shopper, 'household_id', 'inner')
         .join(prior_pre_sku_shopper, 'household_id', 'leftanti')
         .select('household_id', 'customer_macro_flag')
         .withColumn('customer_micro_flag', F.lit('new_to_sku'))
        )
        
        result_movement = \
        (existing_exposed_cust_and_sku_shopper
         .unionByName(new_sku_new_store)
         .unionByName(new_sku_new_class)
         .unionByName(new_sku_new_subclass)
         .unionByName(new_sku_new_brand_shopper)
         .unionByName(new_sku_within_brand_shopper)
        )
        result_movement = result_movement.checkpoint()
        
        return result_movement, new_exposed_cust_and_sku_shopper, activated_df

    elif switching_lv == 'class':
        
        prior_pre_brand_in_class_shopper = \
        (prior_pre_cc_txn
         .join(sec_id_class_id_feature_product, ['section_id', 'class_id'])
         .join(brand_of_feature_product, ['brand_name'])
         .select('household_id')
        ).drop_duplicates()

        #---- Current subclass shopper , new to brand : brand switcher within class
        new_sku_new_brand_shopper = \
        (new_exposed_cust_and_sku_shopper
         .join(prior_pre_store_shopper, 'household_id', 'inner')
         .join(prior_pre_class_shopper, 'household_id', 'inner')
         .join(prior_pre_brand_in_class_shopper, 'household_id', 'leftanti')
         .select('household_id', 'customer_macro_flag')
         .withColumn('customer_micro_flag', F.lit('new_to_brand'))
        )
        
        new_sku_within_brand_shopper = \
        (new_exposed_cust_and_sku_shopper
         .join(prior_pre_store_shopper, 'household_id', 'inner')
         .join(prior_pre_class_shopper, 'household_id', 'inner')
         .join(prior_pre_brand_in_class_shopper, 'household_id', 'inner')
         .join(prior_pre_sku_shopper, 'household_id', 'leftanti')
         .select('household_id', 'customer_macro_flag')
         .withColumn('customer_micro_flag', F.lit('new_to_sku'))
        )
        
        result_movement = \
        (existing_exposed_cust_and_sku_shopper
         .unionByName(new_sku_new_store)
         .unionByName(new_sku_new_class)
         .unionByName(new_sku_new_brand_shopper)
         .unionByName(new_sku_within_brand_shopper)
        )
        result_movement = result_movement.checkpoint()
        return result_movement, new_exposed_cust_and_sku_shopper, activated_df

    else:
        print('Not recognized Movement and Switching level param')
        return None