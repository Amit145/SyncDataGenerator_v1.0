import pyspark.sql.functions as F

# Run for Incremental load or SCD2 load
input_csv_path = "/Volumes/allianz/general/files_volume/amit/scd2/"

catalog = 'allianz_coe'
schema = 'dvault_faker01'

files = dbutils.fs.ls(input_csv_path)

for f in files:
    if f.name.endswith('.csv'):
        table_name = f.name[:-4]

        # check if tables are hub or link
        if table_name.startswith('hub_') or table_name.startswith('link_'):

            # Load CSV Dataframe
            df = spark.read.format('csv').option('header', 'true').option('inferSchema', 'true').option('multiLine', 'true').load(f.path)
            
            # Load relevant table
            delta_table = spark.table(f'{catalog}.{schema}.{table_name}')

            # Get business hash key
            if table_name.startswith('hub_'):
                keys = [c for c in delta_table.columns if c.endswith("_hash_key")]
                if not keys:
                    raise Exception(f"No hash key found in {table_name}")

                business_key = keys[0]

            elif table_name.startswith('link_'):
                suffix = table_name.replace('link_','')
                if table_name == 'link_policy_product':
                    suffix = 'policy_customer'
                keys = [c for c in delta_table.columns 
                    if c.startswith(suffix) and c.endswith("_hash_key")]
                
                if not keys:
                    raise Exception(f"No hash key found in {table_name}")

                business_key = keys[0]
    
            # Get all current values of business hash key
            business_key_value = delta_table.select(business_key).distinct()

            # Get only non existing records
            new_rows = df.join(
                        business_key_value,
                        on=business_key,
                        how="left_anti")

            # Append non exisiting records
            new_rows.write.mode('append').saveAsTable(f"{catalog}.{schema}.{table_name}")

        # If Sat only append data
        elif table_name.startswith('sat_'):

            # Load CSV Dataframe
            df = spark.read.format('csv').option('header', 'true').option('inferSchema', 'true').option('multiLine', 'true').load(f.path)
            df.write.mode('append').saveAsTable(f"{catalog}.{schema}.{table_name}")