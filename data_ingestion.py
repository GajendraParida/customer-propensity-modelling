import pandas as pd
import numpy as np
from datetime import datetime
import pyodbc

def fetch_multiple_dataframes(schema, queries, loc, chunksize=10000):
    """
    Fetch multiple dataframes from a database using provided queries and connection details.
    """
    # Set connection details based on the location (Azure or Local)
    if loc == 'Azure':
        server = ''
        database = 'IFBDW'
        username = ''
        password = ''
    elif loc == 'Local':
        server = 'your_local_server_name'
        database = 'your_local_database_name'
        username = 'your_local_username'
        password = 'your_local_password'
    else:
        raise ValueError("Invalid 'loc' value. Use 'Azure' or 'Local'.")

    driver = '{ODBC Driver 17 for SQL Server}'
    conn_str = f"""DRIVER={driver};SERVER={''};DATABASE={''};UID={''};PWD={''}"""
    
    dataframes = {}

    try:
        # Establish database connection
        conn = pyodbc.connect(conn_str)

        # Loop through queries and fetch data
        for table_name, query in queries.items():
            chunk_list = []  # For large datasets, fetch data in chunks
            for chunk in pd.read_sql_query(query, conn, chunksize=chunksize):
                chunk_list.append(chunk)
            
            # Concatenate chunks into a single DataFrame
            final_df = pd.concat(chunk_list, ignore_index=True)
            dataframes[table_name] = final_df

        return dataframes
    
    except Exception as e:
        print(f"Error: {e}")
        return None
    
    finally:
        # Close connection after fetching data
        if 'conn' in locals() and conn is not None:
            conn.close()


def preprocessing(dataframes):
    import pandas as pd
    """
    Example preprocessing function to work with fetched dataframes.
    """
    try:
        file_path1 = r"Data\Bangalore\Initload_202407191043.csv"
        initdf=pd.read_csv(file_path1,nrows=100)
        initdf.shape
        initdf.rename(columns={'zzpurchase_date':'zzpurchaseDate','zzinstall_date': 'zzinstallDate','warranty_sdate':'zzstartDate','warranty_edate': 'zzendDate',
                    'zzr3mat_id':'zzr3matId','zzr3ser_no':'zzr3serNo'},inplace=True) 
        initdf["zzinstallDate"] = pd.to_datetime(initdf["zzinstallDate"],errors='coerce')
        initdf["zzendDate"] = pd.to_datetime(initdf["zzendDate"],errors='coerce')
        initdf['zzsoldto']=initdf['zzsoldto'].map(str)
        initdf['zzr3serNo']=initdf['zzr3serNo'].map(str)
        #addition_cat_column
        cat_num = {"AC":1,"DW": 2,"MW" : 3,"WM" : 4}
        initdf["cat_num"] = initdf["zz0012"].map(cat_num)
        #creating a copy for making changes in date column
        init_copy = initdf.copy()
        init_copy["zzendDate"] = pd.to_datetime(init_copy["zzendDate"],errors='coerce')
        init_copy["zzpurchaseDate"] = pd.to_datetime(init_copy["zzpurchaseDate"],errors='coerce')
        #first_variable: No. of products purchased (zzr3serNo from INIT Table) :unique
        unique_ser_no = initdf.groupby("zzsoldto")["zzr3serNo"].nunique()
        customdata = pd.DataFrame({"customer":unique_ser_no.index,"No_of_unique_products_purchased":unique_ser_no.values})
        #second variable: Frequency of purchase
        first_purchase_date = init_copy.groupby("zzsoldto")["zzpurchaseDate"].min()
        current_date = datetime.now()
        no_of_days_since_first_purchase = (current_date - first_purchase_date).dt.days
        customdata["no_of_days_since_first_purchase"] = no_of_days_since_first_purchase.values
        customdata["frequency_of_purchase"] = customdata["No_of_unique_products_purchased"]/customdata["no_of_days_since_first_purchase"]
        #third variable:Count of unique values of zz0012 column in INIT table:Unique categories purchased
        no_of_unique_categories_purchased = initdf.groupby("zzsoldto")["zz0012"].nunique()
        customdata["no_of_unique_categories_purchased"] = no_of_unique_categories_purchased.values
        #fourth variable:Count of purchases where  zz0012 = AC in INIT Table:No of Acs purchased
        ac_purchased = initdf[initdf["zz0012"] == "AC"].groupby("zzsoldto").size().reset_index(name = "no_of_ac_purchased")
        ac_purchased.rename(columns={"zzsoldto":"customer"},inplace = True)
        customdata = customdata.merge(ac_purchased,on = "customer",how ="left").fillna(0)
        customdata["no_of_ac_purchased"] = customdata["no_of_ac_purchased"].astype(int)

        wm_purchased = initdf[initdf["zz0012"] == "WM"].groupby("zzsoldto").size().reset_index(name = "no_of_wm_purchased")
        wm_purchased.rename(columns={"zzsoldto":"customer"},inplace = True)
        customdata = customdata.merge(wm_purchased,on = "customer",how ="left").fillna(0)
        customdata["no_of_wm_purchased"] = customdata["no_of_wm_purchased"].astype(int)

        mw_purchased = initdf[initdf["zz0012"] == "MW"].groupby("zzsoldto").size().reset_index(name = "no_of_mw_purchased")
        mw_purchased.rename(columns={"zzsoldto":"customer"},inplace = True)
        customdata = customdata.merge(mw_purchased,on = "customer",how ="left").fillna(0)
        customdata["no_of_mw_purchased"] = customdata["no_of_mw_purchased"].astype(int)
        import pandas as pd

        # Assuming init_copy is already defined

        # Group by 'zzsoldto' and sort within each group
        init_copy = init_copy.sort_values(by=['zzsoldto', 'zzpurchaseDate'])

        # Calculate the difference of 'cat_num' within each group
        init_copy['cat_num_diff'] = init_copy.groupby('zzsoldto')['cat_num'].diff()

        # Calculate the difference in days within each group where 'cat_num_diff' is 0
        init_copy['time_diff'] = init_copy.groupby('zzsoldto')['zzpurchaseDate'].diff().dt.days.where(init_copy['cat_num_diff'] == 0)

        # Calculate the average time difference for each 'zzsoldto'
        avg_time_diff_dict = init_copy.groupby('zzsoldto')['time_diff'].mean().to_dict()

        # Map the average time difference to the customdata DataFrame
        customdata['no_days_btw_same_cat_purchase_avg'] = customdata['customer'].map(avg_time_diff_dict)

        init_copy = init_copy.sort_values(by=['zzsoldto', 'zzpurchaseDate'])
        init_copy['cat_num_diff'] = init_copy.groupby('zzsoldto')['cat_num'].diff()
        init_copy['time_diff'] = init_copy.groupby('zzsoldto')['zzpurchaseDate'].diff().dt.days.where(init_copy['cat_num_diff'] == 0)
        max_time_diff_dict = init_copy.groupby('zzsoldto')['time_diff'].max().to_dict()
        customdata['no_days_btw_same_cat_purchase_max'] = customdata['customer'].map(max_time_diff_dict)
        init_copy = init_copy.sort_values(by=['zzsoldto', 'zzpurchaseDate'])
        init_copy['cat_num_diff'] = init_copy.groupby('zzsoldto')['cat_num'].diff()
        init_copy['time_diff'] = init_copy.groupby('zzsoldto')['zzpurchaseDate'].diff().dt.days.where(init_copy['cat_num_diff'] == 0)
        avg_time_diff_dict = init_copy.groupby('zzsoldto')['time_diff'].min().to_dict()
        customdata['no_days_btw_same_cat_purchase_min'] = customdata['customer'].map(avg_time_diff_dict)
        init_copy = init_copy.sort_values(by=['zzsoldto', 'zzpurchaseDate'])
        init_copy['cat_num_diff'] = init_copy.groupby('zzsoldto')['cat_num'].diff()
        init_copy['time_diff'] = init_copy.groupby('zzsoldto')['zzpurchaseDate'].diff().dt.days.where(init_copy['cat_num_diff'] != 0)
        avg_time_diff_dict = init_copy.groupby('zzsoldto')['time_diff'].mean().to_dict()
        customdata['no_days_btw_cross_cat_purchase_avg'] = customdata['customer'].map(avg_time_diff_dict)
        init_copy = init_copy.sort_values(by=['zzsoldto', 'zzpurchaseDate'])
        init_copy['cat_num_diff'] = init_copy.groupby('zzsoldto')['cat_num'].diff()
        init_copy['time_diff'] = init_copy.groupby('zzsoldto')['zzpurchaseDate'].diff().dt.days.where(init_copy['cat_num_diff'] != 0)
        max_time_diff_dict = init_copy.groupby('zzsoldto')['time_diff'].max().to_dict()
        customdata['no_days_btw_cross_cat_purchase_max'] = customdata['customer'].map(max_time_diff_dict)
        init_copy = init_copy.sort_values(by=['zzsoldto', 'zzpurchaseDate'])
        init_copy['cat_num_diff'] = init_copy.groupby('zzsoldto')['cat_num'].diff()
        init_copy['time_diff'] = init_copy.groupby('zzsoldto')['zzpurchaseDate'].diff().dt.days.where(init_copy['cat_num_diff'] != 0)
        min_time_diff_dict = init_copy.groupby('zzsoldto')['time_diff'].min().to_dict()
        customdata['no_days_btw_cross_cat_purchase_min'] = customdata['customer'].map(min_time_diff_dict)

        wm_purchases = init_copy[init_copy["zz0012"] == "WM"]
        latest_wm_purchase = wm_purchases.groupby("zzsoldto")["zzpurchaseDate"].max().reset_index()
        latest_wm_purchase.rename(columns={"zzsoldto":"customer"},inplace = True)
        latest_wm_purchase.rename(columns={"zzpurchaseDate":"latest_wm_purchase_date"},inplace = True)
        latest_wm_purchase["age_of_last_wm_bought"] = (datetime.now() - latest_wm_purchase["latest_wm_purchase_date"]).dt.days
        customdata = customdata.merge(latest_wm_purchase[["customer","age_of_last_wm_bought"]],on = "customer",how = "left")
        #thirteenth variable: Age of machine bought-DW
        dw_purchases = init_copy[init_copy["zz0012"] == "DW"]
        latest_dw_purchase = dw_purchases.groupby("zzsoldto")["zzpurchaseDate"].max().reset_index()
        latest_dw_purchase.rename(columns={"zzsoldto":"customer"},inplace = True)
        latest_dw_purchase.rename(columns={"zzpurchaseDate":"latest_dw_purchase_date"},inplace = True)
        latest_dw_purchase["age_of_last_dw_bought"] = (datetime.now() - latest_dw_purchase["latest_dw_purchase_date"]).dt.days
        customdata = customdata.merge(latest_dw_purchase[["customer","age_of_last_dw_bought"]],on = "customer",how = "left")
        #fourteenth variable: Age of machine bought-AC
        ac_purchases = init_copy[init_copy["zz0012"] == "AC"]
        latest_ac_purchase = ac_purchases.groupby("zzsoldto")["zzpurchaseDate"].max().reset_index()
        latest_ac_purchase.rename(columns={"zzsoldto":"customer"},inplace = True)
        latest_ac_purchase.rename(columns={"zzpurchaseDate":"latest_ac_purchase_date"},inplace = True)
        latest_ac_purchase["age_of_last_ac_bought"] = (datetime.now() - latest_ac_purchase["latest_ac_purchase_date"]).dt.days
        customdata = customdata.merge(latest_ac_purchase[["customer","age_of_last_ac_bought"]],on = "customer",how = "left")

        # Calling data
        allcalldf1 = dataframes.get('CrmAllCall')
        allcalldf1=allcalldf1.dropna(subset=['CustomerCode'])
        allcalldf1[allcalldf1['CustomerCode'].isnull()]
        allcalldf1['CustomerCode'] = allcalldf1['CustomerCode'].astype(float).astype(int)
        allcalldf=allcalldf1.copy()
        allcalldf.rename(columns={'customer':'CustomerCode'},inplace=True)

        allcalldf['CustomerCode']=allcalldf['CustomerCode'].map(str)
        closed_status_counts = allcalldf[allcalldf["Status"] == "Closed"].groupby("CustomerCode").size().reset_index()
        closed_status_counts.columns = ["customer","closed_status_count"]
        customdata = pd.merge(customdata,closed_status_counts,on = "customer",how = "left")

        cancelled_status_counts = allcalldf[allcalldf["Status"] == "Cancelled"].groupby("CustomerCode").size().reset_index()
        cancelled_status_counts.columns = ["customer","cancelled_status_count"]
        customdata = pd.merge(customdata,cancelled_status_counts,on = "customer",how = "left")
        customer_closed_status_count = allcalldf[(allcalldf["Origin"]=="Customer")&(allcalldf["Status"]=="Closed")].groupby("CustomerCode").size().reset_index()
        customer_closed_status_count.columns = ["customer","customer_closed_status_count"]
        customdata = pd.merge(customdata,customer_closed_status_count,on = "customer",how = "left")
        customer_cancelled_status_count = allcalldf[(allcalldf["Origin"]=="Customer")&(allcalldf["Status"]=="Cancelled")].groupby("CustomerCode").size().reset_index()
        customer_cancelled_status_count.columns = ["customer","customer_cancelled_status_count"]
        customdata = pd.merge(customdata,customer_cancelled_status_count,on = "customer",how = "left")
        allcall_copy = allcalldf.copy()
        allcall_copy["ClosedOn"] = pd.to_datetime(allcall_copy["ClosedOn"],errors='coerce')
        allcall_copy["SoftClosureDate"] = pd.to_datetime(allcall_copy["SoftClosureDate"],errors='coerce')
        allcall_copy["PostingDate"] = pd.to_datetime(allcall_copy["PostingDate"],errors='coerce')
        allcall_copy["NegativeReponseRemarksDate"] = pd.to_datetime(allcall_copy["NegativeReponseRemarksDate"],errors='coerce')
        closed_rows = allcall_copy[allcall_copy["Status"] == "Closed"]
        latest_closed_date = closed_rows.groupby("CustomerCode")["ClosedOn"].max().reset_index()
        latest_closed_date["days_since_last_closed_status"] = (datetime.now() - latest_closed_date["ClosedOn"]).dt.days
        latest_closed_date.rename(columns={"CustomerCode":"customer"},inplace = True)
        customdata = pd.merge(customdata,latest_closed_date[["customer","days_since_last_closed_status"]],on = "customer",how = "left")

        negative_remarks_count = allcalldf[(allcalldf["NegativeResponseRemarks"].notnull())&(allcalldf["Status"]=="Closed")].groupby("CustomerCode").size().reset_index()
        negative_remarks_count.columns = ["customer","negative_remarks_count"]
        customdata = pd.merge(customdata,negative_remarks_count,on = "customer",how = "left")

        negremarks_not_null = allcall_copy[allcall_copy["NegativeResponseRemarks"].notnull()]
        latest_negremark_date = negremarks_not_null.groupby("CustomerCode")["NegativeReponseRemarksDate"].max().reset_index()
        latest_negremark_date.columns = ["customer","latest_negremark_date"]
        customdata = pd.merge(customdata,latest_negremark_date,on = "customer",how = "left")
        allcall_copy["soft_post_days_diff"] = (allcall_copy["SoftClosureDate"] - allcall_copy["PostingDate"]).dt.days
        avg_diff_per_cust_prodse = allcall_copy.groupby(["CustomerCode","ProductSerial"])["soft_post_days_diff"].mean().reset_index()
        avg_diff_per_cust = avg_diff_per_cust_prodse.groupby("CustomerCode")["soft_post_days_diff"].mean().reset_index()
        avg_diff_per_cust.rename(columns={"CustomerCode":"customer"},inplace = True)
        customdata = pd.merge(customdata,avg_diff_per_cust,on = "customer",how = "left")
        allcalldf['MachineStatus'].unique()
        allcalldf["Esclationlevel"].fillna(0,inplace = True)
        allcalldf["npSrating"].fillna(0,inplace = True)
        allcallescalationhandle = allcalldf[~allcalldf["Esclationlevel"].isin([98.0,99.0])]
        max_escalation  = allcallescalationhandle.groupby("CustomerCode")["Esclationlevel"].max().reset_index()
        max_escalation.rename(columns={"CustomerCode":"customer"},inplace = True)
        max_escalation.rename(columns={"Esclationlevel":"max_escalation"},inplace = True)
        customdata = pd.merge(customdata,max_escalation,on = "customer",how = "left")
        socialmediacount = allcalldf[allcalldf["Esclationlevel"] == 99]
        socialmediacount_result = socialmediacount.groupby("CustomerCode").size().reset_index(name= "count_of_99")
        socialmediacount_result.rename(columns={"CustomerCode":"customer"},inplace = True)
        customdata = pd.merge(customdata,socialmediacount_result,on = "customer",how = "left")
        seniormanagementcount = allcalldf[allcalldf["Esclationlevel"] == 98]
        seniormanagementcount_result = seniormanagementcount.groupby("CustomerCode").size().reset_index(name= "count_of_98")
        seniormanagementcount_result.rename(columns={"CustomerCode":"customer"},inplace = True)
        customdata = pd.merge(customdata,seniormanagementcount_result,on = "customer",how = "left")
        avg_nps  = allcalldf.groupby("CustomerCode")["npSrating"].mean().reset_index()
        avg_nps.rename(columns={"CustomerCode":"customer"},inplace = True)
        avg_nps.rename(columns={"npSrating":"avg_nps"},inplace = True)
        customdata = pd.merge(customdata,avg_nps,on = "customer",how = "left")
        customer_closed_email_count = allcalldf[(allcalldf["Origin"]=="Customer")&(allcalldf["Status"]=="Closed")&(allcalldf["Medium"]=="Email")].groupby("CustomerCode").size().reset_index()
        customer_closed_email_count.columns = ["customer","customer_closed_email_count"]
        customdata = pd.merge(customdata,customer_closed_email_count,on = "customer",how = "left")
        customer_closed_phone_count = allcalldf[(allcalldf["Origin"]=="Customer")&(allcalldf["Status"]=="Closed")&(allcalldf["Medium"]=="Phone")].groupby("CustomerCode").size().reset_index()
        customer_closed_phone_count.columns = ["customer","customer_closed_phone_count"]
        customdata = pd.merge(customdata,customer_closed_phone_count,on = "customer",how = "left")
        customer_closed_wp_count = allcalldf[(allcalldf["Origin"]=="Customer")&(allcalldf["Status"]=="Closed")&(allcalldf["Medium"]=="WhatsApp")].groupby("CustomerCode").size().reset_index()
        customer_closed_wp_count.columns = ["customer","customer_closed_wp_count"]
        customdata = pd.merge(customdata,customer_closed_wp_count,on = "customer",how = "left")
        closed_minor_og_count = allcalldf[(allcalldf["ServiceType"]=="Minor")&(allcalldf["Status"]=="Closed")&(allcalldf["MachineStatus"]=="OG")].groupby("CustomerCode").size().reset_index()
        closed_minor_og_count.columns = ["customer","closed_minor_og_count"]
        customdata = pd.merge(customdata,closed_minor_og_count,on = "customer",how = "left")
        closed_minor_amc_count = allcalldf[(allcalldf["ServiceType"]=="Minor")&(allcalldf["Status"]=="Closed")&(allcalldf["MachineStatus"]=="AMC")].groupby("CustomerCode").size().reset_index()
        closed_minor_amc_count.columns = ["customer","closed_minor_amc_count"]
        customdata = pd.merge(customdata,closed_minor_amc_count,on = "customer",how = "left")
        closed_minor_sw_count = allcalldf[(allcalldf["ServiceType"]=="Minor")&(allcalldf["Status"]=="Closed")&(allcalldf["MachineStatus"]=="SW")].groupby("CustomerCode").size().reset_index()
        closed_minor_sw_count.columns = ["customer","closed_minor_sw_count"]
        customdata = pd.merge(customdata,closed_minor_sw_count,on = "customer",how = "left")
        closed_minor_ew_count = allcalldf[(allcalldf["ServiceType"]=="Minor")&(allcalldf["Status"]=="Closed")&(allcalldf["MachineStatus"]=="EW")].groupby("CustomerCode").size().reset_index()
        closed_minor_ew_count.columns = ["customer","closed_minor_ew_count"]
        customdata = pd.merge(customdata,closed_minor_ew_count,on = "customer",how = "left")
        closed_major_sw_count = allcalldf[(allcalldf["ServiceType"]=="Major")&(allcalldf["Status"]=="Closed")&(allcalldf["MachineStatus"]=="SW")].groupby("CustomerCode").size().reset_index()
        closed_major_sw_count.columns = ["customer","closed_major_sw_count"]
        customdata = pd.merge(customdata,closed_major_sw_count,on = "customer",how = "left")
        closed_major_amc_count = allcalldf[(allcalldf["ServiceType"]=="Major")&(allcalldf["Status"]=="Closed")&(allcalldf["MachineStatus"]=="AMC")].groupby("CustomerCode").size().reset_index()
        closed_major_amc_count.columns = ["customer","closed_major_amc_count"]
        customdata = pd.merge(customdata,closed_major_amc_count,on = "customer",how = "left")
        closed_major_ew_count = allcalldf[(allcalldf["ServiceType"]=="Major")&(allcalldf["Status"]=="Closed")&(allcalldf["MachineStatus"]=="EW")].groupby("CustomerCode").size().reset_index()
        closed_major_ew_count.columns = ["customer","closed_major_ew_count"]
        customdata = pd.merge(customdata,closed_major_ew_count,on = "customer",how = "left")
        closed_major_og_count = allcalldf[(allcalldf["ServiceType"]=="Major")&(allcalldf["Status"]=="Closed")&(allcalldf["MachineStatus"]=="OG")].groupby("CustomerCode").size().reset_index()
        closed_major_og_count.columns = ["customer","closed_major_og_count"]
        customdata = pd.merge(customdata,closed_major_og_count,on = "customer",how = "left")
        unique_medium_count = allcalldf.groupby("CustomerCode")["Medium"].nunique().reset_index()
        unique_medium_count.columns = ["customer","unique_medium_count"]
        customdata = pd.merge(customdata,unique_medium_count,on = "customer",how = "left")
        allcall_copy["npSrating"].fillna(0,inplace = True)
        allcall_copy['ServiceType'].unique()
        allcall_copy_sorted_date = allcall_copy.groupby("CustomerCode")["ClosedOn"].max().reset_index()
        allcall_copy_sorted_date = pd.merge(allcall_copy_sorted_date,allcall_copy[["CustomerCode","ClosedOn","npSrating"]].drop_duplicates(subset = ["CustomerCode","ClosedOn"],keep = "first"),on = ["CustomerCode","ClosedOn"],how = "left")
        allcall_copy_sorted_date.rename(columns={"CustomerCode":"customer"},inplace = True)
        allcall_copy_sorted_date.rename(columns={"npSrating":"latest_nps_rating"},inplace = True)
        customdata = pd.merge(customdata,allcall_copy_sorted_date,on = "customer",how = "left")
        allcall_copy["close_nr_days_diff"] = (allcall_copy["ClosedOn"] - allcall_copy["NegativeReponseRemarksDate"]).dt.days
        avg_diff_per_cust_prodser = allcall_copy.groupby(["CustomerCode","ProductSerial"])["close_nr_days_diff"].mean().reset_index()
        avg_diff_per_cust0 = avg_diff_per_cust_prodser.groupby("CustomerCode")["close_nr_days_diff"].mean().reset_index()
        avg_diff_per_cust0.rename(columns={"CustomerCode":"customer"},inplace = True)
        customdata = pd.merge(customdata,avg_diff_per_cust0,on = "customer",how = "left")
        #machine_status_sw = allcall_copy[allcall_copy["MachineStatus"] == "SW"]
        #machine_status_sw["AVG_TAT_SW"] = (machine_status_sw["ClosedOn"] - machine_status_sw["PostingDate"] ).dt.days

        # Create a copy of the filtered DataFrame
        machine_status_sw = allcall_copy[allcall_copy["MachineStatus"] == "SW"].copy()
        machine_status_sw["ClosedOn"] = pd.to_datetime(machine_status_sw["ClosedOn"])
        machine_status_sw["PostingDate"] = pd.to_datetime(machine_status_sw["PostingDate"])
        machine_status_sw["AVG_TAT_SW"] = (machine_status_sw["ClosedOn"] - machine_status_sw["PostingDate"]).dt.days
        sw_closed_post_diff_prod = machine_status_sw.groupby(["CustomerCode","ProductSerial"])["AVG_TAT_SW"].mean().reset_index()
        sw_closed_post_diff_cust = sw_closed_post_diff_prod.groupby("CustomerCode")["AVG_TAT_SW"].mean().reset_index()
        sw_closed_post_diff_cust.rename(columns={"CustomerCode":"customer"},inplace = True)
        customdata = pd.merge(customdata,sw_closed_post_diff_cust,on = "customer",how = "left")
        machine_status_amc = allcall_copy[allcall_copy["MachineStatus"] == "AMC"].copy()
        machine_status_amc["AVG_TAT_AMC"] = (machine_status_amc["ClosedOn"] - machine_status_amc["PostingDate"] ).dt.days
        aMC_closed_post_diff_prod = machine_status_amc.groupby(["CustomerCode","ProductSerial"])["AVG_TAT_AMC"].mean().reset_index()
        aMC_closed_post_diff_cust = aMC_closed_post_diff_prod.groupby("CustomerCode")["AVG_TAT_AMC"].mean().reset_index()
        aMC_closed_post_diff_cust.rename(columns={"CustomerCode":"customer"},inplace = True)
        customdata = pd.merge(customdata,aMC_closed_post_diff_cust,on = "customer",how = "left")
        machine_status_ew = allcall_copy[allcall_copy["MachineStatus"] == "EW"].copy()
        machine_status_ew["AVG_TAT_EW"] = (machine_status_ew["ClosedOn"] - machine_status_ew["PostingDate"] ).dt.days
        ew_closed_post_diff_prod = machine_status_ew.groupby(["CustomerCode","ProductSerial"])["AVG_TAT_EW"].mean().reset_index()
        ew_closed_post_diff_cust = ew_closed_post_diff_prod.groupby("CustomerCode")["AVG_TAT_EW"].mean().reset_index()
        ew_closed_post_diff_cust.rename(columns={"CustomerCode":"customer"},inplace = True)
        customdata = pd.merge(customdata,ew_closed_post_diff_cust,on = "customer",how = "left")
        machine_status_og = allcall_copy[allcall_copy["MachineStatus"] == "OG"].copy()
        machine_status_og["close_post_diff"] = (machine_status_og["ClosedOn"] - machine_status_og["PostingDate"] ).dt.days
        av_closed_post_diff_prod = machine_status_og.groupby(["CustomerCode","ProductSerial"])["close_post_diff"].mean().reset_index()
        av_closed_post_diff_cust = av_closed_post_diff_prod.groupby("CustomerCode")["close_post_diff"].mean().reset_index()
        av_closed_post_diff_cust.rename(columns={"CustomerCode":"customer"},inplace = True)
        customdata = pd.merge(customdata,av_closed_post_diff_cust,on = "customer",how = "left")
        customdata.rename(columns={"close_post_diff":"AVG_TAT_OG"},inplace = True)
        max_nps = allcalldf.groupby("CustomerCode")["npSrating"].max().reset_index()
        max_nps.rename(columns={"CustomerCode":"customer"},inplace = True)
        max_nps.rename(columns={"npSrating":"max_nps"},inplace = True)
        customdata = pd.merge(customdata,max_nps,on = "customer",how = "left")
        customdata["latest_max_nps_diff"] = customdata["latest_nps_rating"] - customdata["max_nps"]
        #customdata.to_csv('50_features.csv',index=False)

        #AMC DATA
        amcdf=dataframes.get('CrmAMCContracts')
        amcdf['AMCPostingDate']=pd.to_datetime(amcdf['AMCPostingDate'],errors='coerce')
        amcdf['CustomerCode']=amcdf['CustomerCode'].map(str)
        amcdf['SrNo']=amcdf['SrNo'].map(str)
        amcdf['IcrNo']=amcdf['IcrNo'].map(str)
        ser_superwarranty_count = amcdf[amcdf["AmcType"]=="Super Warranty"].groupby("CustomerCode")["SrNo"].nunique().reset_index()
        ser_superwarranty_count.rename(columns={"CustomerCode":"customer"},inplace = True)
        ser_superwarranty_count.rename(columns={"SrNo":"unique_prod_ser_superwarranty"},inplace = True)
        customdata = pd.merge(customdata,ser_superwarranty_count,on = "customer",how = "left")
        ser_exwarranty_count = amcdf[amcdf["AmcType"]=="Extended Warranty"].groupby("CustomerCode")["SrNo"].nunique().reset_index()
        ser_exwarranty_count.rename(columns={"CustomerCode":"customer"},inplace = True)
        ser_exwarranty_count.rename(columns={"SrNo":"unique_prod_ser_exwarranty"},inplace = True)
        customdata = pd.merge(customdata,ser_exwarranty_count,on = "customer",how = "left")
        ser_labamc_count = amcdf[amcdf["AmcType"]=="Labour AMC"].groupby("CustomerCode")["SrNo"].nunique().reset_index()
        ser_labamc_count.rename(columns={"CustomerCode":"customer"},inplace = True)
        ser_labamc_count.rename(columns={"SrNo":"unique_prod_ser_labamc"},inplace = True)
        customdata = pd.merge(customdata,ser_labamc_count,on = "customer",how = "left")
        ser_mot_count = amcdf[amcdf["AmcType"]=="Motor Warranty"].groupby("CustomerCode")["SrNo"].nunique().reset_index()
        ser_mot_count.rename(columns={"CustomerCode":"customer"},inplace = True)
        ser_mot_count.rename(columns={"SrNo":"unique_prod_ser_mot"},inplace = True)
        customdata = pd.merge(customdata,ser_mot_count,on = "customer",how = "left")
        ser_amc_count = amcdf[amcdf["AmcType"]=="AMC"].groupby("CustomerCode")["SrNo"].nunique().reset_index()
        ser_amc_count.rename(columns={"CustomerCode":"customer"},inplace = True)
        ser_amc_count.rename(columns={"SrNo":"unique_prod_ser_amc"},inplace = True)
        customdata = pd.merge(customdata,ser_amc_count,on = "customer",how = "left")
        unique_icrno_count = amcdf.groupby("CustomerCode")["IcrNo"].nunique().reset_index()
        unique_icrno_count.rename(columns={"IcrNo":"unique_icrno_count"},inplace = True)
        unique_icrno_count.rename(columns={"CustomerCode":"customer"},inplace = True)
        customdata = pd.merge(customdata,unique_icrno_count,on = "customer",how = "left")
        amcdf_copy = amcdf.copy()
        amcdf_copy["ContEndDat"] = pd.to_datetime(amcdf_copy["ContEndDat"],errors = "coerce")
        amcdf_copy["ContStrtDat"] = pd.to_datetime(amcdf_copy["ContStrtDat"],errors = "coerce")
        latest_cont_end_date = amcdf_copy.groupby("CustomerCode")["ContEndDat"].max().reset_index()
        latest_cont_end_date.rename(columns={"CustomerCode":"customer"},inplace = True)
        latest_cont_end_date.rename(columns={"ContEndDat":"latest_cont_end_date"},inplace = True)
        customdata = pd.merge(customdata,latest_cont_end_date,on = "customer",how = "left")
        customdata["days_left_for_current_amc_to_be_ended"] = (datetime.now() - customdata["latest_cont_end_date"] ).dt.days
        sum_amc_price = amcdf.groupby("CustomerCode")["Price"].sum().reset_index()
        sum_amc_price.rename(columns={"CustomerCode":"customer"},inplace = True)
        sum_amc_price.rename(columns={"Price":"sum_amc_price"},inplace = True)
        customdata = pd.merge(customdata,sum_amc_price,on = "customer",how = "left")
        customdata["avg_price_amc"] = customdata["sum_amc_price"]/customdata["unique_icrno_count"]
        first_filter = init_copy[init_copy["zz0010"] != "OG"]
        zzenddate_max = first_filter.groupby("zzsoldto")["zzendDate"].max().reset_index()
        zzenddate_max.rename(columns={"zzsoldto":"customer"},inplace = True)
        zzenddate_max.rename(columns={"zzendDate":"zzenddate_max"},inplace = True)
        customdata = pd.merge(customdata,zzenddate_max,on = "customer",how = "left")
        con_end_date_max = amcdf_copy.groupby("CustomerCode")["ContEndDat"].max().reset_index()
        con_end_date_max.rename(columns={"CustomerCode":"customer"},inplace = True)
        con_end_date_max.rename(columns={"ContEndDat":"con_end_date_max"},inplace = True)
        customdata = pd.merge(customdata,con_end_date_max,on = "customer",how = "left")
        customdata["latest_warranty_date"] = customdata[["zzenddate_max","con_end_date_max"]].max(axis = 1)
        customdata["days_out_of_warranty"] = (datetime.now() - customdata["latest_warranty_date"] ).dt.days

        #SAPZSPU DATA
        file_path4=r"Data\Bangalore\Sapzspu_bangalore.csv"
        spudf=pd.read_csv(file_path4,nrows=100)
        spudf1=spudf.rename(columns={'CustomerCode':'CustomerC','ProductSerial':'Serial','MATERIAL':'ItemNo','COGS':'NetValue','MACHSTAT':'ItemCategory'})
        spudf1['CustomerC']=spudf1['CustomerC'].map(str)
        spudf1['Serial']=spudf1['Serial'].map(str)
        spudf_copy = spudf1.copy()
        spudf_copy["CallBookDate"] = pd.to_datetime(spudf_copy["CallBookDate"],errors = "coerce")
        unique_item_count_spu_2 = spudf_copy.groupby(["CustomerC","Serial","ItemNo"])["CallBookDate"].nunique().reset_index()
        unique_item_count_spu_2=unique_item_count_spu_2[unique_item_count_spu_2['Serial']!='nan']
        further_filter_1 = unique_item_count_spu_2[unique_item_count_spu_2["CallBookDate"]>1]
        filter_count_customer_1 = further_filter_1.groupby("CustomerC")["ItemNo"].nunique().reset_index()
        filter_count_customer_1.rename(columns={"CustomerC":"customer"},inplace = True)
        filter_count_customer_1.rename(columns={"ItemNo":"repeat_parts_for_customer"},inplace = True)
        customdata = pd.merge(customdata,filter_count_customer_1,on = "customer",how = "left")
        # Crmallcallitem
        file_path5=r"Data\Bangalore\crmallcallitem.csv"
        allcall_item_df=pd.read_csv(file_path5,nrows=100)
        allcall_item_df['ItemCode']=allcall_item_df['Accessories'].combine_first(allcall_item_df['Additive'])
        allcall_item_df.rename(columns={'SOLD_TO_PARTY':'CustomerCode'},inplace=True)
        allcall_item_df.rename(columns={'ZZSERIAL_NUMB':'ProductSerial'},inplace=True)
        allcall_item_df.rename(columns={'POSTING_DATE':'PostingDate'},inplace=True)
        allcall_item_df.rename(columns={'Quantity':'Qty'},inplace=True)
        allcall_item_df['CustomerCode']=allcall_item_df['CustomerCode'].map(str)
        allcall_item_df['ProductSerial']=allcall_item_df['ProductSerial'].map(str)
        allcall_item_df[allcall_item_df['itemtype']=='ZADD']
        allcall_item_copy = allcall_item_df.copy()
        allcall_item_df[['Accessories','Additive','ItemCode']]
        allcall_item_copy["PostingDate"] = pd.to_datetime(allcall_item_copy["PostingDate"],errors = "coerce")
        filtered_allcallitem_2 = allcall_item_copy[allcall_item_copy["itemtype"] == "ZACC"]
        latest_allcallitem_date_zacc = filtered_allcallitem_2.groupby("CustomerCode")["PostingDate"].max().reset_index()
        latest_allcallitem_date_zacc.rename(columns={"CustomerCode":"customer"},inplace = True)
        latest_allcallitem_date_zacc.rename(columns={"PostingDate":"latest_allcallitem_date_zacc"},inplace = True)
        customdata = pd.merge(customdata,latest_allcallitem_date_zacc,on = "customer",how = "left")
        customdata["days_since_last_purchase_zacc"] = (datetime.now() - customdata["latest_allcallitem_date_zacc"] ).dt.days
        filtered_allcallitem_1 = allcall_item_copy[allcall_item_copy["itemtype"] == "ZADD"]
        latest_allcallitem_date = filtered_allcallitem_1.groupby("CustomerCode")["PostingDate"].max().reset_index()
        latest_allcallitem_date.rename(columns={"CustomerCode":"customer"},inplace = True)
        latest_allcallitem_date.rename(columns={"PostingDate":"latest_allcallitem_date"},inplace = True)
        customdata = pd.merge(customdata,latest_allcallitem_date,on = "customer",how = "left")
        customdata["days_since_last_purchase_zadd"] = (datetime.now() - customdata["latest_allcallitem_date"] ).dt.days
        filtered_data_for_qty_2 = allcall_item_copy[allcall_item_copy["itemtype"] == "ZACC"]
        latest_row_for_cust_2 = filtered_data_for_qty_2.loc[filtered_data_for_qty_2.groupby("CustomerCode")["PostingDate"].idxmax()]
        latest_row_for_cust_2.rename(columns={"CustomerCode":"customer"},inplace = True)
        latest_row_for_cust_2.rename(columns={"Qty":"zacc_qty"},inplace = True)
        customdata = pd.merge(customdata,latest_row_for_cust_2[["customer","zacc_qty"]],on = "customer",how = "left")
        filtered_data_for_qty_1 = allcall_item_copy[allcall_item_copy["itemtype"] == "ZADD"]
        latest_row_for_cust_1 = filtered_data_for_qty_1.loc[filtered_data_for_qty_1.groupby("CustomerCode")["PostingDate"].idxmax()]
        latest_row_for_cust_1.rename(columns={"CustomerCode":"customer"},inplace = True)
        latest_row_for_cust_1.rename(columns={"Qty":"zadd_qty"},inplace = True)
        customdata = pd.merge(customdata,latest_row_for_cust_1[["customer","zadd_qty"]],on = "customer",how = "left")
        amc_constraint_1 = amcdf_copy[["CustomerCode","SrNo","ContStrtDat","ContEndDat","Price"]]
        amc_constraint_1=amc_constraint_1.rename(columns={"CustomerCode":"customer"})
        amc_constraint_1=amc_constraint_1.rename(columns={"SrNo":"Serial"})
        spu_constraint_1 = spudf_copy[["CustomerC","Serial","ItemCategory","CallBookDate","NetValue"]]
        spu_constraint_1=spu_constraint_1.rename(columns={"CustomerC":"customer"})
        spu_constraint_1 = pd.merge(spu_constraint_1,amc_constraint_1,on = ["customer","Serial"],how = "left" )
        cleaned_constraint_spu1 = spu_constraint_1.dropna(subset=["ContStrtDat","ContEndDat"])
        amc_constraint = amcdf_copy[["CustomerCode","SrNo","ContStrtDat","ContEndDat"]]
        result_1 = amc_constraint.groupby(["CustomerCode","SrNo"]).agg({"ContStrtDat":"min","ContEndDat":"max"}).reset_index()
        result_1.rename(columns={"ContStrtDat":"minContStrtDat"},inplace = True)
        result_1.rename(columns={"CustomerCode":"customer"},inplace = True)
        result_1.rename(columns={"ContEndDat":"maxContEndDat"},inplace = True)
        spu_constraint = spudf_copy[["CustomerC","Serial","ItemNo","CallBookDate"]]
        spu_constraint=spu_constraint.rename(columns={"CustomerC":"customer"})
        spu_constraint=spu_constraint.rename(columns={"Serial":"SrNo"})
        merged_constraint = pd.merge(spu_constraint,result_1,on=["customer","SrNo"],how = "left")
        cleaned_constraint = merged_constraint.dropna(subset=["minContStrtDat","maxContEndDat"])
        filteres_constraint = cleaned_constraint[(cleaned_constraint["CallBookDate"]>cleaned_constraint["minContStrtDat"])&(cleaned_constraint["CallBookDate"]<cleaned_constraint["maxContEndDat"])]
        unique_item_filterconstraint = filteres_constraint.groupby(["customer","SrNo","ItemNo"])["CallBookDate"].nunique().reset_index()
        further_filter_2 = unique_item_filterconstraint[unique_item_filterconstraint["CallBookDate"]>1]
        filter_count_customer_2 = further_filter_2.groupby("customer")["ItemNo"].nunique().reset_index()
        customdata = pd.merge(customdata,filter_count_customer_2,on = "customer",how = "left")
        customdata = pd.merge(customdata,filter_count_customer_2,on = "customer",how = "left")
        cust_ser_min_contr_amc = amcdf_copy.groupby(["CustomerCode","SrNo"])["ContStrtDat"].min().reset_index()
        cust_ser_min_contr_amc.rename(columns={"CustomerCode":"customer"},inplace = True)
        cust_ser_min_contr_amc.rename(columns={"SrNo":"Serial"},inplace = True)
        cust_ser_min_callb_spu = spudf_copy.groupby(["CustomerC","Serial"])["CallBookDate"].min().reset_index()
        cust_ser_min_callb_spu.rename(columns={"CustomerC":"customer"},inplace = True)
        cust_ser_min_contr_amc = pd.merge(cust_ser_min_contr_amc,cust_ser_min_callb_spu,on = ["customer","Serial"],how = "left" )
        cust_ser_min_contr_amc["diff_constrt_callbook"] = (cust_ser_min_contr_amc["ContStrtDat"]-cust_ser_min_contr_amc["CallBookDate"]).dt.days
        av_dur_diff_constrt_callbook = cust_ser_min_contr_amc.groupby("customer")["diff_constrt_callbook"].mean().reset_index()
        av_dur_diff_constrt_callbook.rename(columns={"diff_constrt_callbook":"Days_between_AMC_start_data_and_1st_consumption_date"},inplace = True)
        customdata = pd.merge(customdata,av_dur_diff_constrt_callbook,on = "customer",how = "left")
        init_copy.rename(columns={"zzr3serNo":"ProductSerial"},inplace = True)
        init_copy.rename(columns={"zzsoldto":"customer"},inplace = True)
        allcall_item_copy.rename(columns={"CustomerCode":"customer"},inplace = True)
        all_call_init_merge1 = pd.merge(allcall_item_copy,init_copy[["customer","ProductSerial","zzpurchaseDate","cat_num"]],on = ["customer","ProductSerial"],how = "left")
        all_call_init_merge_grouped = all_call_init_merge1.groupby(["customer","ProductSerial"],as_index = False,sort = False).apply(lambda x:x).reset_index(drop = True)
        filtered_all_call_init_merge_grouped = all_call_init_merge_grouped[all_call_init_merge_grouped["zzpurchaseDate"] != all_call_init_merge_grouped["PostingDate"]]
        filtered_all_call_init_merge_grouped.reset_index(drop = True,inplace = True)
        essential_filtered_all_call_init_merge_grouped = filtered_all_call_init_merge_grouped[filtered_all_call_init_merge_grouped["itemtype"] == "ZADD"]
        number_of_products_for_which_essentials_purchased = essential_filtered_all_call_init_merge_grouped.groupby("customer")["cat_num"].nunique().reset_index()
        number_of_products_for_which_essentials_purchased.rename(columns={"cat_num":"number_of_productcats_for_which_essentials_purchased"},inplace = True)
        customdata = pd.merge(customdata,number_of_products_for_which_essentials_purchased,on = "customer",how = "left")
        accessory_filtered_all_call_init_merge_grouped = filtered_all_call_init_merge_grouped[filtered_all_call_init_merge_grouped["itemtype"] == "ZACC"]
        number_of_products_for_which_accessories_purchased = accessory_filtered_all_call_init_merge_grouped.groupby("customer")["cat_num"].nunique().reset_index()
        number_of_products_for_which_accessories_purchased.rename(columns={"cat_num":"number_of_productcats_for_which_accessories_purchased"},inplace = True)
        customdata = pd.merge(customdata,number_of_products_for_which_accessories_purchased,on = "customer",how = "left")
        number_of_productser_for_which_essentials_purchased = essential_filtered_all_call_init_merge_grouped.groupby("customer")["ProductSerial"].nunique().reset_index()
        number_of_productser_for_which_essentials_purchased.rename(columns={"ProductSerial":"number_of_productser_for_which_essentials_purchased"},inplace = True)
        customdata = pd.merge(customdata,number_of_productser_for_which_essentials_purchased,on = "customer",how = "left")
        number_of_productser_for_which_accessories_purchased = accessory_filtered_all_call_init_merge_grouped.groupby("customer")["ProductSerial"].nunique().reset_index()
        number_of_productser_for_which_accessories_purchased.rename(columns={"ProductSerial":"number_of_productser_for_which_accessories_purchased"},inplace = True)
        customdata = pd.merge(customdata,number_of_productser_for_which_accessories_purchased,on = "customer",how = "left")
        essential_all_call = allcall_item_copy[allcall_item_copy["itemtype"] == "ZADD"]
        unique_date_cust_prod = essential_all_call.groupby(["customer","ProductSerial"]).agg({"PostingDate":"nunique"}).reset_index()
        unique_date_cust_prod.rename(columns={"PostingDate":"number_of_purchases_cust_prod"},inplace = True)
        filtered_unique_date_cust_prod = unique_date_cust_prod[unique_date_cust_prod["number_of_purchases_cust_prod"]>1]
        final_counts1 = filtered_unique_date_cust_prod.groupby("customer")["ProductSerial"].nunique().reset_index()
        final_counts1.rename(columns={"ProductSerial":"number_of_products_for_which_repurchase_done"},inplace = True)
        customdata = pd.merge(customdata,final_counts1,on = "customer",how = "left")
        accessory_all_call = allcall_item_copy[allcall_item_copy["itemtype"] == "ZACC"]
        unique_date_cust_prod_accessory = accessory_all_call.groupby(["customer","ProductSerial"]).agg({"PostingDate":"nunique"}).reset_index()
        unique_date_cust_prod_accessory.rename(columns={"PostingDate":"number_of_purchases_cust_prod_accessory"},inplace = True)
        filtered_unique_date_cust_prod_accessory = unique_date_cust_prod_accessory[unique_date_cust_prod_accessory["number_of_purchases_cust_prod_accessory"]>1]
        final_counts2 = filtered_unique_date_cust_prod_accessory.groupby("customer")["ProductSerial"].nunique().reset_index()
        final_counts2.rename(columns={"ProductSerial":"number_of_products_for_which_repurchase_done_accessories"},inplace = True)
        customdata = pd.merge(customdata,final_counts2,on = "customer",how = "left")
        def calculate_avg_diff(dates1):
            dates = pd.Series(dates1).sort_values().drop_duplicates()
            diffs = dates.diff().dt.days.dropna()
            if diffs.empty:
                return 0
            return diffs.mean()
        avg_diff = essential_all_call.groupby("customer")["PostingDate"].apply(calculate_avg_diff).reset_index()
        avg_diff.rename(columns={"PostingDate":"avg_date_diff_essentials"},inplace = True)
        customdata = pd.merge(customdata,avg_diff,on = "customer",how = "left")
        avg_diff_accessory = accessory_all_call.groupby("customer")["PostingDate"].apply(calculate_avg_diff).reset_index()
        avg_diff_accessory.rename(columns={"PostingDate":"avg_date_diff_accessory"},inplace = True)
        customdata = pd.merge(customdata,avg_diff_accessory,on = "customer",how = "left")
        number_of_unique_essentials = essential_all_call.groupby("customer")["ItemCode"].nunique().reset_index()
        number_of_unique_essentials.rename(columns={"ItemCode":"number_of_unique_essentials"},inplace = True)
        customdata = pd.merge(customdata,number_of_unique_essentials,on = "customer",how = "left")
        number_of_unique_accessories = accessory_all_call.groupby("customer")["ItemCode"].nunique().reset_index()
        number_of_unique_accessories.rename(columns={"ItemCode":"number_of_unique_accessories"},inplace = True)
        customdata = pd.merge(customdata,number_of_unique_accessories,on = "customer",how = "left")    
        file_path6=r"Data\Bangalore\Price_sensitivity_ratio_FINAL .csv"
        Price_sensitivity_ratio_FINAL_df=pd.read_csv(file_path6)
        initdf.rename(columns={"zzr3matId":"Mat_ID1"},inplace = True)
        Price_sensitivity_ratio_FINAL_df['Mat_ID1']=Price_sensitivity_ratio_FINAL_df['Mat_ID1'].map(str)
        initdf['Mat_ID1']=initdf['Mat_ID1'].map(str)
        price_init_merged_df = pd.merge(initdf,Price_sensitivity_ratio_FINAL_df,on = "Mat_ID1",how = "left")
        both_null = price_init_merged_df["Mat_ID1_price"].isnull() & price_init_merged_df["Relative_ratio"].isnull()
        price_init_merged_df.loc[both_null,"Mat_ID1_price"] = 1
        price_init_merged_df.loc[both_null, "Relative_ratio"] = 0.5
        price_init_merged_df["Weighted component"] = price_init_merged_df["Mat_ID1_price"] * price_init_merged_df["Relative_ratio"]
        agg_cust = price_init_merged_df.groupby("zzsoldto").agg({"Weighted component":"sum","Mat_ID1_price":"sum"}).reset_index()
        agg_cust["price_sensitivity"] = agg_cust["Weighted component"]/agg_cust["Mat_ID1_price"]
        agg_cust.rename(columns={"zzsoldto":"customer"},inplace = True)
        customdata = pd.merge(customdata,agg_cust,on = "customer",how = "left")
        customdata.drop(columns = ["Mat_ID1_price"],inplace = True)
        customdata.drop(columns = ["Weighted component"],inplace = True)
        amc_prior = {"AMC to AMC":7,"EW to AMC": 6,"IW to AMC" : 5,"IW to EW" : 4,"OG to AMC" : 3,"WTY to MTY": 2,"OG to EW": 1}
        amcdf["amc_prior"] = amcdf["WarConv"].map(amc_prior)
        cust_prod_war_conv_max = amcdf.groupby(["CustomerCode","SrNo"])["amc_prior"].max().reset_index()
        loyalty_score = cust_prod_war_conv_max.groupby("CustomerCode")["amc_prior"].mean().reset_index()
        loyalty_score.rename(columns={"amc_prior":"loyalty_score"},inplace = True)
        loyalty_score.rename(columns={"CustomerCode":"customer"},inplace = True)
        customdata = pd.merge(customdata,loyalty_score,on = "customer",how = "left")

        amc_constraint_1 = amcdf_copy[["CustomerCode","SrNo","ContStrtDat","ContEndDat","Price"]]
        amc_constraint_1=amc_constraint_1.rename(columns={"CustomerCode":"customer"})
        amc_constraint_1=amc_constraint_1.rename(columns={"SrNo":"Serial"})
        spu_constraint_1 = spudf_copy[["CustomerC","Serial","ItemCategory","CallBookDate","NetValue"]]
        spu_constraint_1=spu_constraint_1.rename(columns={"CustomerC":"customer"})
        spu_constraint_1 = pd.merge(spu_constraint_1,amc_constraint_1,on = ["customer","Serial"],how = "left" )
        filtered_spu_constraint_1 = spu_constraint_1[spu_constraint_1["ItemCategory"] == "ZAMC"]
        result_filtered_spu_constraint_1 = filtered_spu_constraint_1.groupby(["customer","Serial"]).agg({"NetValue":"sum","Price":"max"}).reset_index()
        result_filtered_spu_constraint_1["Profitability"] = result_filtered_spu_constraint_1["NetValue"]/result_filtered_spu_constraint_1["Price"]
        result_filtered_spu_constraint_1["Profitable"] = np.where(result_filtered_spu_constraint_1["Profitability"]<= 1,1,0)
        pr_perc=result_filtered_spu_constraint_1.groupby("customer")["Profitable"].agg(["sum","count"]).reset_index()
        pr_perc.columns = ["customer","p_sum","p_count"]
        pr_perc['AMC_perc']=pr_perc["p_sum"]/pr_perc["p_count"]
        customdata = pd.merge(customdata,pr_perc,on = "customer",how = "left")
        avg_profitability = result_filtered_spu_constraint_1.groupby("customer")["Profitability"].mean().reset_index()
        avg_profitability.rename(columns={"Profitability":"avg_profitability"},inplace = True)
        customdata = pd.merge(customdata,avg_profitability,on = "customer",how = "left")

        essential_all_call = allcall_item_copy[allcall_item_copy["itemtype"] == "ZADD"]
        min_date_cust_prod_ess = essential_all_call.groupby(["customer","ProductSerial"])["PostingDate"].min().reset_index()
        min_date_cust_prod_ess.rename(columns = {"PostingDate": "min_date_cust_prod_ess"},inplace = True)
        init_copy["min_purchase_install"] = init_copy[["zzpurchaseDate","zzinstallDate"]].min(axis = 1)
        min_date_cust_prod = init_copy.groupby(["customer","ProductSerial"])["min_purchase_install"].min().reset_index()
        required_merged_ess_init = pd.merge(min_date_cust_prod_ess,min_date_cust_prod,on = ["customer","ProductSerial"],how = "left")
        required_merged_ess_init["diff_min_ifb_min_post"] = (required_merged_ess_init["min_purchase_install"]-required_merged_ess_init["min_date_cust_prod_ess"]).dt.days
        avg_required_merged_ess_init = required_merged_ess_init.groupby("customer")["diff_min_ifb_min_post"].mean().reset_index()
        avg_required_merged_ess_init.rename(columns={"diff_min_ifb_min_post":"difference_btw_first_ess_purchase_and_first_ifb_interaction"},inplace = True)
        customdata = pd.merge(customdata,avg_required_merged_ess_init,on = "customer",how = "left")
        accessory_all_call = allcall_item_copy[allcall_item_copy["itemtype"] == "ZACC"]
        min_date_cust_prod_acc = accessory_all_call.groupby(["customer","ProductSerial"])["PostingDate"].min().reset_index()
        min_date_cust_prod_acc.rename(columns = {"PostingDate": "min_date_cust_prod_acc"},inplace = True)
        required_merged_acc_init = pd.merge(min_date_cust_prod_acc,min_date_cust_prod,on = ["customer","ProductSerial"],how = "left")
        required_merged_acc_init["diff_min_ifb_min_post_acc"] = (required_merged_acc_init["min_purchase_install"]-required_merged_acc_init["min_date_cust_prod_acc"]).dt.days
        avg_required_merged_acc_init = required_merged_acc_init.groupby("customer")["diff_min_ifb_min_post_acc"].mean().reset_index()
        avg_required_merged_acc_init.rename(columns={"diff_min_ifb_min_post_acc":"difference_btw_first_acc_purchase_and_first_ifb_interaction"},inplace = True)
        customdata = pd.merge(customdata,avg_required_merged_acc_init,on = "customer",how = "left")
        customdata["days_since_latest_neg_remark"] = (datetime.now() - customdata["latest_negremark_date"]).dt.days
        zero_impute_cols=['loyalty_score','max_nps','no_of_mw_purchased','price_sensitivity',
        'number_of_productser_for_which_essentials_purchased','number_of_productser_for_which_accessories_purchased',
        'number_of_products_for_which_repurchase_done','number_of_products_for_which_repurchase_done_accessories',
        'number_of_unique_essentials','number_of_unique_accessories','AMC_perc','avg_profitability',
        'count_of_99','count_of_98','number_of_productcats_for_which_essentials_purchased',
        'number_of_productcats_for_which_accessories_purchased',
        'zadd_qty','zacc_qty','repeat_parts_for_customer','avg_price_amc','sum_amc_price','unique_icrno_count',
        'unique_prod_ser_amc','unique_prod_ser_mot','unique_prod_ser_labamc','unique_prod_ser_exwarranty',
        'unique_prod_ser_superwarranty','latest_max_nps_diff','latest_nps_rating','unique_medium_count',
        'closed_major_og_count','closed_major_amc_count','closed_major_sw_count','closed_major_ew_count',
        'closed_minor_ew_count','closed_minor_sw_count','closed_minor_amc_count','closed_minor_og_count',
        'customer_closed_wp_count','customer_closed_phone_count','customer_closed_email_count','avg_nps',
        'max_escalation','negative_remarks_count','customer_cancelled_status_count','customer_closed_status_count',
        'cancelled_status_count','closed_status_count','no_of_ac_purchased','no_of_unique_categories_purchased',
        'No_of_unique_products_purchased']
        
        for z in zero_impute_cols:
            customdata[z]=customdata[z].fillna(0)

        nine_impute_cols=['difference_btw_first_ess_purchase_and_first_ifb_interaction','difference_btw_first_acc_purchase_and_first_ifb_interaction','avg_date_diff_essentials','days_since_latest_neg_remark','avg_date_diff_accessory','Days_between_AMC_start_data_and_1st_consumption_date','days_since_last_purchase_zadd','days_since_last_purchase_zacc','days_out_of_warranty','days_left_for_current_amc_to_be_ended','AVG_TAT_OG','AVG_TAT_EW','AVG_TAT_AMC','AVG_TAT_SW','close_nr_days_diff','soft_post_days_diff','days_since_last_closed_status','age_of_last_dw_bought','age_of_last_wm_bought','age_of_last_ac_bought','no_days_btw_cross_cat_purchase_min','no_days_btw_cross_cat_purchase_max','no_days_btw_cross_cat_purchase_avg','no_days_btw_same_cat_purchase_min','no_days_btw_same_cat_purchase_max','no_days_btw_same_cat_purchase_avg','frequency_of_purchase','no_of_days_since_first_purchase']
        
        for n in nine_impute_cols:
            customdata[n]=customdata[n].fillna(99999)    

        Final_data=customdata.drop(columns=['p_sum','p_count','latest_allcallitem_date','latest_cont_end_date','zzenddate_max','con_end_date_max','latest_warranty_date',
                                            'latest_allcallitem_date_zacc','latest_allcallitem_date','latest_negremark_date','ClosedOn'])    

     

        output_file = "CX_segementation_final_data_Bangalore.csv"
        Final_data.to_csv(output_file, index=False)
        print(f"Preprocessed data saved to {output_file}")
        
    except Exception as e:
        print(f"Error during preprocessing: {e}")


# Example usage
if __name__ == "__main__":
    schema = 'Fact'
    queries = {
        'CrmAMCContracts': f"SELECT * FROM {schema}.CrmAMCContracts WHERE AMCPostingDate = CAST(GETDATE() AS DATE);",
        'CrmAllCall': f"SELECT * FROM {schema}.CrmAllCall WHERE PostingDate = CAST(GETDATE() AS DATE);",
        'sap': f"SELECT * FROM {schema}.SapZSPU WHERE POSTDATE = CAST(GETDATE() AS DATE);"  
    }
    loc = 'Azure'  # or 'Local'

    # Fetch the dataframes
    dataframes = fetch_multiple_dataframes(schema, queries, loc)

    if dataframes:
        for table_name, df in dataframes.items():
            print(f"Data from {table_name}:")
            print(df.head())  # Display the first few rows of each dataframe

        # Call preprocessing on the fetched data
        preprocessing(dataframes)
    else:
        print("Failed to fetch data.")
