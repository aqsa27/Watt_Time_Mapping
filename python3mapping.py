import pandas as pd

def get_files_dataframes():
    # reading csv data into dataframes
    entso_df=pd.read_csv("input/entso.csv",encoding="utf-8")
    gppd_df=pd.read_csv("input/gppd.csv",encoding="utf-8")
    platts_df=pd.read_csv("input/platts.csv",encoding="utf-8")
    return {
        "entso" : entso_df,
        "gppd"  : gppd_df,
        "platts": platts_df
    }

def data_cleaning(data):
    entso_df = data.get("entso")
    gppd_df = data.get("gppd")
    platts_df = data.get("platts")
    # data clean up
    #Remove the Country Abbrevation from country Column 
    entso_df['country'] = entso_df['country'].str.slice(stop=-5)
    #Converting the plant_name column to upper case
    entso_df['plant_name'] = entso_df['plant_name'].str.upper()
    #Converting the unit_name column to upper case
    entso_df['unit_name'] = entso_df['unit_name'].str.upper()
    #Converting the Country name to upper
    gppd_df['country_long'] = gppd_df['country_long'].str.upper()

    return {
        "entso" : entso_df,
        "gppd"  : gppd_df,
        "platts": platts_df
    }

def data_preprocessing(data):
    entso_df = data.get("entso")
    gppd_df = data.get("gppd")
    platts_df = data.get("platts")

    #data pre-preprocessing
    platts_df['platts_plant_id'] = platts_df['platts_plant_id'].astype(str)
    gppd_df['platts_plant_id'] = gppd_df['platts_plant_id'].astype(str)

    return {
        "entso" : entso_df,
        "gppd"  : gppd_df,
        "platts": platts_df
    }

def get_merged_results(data):
    entso_df = data.get("entso")
    gppd_df = data.get("gppd")
    platts_df = data.get("platts")
    #I am merging and the length of p_d is same as length of gppd_df)
    platts_gppd_df=pd.merge(platts_df,gppd_df,on='platts_plant_id',how='left',indicator=True)
    final_df=pd.merge(entso_df,platts_gppd_df,how='inner',left_on=['unit_fuel','plant_name'],right_on=['unit_fuel','plant_name_x'])
    output = final_df[['entso_unit_id', 'platts_unit_id', 'gppd_plant_id']]

    return output

def write_data_to_file(results):
    results.to_csv("output/mapping.csv",index=False)

if __name__ == '__main__':
	
    # Reading data from CSV into dataframes
    data = get_files_dataframes()
    entso_df = data.get("entso")
    gppd_df = data.get("gppd")
    platts_df = data.get("platts")

    # Cleaning and preprocessing data
    data = data_cleaning(data)
    data = data_preprocessing(data)

    # Merging data from 3 files
    merged_results = get_merged_results(data)

    # Writing results to mapping.csv file
    write_data_to_file(merged_results)
    

    

