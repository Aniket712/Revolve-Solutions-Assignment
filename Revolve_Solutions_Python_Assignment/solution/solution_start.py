import argparse
import pandas as pd
import glob
import json
import os
import errno

def get_params() -> dict:
    parser = argparse.ArgumentParser(description='DataTest')
    parser.add_argument('--customers_location', required=False, default="C:/Users/anike/PycharmProjects/Revolve_Solutions_Python_Assignment/input_data/starter/customers.csv")
    parser.add_argument('--products_location', required=False, default="C:/Users/anike/PycharmProjects/Revolve_Solutions_Python_Assignment/input_data/starter/products.csv")
    parser.add_argument('--transactions_location', required=False, default="C:/Users/anike/PycharmProjects/Revolve_Solutions_Python_Assignment/input_data/starter/transactions/")
    parser.add_argument('--output_location', required=False, default="C:/Users/anike/PycharmProjects/Revolve_Solutions_Python_Assignment/output_data/outputs/")
    return vars(parser.parse_args())

def get_trans_filepath(params):
    #using glob to get paths of all json files inside every directory
    path_trans = glob.glob(params['transactions_location'] + '**/*.json', recursive=True)
    return path_trans

def populate_dfs(params,path_trans):
    #Read data from customer.csv to Dataframe
    df_cust = pd.read_csv(params['customers_location'])
    #Read data from products.csv to Dataframe
    df_prod = pd.read_csv(params['products_location'])

    #Use a loop to read and append data from multiple json files to a Dataframe
    appended_data = []
    for i in path_trans:
        data = [json.loads(line) for line in open(i, 'r')]
        #Noramlize json to get product_id and price column in Dataframe
        df = pd.json_normalize(data, meta=['customer_id', 'date_of_purchase'], record_path=['basket'])
        appended_data.append(df)
    df_trans = pd.concat(appended_data)
    return df_cust,df_prod,df_trans

def join_group_df(df_cust,df_prod,df_trans):
    #Merging the 3 Dataframes
    df_merge1 = pd.merge(df_trans,df_cust, on = 'customer_id',how = 'left')
    df_merge2 = pd.merge(df_merge1,df_prod, on = 'product_id', how='right')

    #Grouping on the basis of customer_id','loyalty_score','product_id','product_category','date_of_purchase to get product_count date-wise
    df_grouped = df_merge2.groupby(['customer_id','loyalty_score','product_id','product_category','date_of_purchase'],as_index = False).agg({'product_description':['count']})
    df_grouped.rename(columns={'product_description':'product_count'},inplace = True)
    return df_grouped

def write_weekly_json(params,df_grouped):
    df_grouped['date_of_purchase'] = pd.to_datetime(df_grouped['date_of_purchase'])

    #Split dataframe in list of dataframe week-wise
    df_res_weekwise = [g for n, g in
                       df_grouped.groupby(pd.to_datetime(df_grouped['date_of_purchase']).dt.isocalendar().week)]

    #Looping through the list to group and load into JSON files
    for i in range(0, len(df_res_weekwise) - 1):
        #Creating variables for proper weekly date-wise naming of json files
        week_start = df_res_weekwise[i].sort_values(by='date_of_purchase')['date_of_purchase'].head(1).to_string().split()[1]
        week_end = df_res_weekwise[i].sort_values(by='date_of_purchase', ascending=False)['date_of_purchase'].head(
            1).to_string().split()[1]

        #Final grouping to get weekly data into json files
        df_res_json = df_res_weekwise[i].groupby(['customer_id', 'loyalty_score', 'product_id', 'product_category'],as_index=False).agg({('product_count', 'count'): ['sum']}).sort_values(by='customer_id')

        #Renaming columns as per requirement
        df_res_json.columns = ['customer_id', 'loyalty_score', 'product_id', 'product_category', 'product_count']

        #Writing json files into filesystem
        data_json = df_res_json.to_dict(orient='records')
        filename = params['output_location'] + week_start + '__' + week_end + '.json'
        if not os.path.exists(os.path.dirname(filename)):
            try:
                os.makedirs(os.path.dirname(filename))
            except OSError as exc:  # Guard against race condition
                if exc.errno != errno.EEXIST:
                    raise
        with open(filename, "w") as writeJSON:
            json.dump(data_json, writeJSON)

def main():
    params = get_params()

    #Populate a list with filepaths of transaction json files
    path_trans = get_trans_filepath(params)

    #Load the data from all files into respective Dataframes
    df_cust,df_prod,df_trans= populate_dfs(params,path_trans)

    #Join and Groupby to aggregate the product_count
    df_grouped = join_group_df(df_cust,df_prod,df_trans)

    #Divide Dataframe week-wise before loading into json files as per requirement.
    write_weekly_json(params,df_grouped)


if __name__ == "__main__":
    main()
