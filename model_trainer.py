import pandas as pd
import os
import numpy as np
import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
from data_ingestion import preprocessing
from data_ingestion import  fetch_multiple_dataframes
from sklearn.cluster import MiniBatchKMeans

#from src.logger.logging import logging
#from src.Exception.exception import customexception


def perform_clustering(input_file, output_file):
    """
    Perform clustering on the input data and save the results.
    """
    try:
        # Load the data
        df = pd.read_csv(input_file)
        df1 = df.copy()

        # Scaling (Standard scaler)
        col = list(set(list(df.columns)) - set(['customer']))
        for y in col:
            mean_col = df[y].mean()
            std_col = df[y].std()
            df[y] = df[y].apply(lambda x: (x - mean_col) / std_col)
        df = df.fillna(0)

        # Elbow plot for selecting number of clusters
        wcss = []
        for i in range(1, 7):
            kmeans = KMeans(n_clusters=i, init='k-means++', random_state=0)
            kmeans.fit(df[col])
            wcss.append(kmeans.inertia_)
        plt.plot(range(1, 7), wcss)
        plt.title('The Elbow Method')
        plt.xlabel('Number of clusters')
        plt.ylabel('WCSS')
       

        # KMeans clustering
        df_kmean = df.copy()
        kmeans = MiniBatchKMeans(n_clusters=5, init='k-means++', random_state=100, batch_size=1000)
        y_kmeans = kmeans.fit_predict(df_kmean[col])
        df1['Kmeans_cluster'] = y_kmeans

        # Save clustering results
        df1.to_csv(output_file, index=False)
        print(f"Clustering results saved to {output_file}")

    except Exception as e:
        print(f"Error during clustering: {e}")

if __name__ == "__main__":
    schema = 'Fact'
    queries = {
        'CrmAMCContracts': f"SELECT * FROM {schema}.CrmAMCContracts WHERE AMCPostingDate = CAST(GETDATE() AS DATE);",
        'CrmAllCall': f"SELECT * FROM {schema}.CrmAllCall WHERE PostingDate = CAST(GETDATE() AS DATE);",
        'sap': f"SELECT * FROM {schema}.SapZSPU WHERE POSTDATE = CAST(GETDATE() AS DATE);"  
    }
    loc = 'Azure'  # or 'Local'
    # First, preprocess the data
    dataframes = fetch_multiple_dataframes(schema, queries, loc)
    preprocessing(dataframes)

    # Use the preprocessed data for clustering
    input_file = "CX_segementation_final_data_Bangalore.csv"
    output_file = "clustering_result.csv"
    perform_clustering(input_file, output_file)
    #
