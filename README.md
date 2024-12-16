# EDA_Netflix_Dataset_Spark

Exploratory Data Analysis (EDA) on Netflix datasets 2021 using PySpark. 
Assignment 03 of Cloud Computing CE408.

# Description:
Exploratory Data Analysis (EDA) on Netflix datasets 2021 using PySpark to see how in memory computatuons are performed by Spark which is 10-100 times faster than Hadoop.
And how the jobs are assigned the the worker nodes, completed and deleted.

## Dataset:

I have also downloaded the dataset in the repo link 
The dataset is as follows:
https://www.kaggle.com/datasets/swatikhedekar/exploratory-data-analysis-on-netflix-data


## Code Instructions:
1. Starting the Cluster.
 ```bash
   docker-compose up -d
   ```

2. Copy EDA on NEtflix Dataset to container:
   ```bash
   docker cp -L Netflix_dataset_EDA.py spark-master:/opt/bitnami/spark/Netflix_dataset_EDA.py
   ```

 3. Submit the PySpark job:
   ```bash
  docker exec spark-master spark-submit --master spark://spark-master:7077 /opt/bitnami/spark/Netflix_dataset_EDA.py
   ```

4. Check local host for running applicationsa and job summary:
[http://localhost:9090](http://localhost:9090)

## Results:
[https://github.com/Sohaib012/EDA_Netflix_Dataset_Spark/blob/main/Running_Cluster_Result.mp4
](https://github.com/Sohaib012/EDA_Netflix_Dataset_Spark/blob/a34cc78d999c546e971d7f9a94f9f64405814d7b/Running_Cluster_Result.mp4)
