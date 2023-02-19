# Projet data Lakes and Data Integration

Yakout TOUHAMI KADIRI - Mustapha BENHALIMA 

Goal of the project : Build a full pipeline processing a dataset composed of stocks data.

Architecture :

![MicrosoftTeams-image (1).png](Projet%20data%20Lakes%20and%20Data%20Integration%20c7a45c4fa9994755ae247ba445d25920/MicrosoftTeams-image_(1).png)

# Steps:

### **1) The first step is to import data of the stocks that is available in the link into an azure storage solution.**

- We create the script that will upload the data to an azure storage account. (.py on github)
- Install requirements : via the command : **$ pip install -r requirements.txt** + Set the environment variables.
- Create storage account, and a container in the storage account and set the environment variables.
- Now we can run the script created at the beginning

[https://drive.google.com/file/d/1F63LhH7LycfQPiFpjVTqg5mWe2t_YiLy/view?usp=](https://drive.google.com/file/d/1F63LhH7LycfQPiFpjVTqg5mWe2t_YiLy/view?usp=) sharing)

### 2) The second step is to transform the csv files into one csv file and add the stock name column to it, and then store it into a data lake with an Azure batch activity.

In order to do so, We have created a storage account, two containers, one for the csv files and the other for the merged file.
********

### **3) The third step is to create a copy activity that takes the data from the csv file and put it into a SQL database.**

To achieve this we used the file in the source then created a new sql dataset using a linked service in order to achieve that.

### 4) The final step is to create a databricks activity that connect to this sql database (we will code the functions detailled bellow) :

For this we : 

- Created a Databricks workspace
- Then in the Databricks workspace we create a new notebook where we will put our code.
- Then we connected to the SQL database by setting our JDBC connection properties.

a) Now we can create the function  "**Daily Return Rate**" function that get a stock name, a start and end date and output the daily return date of this stock during this period (cf Notebook).

b) Also the function **Moving average** that takes a stock name, a start and end data, and a number of moving points (5 points for example) and return a new dataframe with the applied moving average over the opening price column.

c) The databricks activity gives as an output the results of the notebook into a storage account.