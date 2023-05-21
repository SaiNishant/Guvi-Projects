# Youtube Data Harvesting and SQL Warehousing

1) The youtube.py has the complete code to run the Streamlit Application.
2) Before we run the Python code, please make sure the schemas are bulit. Please create the required tables from the Youtube Warehouse Schemas.SQL file.
3) Run the above mentioned file in MySQL.
4) Now, coming to the Python, we need an API key. Please generate the API key using Google API Services. 
5) In the main block, you can enter your API key.
6) Also, make sure you have all the MySQL connection details ready since there are instances where you need to enter your localhost credentials.
7) We are also Mongo DB, please make sure that localhost of MongoDB is working.
8) The code is written in 3 Classes. 
9) The First Class - youtube_harvest() is responsible for geting all the raw data from youtube using API and pushing it to MongoDB.
10) The Second Class - migration() is responsible in migrating the selected channels' (from Streamlit web App) information from MongoDB to MySQL Warehouse. 
11) The Third Class - Analysis() is responsible in giving the result of the selected query (from Streamlit web App). These results are again shown on the Web Application itself.
12) The Analysis Queries.SQL is attached for reference which are used in Python code for the listed queries. 
