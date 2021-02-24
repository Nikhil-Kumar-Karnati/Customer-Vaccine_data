# Customer-Vaccine_data
We are getting the  customer details of a multi specialty hospital who has vaccinated. Has to perform ETL operations and save it in stage location
**statement:**
I own a Multi-Specialty Hospital chain with locations all across the world. My hospital is famous for
Vaccination. Patients who come to my hospital (across the globe) will be given a User Card with
which they can access any of my hospital in the world.
**Current Status:**
We maintain all customers in one database. There are heaps of customers with user cards to my
hospital. So, I decided to split up the customers based on the country and load them into
corresponding country tables.
To pull the customers as per Country, my developers should know what are all the places the
Customer Data is available. So, the data extracting will be done by our Source System. They will pull
the all the relevant customer data and will give us a Data file
Header Records Layout –
|H|Customer_Name|Customer_Id|Open_Date|Last_Consulted_Date|Vaccination_Id|Dr_Name|State|Count
ry|DOB|Is_Active
◦ Details Record Layout – |D|John|123456|20101012|20121013|MVD|Paul|NSW|AU|06031987|A

Detail Records will tell you what data you are getting from source, what data type, is it mandatory or
not and the length of the column.
File Position Column_Name Filed Length Data Type Mandatory Key Column
1 Customer Name 255 VARCHAR Y Y
2 Customer ID 18 VARCHAR Y N
3 Customer Open Date 8 DATE Y N
4 Last Consulted Date 8 DATE N N
5 Vaccination Type 5 CHAR N N
6 Doctor Consulted 255 CHAR N N
7 State 5 CHAR N N
8 Country 5 CHAR N N
9 Post Code 5 INT N N
10 Date of Birth 8 DATE N N
11 Active Customer 1 CHAR N N

**The sample file format will be:**
|H|Customer_Name|Customer_Id|Open_Date|Last_Consulted_Date|Vaccination_Id|Dr_Name|Sta
te|Country|DOB|Is_Active
|D|Alex|123457|20101012|20121013|MVD|Paul|SA|USA|06031987|A
|D|John|123458|20101012|20121013|MVD|Paul|TN|IND|06031987|A
|D|Mathew|123459|20101012|20121013|MVD|Paul|WAS|PHIL|06031987|A
|D|Matt|12345|20101012|20121013|MVD|Paul|BOS|NYC|06031987|A
|D|Jacob|1256|20101012|20121013|MVD|Paul|VIC|AU|06031987|A

Now using the ETL process now we loaded the data into Staging Tables. Intermediate tables will look
like below:
Name Cust_I Open_Dt Consul_Dt VAC_ID DR_Name State County DOB FLAG
Alex 123457 20101012 20121013 MVD Paul SA USA 6031987 A
John 123458 20101012 20121013 MVD TN IND 6031987 A
Mathew 123459 20101012 20121013 MVD WAS PHIL 6031987 A
Matt 12345 20101012 20121013 MVD BOS NYC 6031987 A
Jacob 1256 20101012 20121013 MVD VIC AU 6031987 A

• All customers related to India will go to Table_India and so on. 
