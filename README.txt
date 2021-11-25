Usage Translator

#Configuration
1. Install XAMPP
2. Open XAMPP control panel and start the MySQL database and Apache
3. Click on admin of MySQL in XAMPP control panel to access MySQL DB
4. Open XAMPP Application and start the MySQL Database and Apache Web Server
5. Import usagetranslator.sql file to MySQL DB
6. Run the Solution.java file to execute the code (personally I am using IntelliJ)

#Dependencies used
1. 'mysql-connector-java' to connect to MySQL DB with the Java program
2. 'json-simple' to parse the JSON file
Please note that you can also refer to the actual dependencies list in pom.xml under <dependencies> tag.

#Assumptions made
Skip the record where 'PartNumber' cannot be found in typemap.json file
Only 1 such record found so far. The 'PartNumber' which cannot be found in json file is 'MOL001NR'

#Printed output of the code's response
Please refer to Output.txt for the code's response. It includes the following:
1. Error log for the entries that skipped (without 'PartNumber' and with non-positive 'itemCount'.
2. Success log of running totals over 'itemCount' for each of the products.
3. 'PartNumber' that cannot be found in typemap.json file.

#Others
You can refer to usagetranslator.sql for the records that are inserted into Database as well as the structures of the tables.