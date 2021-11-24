import java.sql.*;
import java.io.*;
import java.util.*;

import org.json.simple.JSONObject;
import org.json.simple.parser.*;

public class Solution {
    //driver function
    public static void main (String[] args) {
        //define file paths
        String csvFilePath = "ProjectFiles\\Sample_Report.csv";
        String jsonFilePath = "ProjectFiles\\typemap.json";
        //Insert data into DB
        insertData(csvFilePath,jsonFilePath);
    }

    /**
     * This method is to set up DB connection and insert data into DB
     * @param csvFilePath File path for csv file to be read
     * @param jsonFilePath File path for json file to be read
     */
    private static void insertData(String csvFilePath, String jsonFilePath) {
        String jdbcUrl = "jdbc:mysql://localhost:3306/UsageTranslator";
        String username = "root";
        String password = "";

        int batchSize = 50;
        int countChargeable = 0;
        int countDomains = 0;

        //Map to keep track of running total for each product
        HashMap<String,Integer> itemCountMap = new HashMap<>();

        Connection connection;
        try {
            connection = DriverManager.getConnection(jdbcUrl,username,password);
            connection.setAutoCommit(false);

            //SQL insert statement for chargeable table
            String sql_chargeable = "INSERT INTO chargeable values(?,?,?,?,?,?)";
            PreparedStatement statement_chargeable = connection.prepareStatement(sql_chargeable);

            //SQL insert statement for domains table
            String sql_domains = "INSERT INTO domains values(?,?,?)";
            PreparedStatement statement_domains = connection.prepareStatement(sql_domains);

            //Set to store unique domains
            HashSet<String> uniqueDomainsSet = new HashSet<>();

            //list to output the missing part number in json file
            List<String> missingProductMapList = new ArrayList<>();

            String lineText;
            //read json file
            JSONParser jsonparser = new JSONParser();
            Object obj = jsonparser.parse(new FileReader(jsonFilePath));
            JSONObject productObj = (JSONObject) obj;

            //read csv file
            BufferedReader lineReader = new BufferedReader(new FileReader(csvFilePath));
            lineReader.readLine(); //skip first row (header)
            int idx = 1; //row number in csv file
            while ((lineText = lineReader.readLine()) != null) {
                String[] data = lineText.split(",");
                //helper functions to insert data into corresponding tables
                insertChargeableTable(statement_chargeable,data,productObj,missingProductMapList,itemCountMap,idx,countChargeable,batchSize);
                insertDomainsTable(statement_domains, data, uniqueDomainsSet, countDomains, batchSize);
                idx++;
            }

            lineReader.close();
            statement_chargeable.executeBatch();
            statement_domains.executeBatch();
            connection.commit();
            connection.close();

            System.out.println("\nSuccess Log: Stats of running totals over 'itemCount' for each products");
            printRunningTotalStats(itemCountMap);
            System.out.println("\nMissing PartNumber in json file:");
            System.out.println(missingProductMapList);
            System.out.println("\nData successfully inserted!");
        } catch (Exception exception) {
            exception.printStackTrace();
        }
    }

    /**
     * This method is used to insert data into chargeable table
     * @param statement PreparedStatement to set values to be inserted into DB
     * @param data Array String of a line text inside csv file
     * @param productObj JSON object to map partnumber to product
     * @param missingProductMapList List to store partnumber that is not found in json file
     * @param itemCountMap HashMap to store product with its corresponding running total
     * @param idx This is row number of each line in csv file
     * @param count number of records inserted into table
     * @param batchSize size of the batch
     */
    private static void insertChargeableTable(PreparedStatement statement, String[] data, JSONObject productObj, List<String> missingProductMapList, HashMap<String,Integer> itemCountMap, int idx, int count, int batchSize) {
        //Define configurable list to be skipped
        HashSet<Integer> configurableList = new HashSet<>();
        configurableList.add(26392);

        //Define Unit Reduction Rule Map
        HashMap<String,Integer> unitReductionRuleMap = new HashMap<>();
        unitReductionRuleMap.put("EA000001GB0O",1000);
        unitReductionRuleMap.put("PMQ00005GB0R",5000);
        unitReductionRuleMap.put("SSX006NR",1000);
        unitReductionRuleMap.put("SPQ00001MB0R",2000);

        int id = 0; // auto-increment id
        int partnerId = Integer.parseInt(data[0]);
        String partNumber = data[9];
        String product = (String)productObj.get(partNumber);
        String planID = removeNonAlphanumeric(data[3]);
        String plan = data[7];
        int usage = Integer.parseInt(data[10]);

        //Calculate usage with Unit Reduction Rule if corresponding PartNumber is included in Unit Reduction Rule
        if (unitReductionRuleMap.containsKey(partNumber)) {
            usage = applyUnitReductionRule(unitReductionRuleMap,usage,partNumber);
        }

        //Case where PartNumber is not found in typemap.json file
        if (product == null && !partNumber.isEmpty()) {
            missingProductMapList.add(partNumber);
        }

        //Log an error and skip the record with no part number and non-positive item count
        if (product == null || usage < 0 || configurableList.contains(partnerId)) {
            if (product == null && usage < 0)  {
                System.out.println("Skipped record " + idx + ": No Part Number and Non-positive Item Count");
            } else if (product == null) {
                System.out.println("Skipped record " + idx + ": No Part Number");
            } else if (usage < 0) {
                System.out.println("Skipped record " + idx + ": Non-positive Item Count");
            }
        } else {
            //Record total running item count for each product
            itemCountMap.put(product,itemCountMap.getOrDefault(product,0) + usage);

            try {
                //Set the values to be inserted into chargeable table
                statement.setInt(1, id);
                statement.setInt(2, partnerId);
                statement.setString(3, product);
                statement.setString(4, planID);
                statement.setString(5, plan);
                statement.setInt(6, usage);

                statement.addBatch();
                count++;
                if (count % batchSize == 0) {
                    statement.executeBatch();
                }
            } catch (Exception exception) {
                exception.printStackTrace();
            }
        }
    }

    /**
     * This method is used to insert data into domains table
     * @param statement PreparedStatement to set values to be inserted into DB
     * @param data Array String of a line text inside csv file
     * @param uniqueDomainsSet HashSet to store unique domains
     * @param count number of records inserted into table
     * @param batchSize size of the batch
     */
    private static void insertDomainsTable(PreparedStatement statement, String[] data, HashSet<String> uniqueDomainsSet, int count, int batchSize) {
        String domain = data[5];
        int id = 0; // auto-increment id
        String planID = removeNonAlphanumeric(data[3]);

        //Only insert the domain that is not in DB to avoid duplicate
        if (!uniqueDomainsSet.contains(domain) && domain != null && !domain.isEmpty()) {
            uniqueDomainsSet.add(domain);
            try {
                //Set the values to be inserted into domains table
                statement.setInt(1, id);
                statement.setString(2, planID);
                statement.setString(3, domain);
                statement.addBatch();
                count++;
                if (count % batchSize == 0) {
                    statement.executeBatch();
                }
            } catch (Exception exception) {
                exception.printStackTrace();
            }
        }
    }

    /**
     * This method is to remove non-alphanumeric characters
     * @param str String to be altered
     * @return String with non-alphanumeric characters
     */
    private static String removeNonAlphanumeric(String str) {
        return str.replaceAll("[^a-zA-Z0-9]", "");
    }

    /**
     * This method is to Apply Unit Reduction Rule
     * @param map this contains mapping for part number with their corresponding unit reduction rule
     * @param itemCount Current item count of the partnumber
     * @param partnumber to get corresponding unit reduction rule
     * @return integer value with Unit Reduction Rule applied
     */
    private static int applyUnitReductionRule(HashMap<String,Integer> map, int itemCount, String partnumber) {
        return itemCount / map.get(partnumber);
    }


    /**
     * This method is to output the stats of running total over 'itemCount' for each product
     * @param map this contains mapping for product with their corresponding running total
     */
    private static void printRunningTotalStats(HashMap<String,Integer> map) {
        for (String str: map.keySet()){
            System.out.println(str + ": " + map.get(str) + " items");
        }
    }
}
