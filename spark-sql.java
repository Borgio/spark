package myspark;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map;
import java.util.logging.Level; // Logging?
import java.util.logging.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public class SparkSQL {

    private static JavaSparkContext sc;
    private static SQLContext sqlContext;
    
    public static void init() {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("PerformQuery");
        conf.set("spark.driver.allowMultipleContexts", "true");
        sc = new JavaSparkContext(conf);
        sqlContext = new SQLContext(sc);        
    }
    
    public static void destroy() {
        sc.stop();                        
    }
    
    /**
     *
     * @param args
     * @throws java.io.IOException
     */
    public static void test(String[] args) throws IOException {
        init();
        
        String jsonString = new String(Files.readAllBytes(Paths.get("src/testspark/example.json")));
        Map<String, Object> map = APIReader.Json2Map(jsonString);
        Map<String, Object> output = performQuery("Select * from data", map);
        APIReader.printMap(output);
        destroy();
    }
           
    /**
     * @param query
     * @param input
     * @return 
     * @throws java.io.IOException 
     */    
    public static Map<String, Object> performQuery(String query, Map<String, Object> input) throws IOException {
        String inputJson = APIReader.Map2Json(input);

        JavaRDD<String> jsonRDD = sc.parallelize(Arrays.asList(inputJson)); // json string here
        sqlContext.read().json(jsonRDD).registerTempTable("data");
        DataFrame df = sqlContext.sql(query);
                
        String[] result = (String[]) df.toJSON().collect();
        String jsonOut = String.join("", result);
        //System.out.println(jsonOut);     
        Map<String, Object> output = APIReader.Json2Map(jsonOut);
        return output;
    }    
}
