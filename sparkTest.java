import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object VCNFlowProcessor {
    def main(args: Array[String]): Unit = {

        val spark = SparkSession
            .builder()
            .appName("VCNFlowProcessor")
            .enableHiveSupport()
            .getOrCreate()

        val ociConfig = "ociConfigFile.json"
        val bucketName = "my-bucket-name"
        val prefix = "path/to/vcn/flow/data"
        val subnetWhitelist = Seq("subnet-a", "subnet-b")

        // Define the schema for VCN flow logs
        val schema = StructType(Seq(
            StructField("timestamp", TimestampType, true),
            StructField("src_ip", StringType, true),
            StructField("src_port", IntegerType, true),
            StructField("dst_ip", StringType, true),
            StructField("dst_port", IntegerType, true),
            StructField("protocol", StringType, true),
            StructField("subnet", StringType, true)
        ))

        // Continuously read new files from OCI object storage
        val data = spark.readStream
            .format("oci")
            .option("oci.config", ociConfig)
            .option("prefix", prefix)
            .load(bucketName)

        // Extract the space delimited data from the value field of the object storage message
       
        val spaceDelimitedData = data.selectExpr("cast (value as string) as data").select(split(col("data"), " ").as("data"))
        .select(
            $"data"(0).cast(TimestampType).as("timestamp"),
            $"data"(1).cast(StringType).as("src_ip"),
            $"data"(2).cast(IntegerType).as("src_port"),
            $"data"(3).cast(StringType).as("dst_ip"),
            $"data"(4).cast(IntegerType).as("dst_port"),
            $"data"(5).cast(StringType).as("protocol"),
            $"data"(6).cast(StringType).as("subnet")
        )

        // Filter the data based on subnet
        val filteredData = spaceDelimitedData.filter(col("subnet").isin(subnetWhitelist:_*))

        // Filter the events that did not match the whitelist and write them to a Hive table
        val query = filteredData
            .filter(!col("subnet").isin(subnetWhitelist:_*))
            .writeStream
            .format("hive")
            .option("checkpointLocation", "path/to/checkpoint")
            .option("path", "path/to/hive/table")
            .start()

        query.awaitTermination()
    }
}
