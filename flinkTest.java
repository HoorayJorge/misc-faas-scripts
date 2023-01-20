import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.hive.HiveTableSink;
import org.apache.flink.streaming.connectors.hive.HiveOptions;
import org.apache.flink.streaming.connectors.hive.HiveStreamTableSink;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.client.HiveMetastoreClientFactory;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;

import java.util.Properties;

public class VCNFlowProcessor {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("bootstrap.servers", "localhost:9092");
        kafkaProperties.setProperty("group.id", "vcn-flow-consumer");

        // Define the schema for VCN flow logs
        TableSchema schema = TableSchema.builder()
        .field("timestamp", DataTypes.TIMESTAMP(3))
        .field("src_ip", DataTypes.STRING())
        .field("src_port", DataTypes.INT())
        .field("dst_ip", DataTypes.STRING())
        .field("dst_port", DataTypes.INT())
        .field("protocol", DataTypes.STRING())
        .field("subnet", DataTypes.STRING())
        .build();

    // Define the Kafka source
    tableEnv.connect(new Kafka()
        .version("universal")
        .topic("vcn-flow-filenames")
        .property("bootstrap.servers", "localhost:9092")
        .property("group.id", "vcn-flow-consumer")
    )
        .withFormat(new Json().failOnMissingField(false))
        .withSchema(schema)
        .createTemporaryTable("filenames");

    // Register the OCI file system
    tableEnv.registerFileSystem("oci", new OCIFileSystemFactory());

    // Define the OCI source
    tableEnv.connect(new FileSystem()
        .path("oci://my-bucket-name/path/to/vcn/flow/data")
        .fileSystem("oci")
        .format(new GzipTextInputFormat())
    )
        .withFormat(new Json().failOnMissingField(false))
        .withSchema(schema)
        .createTemporaryTable("vcn_flow_data");

    // Define the whitelisted subnets
    String[] subnetWhitelist = new String[] {"subnet-a", "subnet-b"};

    // Define the filter function
    FilterFunction<Row> filter = new FilterFunction<Row>() {
    @Override
    public boolean filter(Row value) {
        String srcIp = value.getField(1).toString();
        String dstIp = value.getField(3).toString();
        return !Arrays.asList(subnetWhitelist).contains(srcIp) && !Arrays.asList(subnetWhitelist).contains(dstIp);
    }
    };

    // Read the filenames from the Kafka topic
    DataStream<String> filenames = env.addSource(new FlinkKafkaConsumer<>("vcn-flow-filenames", new SimpleStringSchema(), kafkaProperties));

    // Read the VCN flow data from the OCI bucket
    DataStream<Row> vcnFlowData = tableEnv.toAppendStream(tableEnv.sqlQuery("SELECT * FROM vcn_flow_data"), Row.class)
    .filter(new FileNameFilter(filenames));

    // Filter the data based on subnet
    DataStream<Row> filteredData = vcnFlowData.filter(filter);

    // Register the Hive catalog
    HiveCatalog hive = new HiveCatalog("my_hive_catalog", "default", "thrift://remote-host:9083", "", "");
    tableEnv.registerCatalog("hive", hive);
    tableEnv.useCatalog("hive");

    // Define the Hive table sink
    HiveTableSink hiveSink = new HiveTableSink(new HiveOptions(hive.getHiveConf()),
            HiveMetastoreClientFactory.create(hive.getHiveConf()), "hive", "non_compliant_vcn_flows");

    // Write the non-compliant events to a Hive table
    filteredData.addSink(hiveSink);

    env.execute("VCN Flow Processor");
    }
}


