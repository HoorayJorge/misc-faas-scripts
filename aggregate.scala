import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
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
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import java.util.Properties;
import java.sql.Timestamp;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class VCNFlowProcessor {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("bootstrap.servers", "localhost:9092");
        kafkaProperties.setProperty("group.id", "vcn-flow-consumer");

        // Define the schema for VCN flow logs
        TableSchema schema = TableSchema.builder()
                .field("start_time", DataTypes.TIMESTAMP(3))
                .field("end_time", DataTypes.TIMESTAMP(3))
                .field("src_ip", DataTypes.STRING())
                .field("dst_ip", DataTypes.STRING())
                .field("protocol", DataTypes.STRING())
                .field("subnet", DataTypes.STRING())
                .field("bytes", DataTypes.BIGINT())
                .field("packets", DataTypes.INT())
                .field("filename", DataTypes.STRING())
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

            // Read the filenames from the Kafka topic
            DataStream<String> filenames = env.addSource(new FlinkKafkaConsumer<>("vcn-flow-filenames", new SimpleStringSchema(), kafkaProperties));

            // Read the VCN flow data from the OCI bucket
            DataStream<Row> vcnFlowData = tableEnv.toAppendStream(tableEnv.sqlQuery("SELECT * FROM vcn_flow_data"), Row.class)
                .filter(new FileNameFilter(filenames))
                .map(new AddFilename(filenames));

            // filter the VCN flow events so only events where src_ip or dest_ip is not on a list of whitelisted subnets
            DataStream<Row> nonCompliantEvents = vcnFlowData.filter(new NonCompliantFilter(whitelistedSubnets));

            // Count the compliant and non compliant events for each filename
            DataStream<Metric> metrics = nonCompliantEvents
                .keyBy("filename")
                .process(new ComplianceCounter());

            // write the metrics to a hive table
            tableEnv
                .connect(new HiveTableSink("compliance_metrics", "hive"))
                .withFormat(new Json().failOnMissingField(false))
                .withSchema(new Schema()
                        .field("filename", DataTypes.STRING())
                        .field("compliant_count", DataTypes.BIGINT())
                        .field("non_compliant_count", DataTypes.BIGINT())
                        .field("timestamp", DataTypes.TIMESTAMP(3))
                        )
                        .createTemporaryTable("compliance_metrics");
            tableEnv.toRetractStream(tableEnv.sqlQuery("SELECT * FROM compliance_metrics"), Metric.class).addSink(new HiveStreamTableSink("compliance_metrics", "hive"));
            env.execute("VCN Flow Processor");
        }
                        
                        // Define the ComplianceCounter function that counts the compliant and non compliant events for each filename
    public static class ComplianceCounter extends KeyedProcessFunction<String, Row, Metric> {
        private ValueState<Tuple2<Long, Long>> countState;
        private ObjectMapper mapper = new ObjectMapper();

        @Override
        public void open(Configuration parameters) {
            countState = getRuntimeContext().getState(new ValueStateDescriptor<>("count-state", Types.TUPLE(Types.LONG, Types.LONG)));
        }

        @Override
        public void processElement(Row value, Context ctx, Collector<Metric> out) throws JsonProcessingException {
            Timestamp currentTimestamp = new Timestamp(System.currentTimeMillis());
            Tuple2<Long, Long> count = countState.value();
            if (count == null) {
                count = Tuple2.of(0L, 0L);
            }
            String filename = value.getField(8);
            if (whitelistedSubnets.contains(value.getField(5))) {
                count.f0 += 1;
            } else {
                count.f1 += 1;
            }
            countState.update(count);
            Metric metric = new Metric(filename, count.f0, count.f1, currentTimestamp);
            out.collect(metric);
        }
    }

    // Define the Metric class
    public static class Metric {
        public String filename;
        public Long compliant_count;
        public Long non_compliant_count;
        public Timestamp timestamp;

        public Metric() {}

        public Metric(String filename, Long compliant_count, Long non_compliant_count, Timestamp timestamp) {
            this.filename = filename;
            this.compliant_count = compliant_count;
            this.non_compliant_count = non_compliant_count;
            this.timestamp = timestamp;
        }

        public String toJson() throws JsonProcessingException {
            return new ObjectMapper().writeValueAsString(this);
        }
    }
}


