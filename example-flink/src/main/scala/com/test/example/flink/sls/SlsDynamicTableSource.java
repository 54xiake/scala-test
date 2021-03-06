package com.test.example.flink.sls;

import com.aliyun.openservices.log.flink.ConfigConstants;
import com.aliyun.openservices.log.flink.FlinkLogConsumer;
import com.aliyun.openservices.log.flink.model.LogDeserializationSchema;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class SlsDynamicTableSource implements ScanTableSource {
    private String project;
    private String accessId;
    private String accessKey;
    private String endpoint;
    private String logstore;
    private String consumerBeginposition;
    private DecodingFormat<LogDeserializationSchema<RowData>> decodingFormat;
    private DataType producedDataType;
    private TableSchema schema;


    public SlsDynamicTableSource(String project, String accessId, String accessKey, String endpoint, String logstore, String consumerBeginposition,
                                 DecodingFormat<LogDeserializationSchema<RowData>> decodingFormat, DataType producedDataType,
                                 TableSchema schema
    ) {
        this.project = project;
        this.accessId = accessId;
        this.accessKey = accessKey;
        this.endpoint = endpoint;
        this.logstore = logstore;
        this.consumerBeginposition = consumerBeginposition;
        this.decodingFormat = decodingFormat;
        this.producedDataType = producedDataType;
        this.schema = schema;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .build();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        // create runtime classes that are shipped to the cluster

        final LogDeserializationSchema<RowData> deserializer = decodingFormat.createRuntimeDecoder(
                scanContext,
                producedDataType);

        //????????????logstores??????
        List<String> topics = Arrays.asList(this.logstore.split(","));
        Properties slsProperties = new Properties();
        // ?????????????????????????????????
        slsProperties.put(ConfigConstants.LOG_ENDPOINT, this.endpoint);
        // ????????????ak
        slsProperties.put(ConfigConstants.LOG_ACCESSSKEYID, this.accessId);
        slsProperties.put(ConfigConstants.LOG_ACCESSKEY, this.accessKey);
        // ????????????????????????????????????
        /**
         * begin_cursor, end_cursor, unix timestamp or consumer_from_checkpoint
         */
        slsProperties.put(ConfigConstants.LOG_CONSUMER_BEGIN_POSITION, this.consumerBeginposition);
//        /**
//         * ????????????
//         */
//        slsProperties.put(ConfigConstants.LOG_CONSUMERGROUP, "flink-consumer-test");
//        slsProperties.put(ConfigConstants.LOG_FETCH_DATA_INTERVAL_MILLIS, 3000);
//        slsProperties.put(ConfigConstants.LOG_MAX_NUMBER_PER_FETCH, 10);
//        /**
//         * DISABLED---Never commit checkpoint to remote server.
//         * ON_CHECKPOINTS---Commit checkpoint only when Flink creating checkpoint, which means Flink
//         *                  checkpointing must be enabled.
//         * PERIODIC---Auto commit checkpoint periodic.
//         */
//        slsProperties.put(ConfigConstants.LOG_CHECKPOINT_MODE, CheckpointMode.ON_CHECKPOINTS.name());
//        /**
//         * ???????????????ConfigConstants.LOG_CHECKPOINT_MODE?????????CheckpointMode.PERIODIC,?????????????????????????????????
//         * slsProperties.put(ConfigConstants.LOG_COMMIT_INTERVAL_MILLIS, "10000");
//         */

        FlinkLogConsumer<RowData> flinkLogConsumer = new FlinkLogConsumer<>(project, topics, (LogDeserializationSchema) deserializer, slsProperties);
        return SourceFunctionProvider.of(flinkLogConsumer, false);
    }

    @Override
    public DynamicTableSource copy() {
        return new SlsDynamicTableSource(project,accessId,accessKey,endpoint,logstore,consumerBeginposition,null, producedDataType,schema);
    }

    @Override
    public String asSummaryString() {
        return "sls Table Source";
    }

}
