package com.test.example.flink.sls;

import com.alibaba.fastjson.JSONObject;
import com.aliyun.openservices.log.common.FastLog;
import com.aliyun.openservices.log.common.FastLogContent;
import com.aliyun.openservices.log.common.FastLogGroup;
import com.aliyun.openservices.log.common.LogGroupData;
import com.aliyun.openservices.log.flink.model.LogDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SlsDeserializer implements LogDeserializationSchema<RowData> {
    private final List<LogicalType> parsingTypes;
    private final DynamicTableSource.DataStructureConverter converter;
    private final TypeInformation<RowData> producedTypeInfo;

    public SlsDeserializer(List<LogicalType> parsingTypes, DynamicTableSource.DataStructureConverter converter, TypeInformation<RowData> producedTypeInfo) {
        this.parsingTypes = parsingTypes;
        this.converter = converter;
        this.producedTypeInfo = producedTypeInfo;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return producedTypeInfo;
    }

    @Override
    public RowData deserialize(List<LogGroupData> logGroups) {
        //在这里把sls consumer接收到的数据解析,转成RowData,内容就是一个json字符串
        //然后flink-log-connector中的com.aliyun.openservices.log.flink.model.LogDataFetcher中emitRecordAndUpdateState把RowData转成多个RowData
        List<Map<String, String>> collect = logGroups.stream()
                .map(LogGroupData::GetFastLogGroup)
                .map(FastLogGroup::getLogs)
                .flatMap(Collection::stream)
                .map(fastLog -> {
                    int count = fastLog.getContentsCount();
                    Map<String, String> log = new HashMap<>();
                    for (int cIdx = 0; cIdx < count; ++cIdx) {
                        FastLogContent content = fastLog.getContents(cIdx);
                        log.put(content.getKey(), content.getValue());
                    }
                    return log;
                }).collect(Collectors.toList());
//        ArrayList<RawLog> rawLogs = new ArrayList<>();
//        ArrayList<RawLog> rawLogs = new ArrayList<>();
//        for (LogGroupData logGroup : logGroups) {
//            FastLogGroup flg = logGroup.GetFastLogGroup();
//            for (int lIdx = 0; lIdx < flg.getLogsCount(); ++lIdx) {
//                FastLog log = flg.getLogs(lIdx);
//                RawLog rlog = new RawLog();
//                rlog.setTime(log.getTime());
//                for (int cIdx = 0; cIdx < log.getContentsCount(); ++cIdx) {
//                    FastLogContent content = log.getContents(cIdx);
//                    rlog.addContent(content.getKey(), content.getValue());
//                }
//                rawLogs.add(rlog);
//            }
//        }
        final RowKind kind = RowKind.valueOf("INSERT");
        final Row row = new Row(kind, parsingTypes.size());
//        Row row = new Row(1);
        row.setField(0, JSONObject.toJSONString(collect));
        return (RowData) converter.toInternal(row);
    }


}
