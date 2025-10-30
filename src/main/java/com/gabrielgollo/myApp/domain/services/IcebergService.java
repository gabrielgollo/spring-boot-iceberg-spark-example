package com.gabrielgollo.myApp.domain.services;

import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.*;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.*;

@Slf4j
@Service
public class IcebergService {
    private Catalog catalog;
    private String warehousePath;
    private String region;
    private String database;
    private String tableName;

    public IcebergService(
            @Value("${iceberg.warehouse}") String warehousePath,
            @Value("${iceberg.awsRegion}") String region,
            @Value("${iceberg.database}") String database,
            @Value("${iceberg.table}") String tableName
    ) {
        this.warehousePath = warehousePath;
        this.region = region;
        this.database = database;
        this.tableName = tableName;
        this.catalog = createGlueCatalog();
    }

    private Catalog createGlueCatalog() {
        GlueCatalog catalog = new GlueCatalog();

        Map<String, String> properties = new HashMap<>();
        properties.put("warehouse", this.warehousePath);
        properties.put("catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog");
        properties.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
        properties.put("s3.client.region", this.region);
        properties.put("glue.skip-archive", "true");

        catalog.initialize("glue_catalog", properties);
        return catalog;
    }

    public void insert(Map<String, Object> data) {
        try {
            List<Map<String, Object>> batch = Collections.singletonList(data);
            insertBatch(batch);

        } catch (Exception e) {
            throw new RuntimeException("Error inserting: " + e.getMessage(), e);
        }
    }

    public void insertBatch(List<Map<String, Object>> batchData) {
        insertBatch(batchData, null);
    }

    public void insertBatch(List<Map<String, Object>> batchData, Map<String, Object> options) {
        Table table = getTable();

        try {
            String partitionFieldName = options != null ? (String) options.get("partitionFieldName") : null;

            if (partitionFieldName == null) {
                insertBatchWithoutPartition(batchData, table);
                return;
            }

            Map<Object, List<Record>> partitionedRecords = new HashMap<>();
            for (Map<String, Object> data : batchData) {
                if (data.containsKey(partitionFieldName)) {
                    Object value = data.get(partitionFieldName);
                    if (value instanceof String) {
                        data.put(partitionFieldName, java.time.LocalDate.parse((String) value));
                    }
                }

                Record record = createRecord(table.schema(), data);
                Object partitionValue = record.getField(partitionFieldName);
                partitionedRecords.computeIfAbsent(partitionValue, k -> new ArrayList<>()).add(record);
            }

            GenericAppenderFactory appenderFactory = new GenericAppenderFactory(table.schema(), table.spec());
            AppendFiles appendFiles = table.newAppend();

            for (Map.Entry<Object, List<Record>> entry : partitionedRecords.entrySet()) {
                Object partitionValue = entry.getKey();
                List<Record> records = entry.getValue();

                String filename = "data-" + UUID.randomUUID() + ".parquet";
                String filePath = table.location() + "/data/" + filename;
                OutputFile outputFile = table.io().newOutputFile(filePath);

                long recordCount = 0;
                try (FileAppender<Record> appender = appenderFactory.newAppender(outputFile, org.apache.iceberg.FileFormat.PARQUET)) {
                    for (Record record : records) {
                        appender.add(record);
                        recordCount++;
                    }
                }

                long fileSize = table.io().newInputFile(filePath).getLength();
                DataFile dataFile = DataFiles.builder(table.spec())
                        .withPath(filePath)
                        .withFormat(org.apache.iceberg.FileFormat.PARQUET)
                        .withFileSizeInBytes(fileSize)
                        .withRecordCount(recordCount)
                        .build();

                appendFiles.appendFile(dataFile);
                log.info("Prepared {} records for partition {}", recordCount, partitionValue);
            }

            appendFiles.commit();
            log.info("Inserted batch successfully, total partitions: {}", partitionedRecords.size());

        } catch (Exception e) {
            throw new RuntimeException("Error inserting batch: " + e.getMessage(), e);
        }
    }

    public void insertBatchWithoutPartition(List<Map<String, Object>> batchData, Table table) {
        try {
            List<Record> records = new ArrayList<>();
            for (Map<String, Object> data : batchData) {
                records.add(createRecord(table.schema(), data));
            }

            GenericAppenderFactory appenderFactory = new GenericAppenderFactory(table.schema(), table.spec());

            String filename = "data-" + UUID.randomUUID() + ".parquet";
            String filePath = table.location() + "/data/" + filename;
            OutputFile outputFile = table.io().newOutputFile(filePath);

            long recordCount = 0;

            try (FileAppender<Record> appender = appenderFactory.newAppender(outputFile, org.apache.iceberg.FileFormat.PARQUET)) {
                for (Record record : records) {
                    appender.add(record);
                    recordCount++;
                }
            }

            long fileSize = table.io().newInputFile(filePath).getLength();

            DataFile dataFile = DataFiles.builder(table.spec())
                    .withPath(filePath)
                    .withFormat(org.apache.iceberg.FileFormat.PARQUET)
                    .withFileSizeInBytes(fileSize)
                    .withRecordCount(recordCount)
                    .build();

            AppendFiles append = table.newAppend();
            append.appendFile(dataFile);
            append.commit();

            log.info("Inserted " + records.size() + " records successfully!");

        } catch (Exception e) {
            throw new RuntimeException("Error inserting batch: " + e.getMessage(), e);
        }
    }

    public List<Map<String, Object>> findByPartitionAndValues(String partitionField, String partitionValue, String field, List<String> values) {
        Table table = getTable();
        List<Map<String, Object>> results = new ArrayList<>();

        try {
            Expression filter = Expressions.and(
                    Expressions.equal(partitionField, partitionValue),
                    Expressions.in(field, values)
            );

            try (CloseableIterable<Record> records = IcebergGenerics.read(table)
                    .where(filter)
                    .build()) {

                for (Record record : records) {
                    results.add(recordToMap(record));
                }
            }

            log.info("Found {} records for partition '{}'={} and field '{}' in {}",
                    results.size(), partitionField, partitionValue, field, values);

        } catch (Exception e) {
            throw new RuntimeException("Search error (partition + multiple values): " + e.getMessage(), e);
        }

        return results;
    }

    public List<Map<String, Object>> findByFieldAndValues(
            String field,
            List<String> values,
            Map<String, Object> options
    ) {
        Table table = getTable();
        List<Map<String, Object>> results = new ArrayList<>();

        try {
            Type fieldType = table.schema().findType(field);
            if (fieldType == null) {
                throw new RuntimeException("Field not found in schema: " + field);
            }

            List<Object> parsedValues = new ArrayList<>();

            for (String v : values) {
                Object val = v;

                switch (fieldType.typeId()) {
                    case INTEGER:
                        val = Integer.parseInt(v);
                        break;
                    case LONG:
                        val = Long.parseLong(v);
                        break;
                    case FLOAT:
                        val = Float.parseFloat(v);
                        break;
                    case DOUBLE:
                        val = Double.parseDouble(v);
                        break;
                    case BOOLEAN:
                        val = Boolean.parseBoolean(v);
                        break;
                    case DATE:
                        val = (int) LocalDate.parse(v).toEpochDay();
                        break;
                    case TIMESTAMP:
                        val = java.time.Instant.parse(v);
                        break;
                    default:
                        break;
                }

                parsedValues.add(val);
            }

            Expression filter = Expressions.in(field, parsedValues);
            var readBuilder = IcebergGenerics.read(table).where(filter);

            if (options != null && options.containsKey("partitionFieldName") && options.containsKey("partitionValue")) {
                String partitionField = (String) options.get("partitionFieldName");
                Object partitionValue = options.get("partitionValue");

                Type partitionType = table.schema().findType(partitionField);
                if (partitionType != null) {
                    if (partitionType.typeId() == Type.TypeID.DATE && partitionValue instanceof String) {
                        partitionValue = (int) LocalDate.parse((String) partitionValue).toEpochDay();
                    } else if (partitionType.typeId() == Type.TypeID.LONG && partitionValue instanceof String) {
                        partitionValue = Long.parseLong((String) partitionValue);
                    } else if (partitionType.typeId() == Type.TypeID.INTEGER && partitionValue instanceof String) {
                        partitionValue = Integer.parseInt((String) partitionValue);
                    }
                }

                Expression partitionFilter = Expressions.equal(partitionField, partitionValue);
                filter = Expressions.and(filter, partitionFilter);
                readBuilder = IcebergGenerics.read(table).where(filter);
            }

            try (CloseableIterable<Record> records = readBuilder.build()) {
                for (Record record : records) {
                    results.add(recordToMap(record));
                }
            }

            log.info("Found {} records where {} in {}, with options={}", results.size(), field, parsedValues, options);
        } catch (Exception e) {
            throw new RuntimeException("Search error: " + e.getMessage(), e);
        }

        return results;
    }



    public List<Map<String, Object>> scanAll() {
        return scanAll(null);
    }

    public List<Map<String, Object>> scanAll(Map<String, Object> options) {
        Table table = getTable();
        List<Map<String, Object>> results = new ArrayList<>();

        try {
            var readBuilder = IcebergGenerics.read(table);

            if (options != null && options.containsKey("partitionFieldName") && options.containsKey("partitionValue")) {
                String partitionField = (String) options.get("partitionFieldName");
                Object partitionValue = options.get("partitionValue");

                if (partitionValue instanceof String) {
                    try {
                        partitionValue = java.time.LocalDate.parse((String) partitionValue);
                    } catch (Exception ignore) {
                    }
                }

                if (partitionValue instanceof java.time.LocalDate) {
                    java.time.LocalDate date = (java.time.LocalDate) partitionValue;
                    long daysSinceEpoch = date.toEpochDay(); // Iceberg DATE = int de dias desde 1970-01-01
                    partitionValue = (int) daysSinceEpoch;
                }

                Expression filter = Expressions.equal(partitionField, partitionValue);
                readBuilder = readBuilder.where(filter);


                log.info("Scanning with partition filter: {} = {}", partitionField, partitionValue);
            } else {
                log.info("Scanning full table (no partition filter)");
            }

            try (CloseableIterable<Record> records = readBuilder.build()) {
                for (Record record : records) {
                    results.add(recordToMap(record));
                }
            }

            log.info("Total records found: {}", results.size());
        } catch (Exception e) {
            throw new RuntimeException("Error performing scanAll: " + e.getMessage(), e);
        }

        return results;
    }

    private Record createRecord(Schema schema, Map<String, Object> data) {
        Record record = GenericRecord.create(schema);

        for (Types.NestedField field : schema.columns()) {
            Object value = data.get(field.name());

            if (value == null) {
                record.setField(field.name(), null);
                continue;
            }

            switch (field.type().typeId()) {
                case LONG:
                    if (value instanceof Integer)
                        value = ((Integer) value).longValue();
                    break;
                case INTEGER:
                    if (value instanceof Long)
                        value = ((Long) value).intValue();
                    break;
            }

            record.setField(field.name(), value);
        }

        return record;
    }

    private boolean recordMatchesField(Record record, String field, String value) {
        try {
            Schema schema = record.struct().asSchema();
            for (int i = 0; i < schema.columns().size(); i++) {
                if (schema.columns().get(i).name().equals(field)) {
                    Object fieldValue = record.get(i);
                    return fieldValue != null && value.equals(fieldValue.toString());
                }
            }
            return false;
        } catch (Exception e) {
            return false;
        }
    }

    private Map<String, Object> recordToMap(Record record) {
        Map<String, Object> map = new HashMap<>();
        Schema schema = record.struct().asSchema();
        for (int i = 0; i < schema.columns().size(); i++) {
            String fieldName = schema.columns().get(i).name();
            map.put(fieldName, record.get(i));
        }
        return map;
    }

    private Table getTable() {
        return catalog.loadTable(TableIdentifier.of(database, tableName));
    }
}
