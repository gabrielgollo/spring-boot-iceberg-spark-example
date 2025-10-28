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
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Types;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

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
        Table table = getTable();

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

    public List<Map<String, Object>> findByFieldAndValue(String field, String value) {
        Table table = getTable();
        List<Map<String, Object>> results = new ArrayList<>();

        try {
            try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
                for (Record record : records) {
                    if (recordMatchesField(record, field, value)) {
                        results.add(recordToMap(record));
                    }
                }
            }

            log.info("Found {} records", results.size());

        } catch (Exception e) {
            throw new RuntimeException("Search error: " + e.getMessage(), e);
        }

        return results;
    }

    public List<Map<String, Object>> scanAll() {
        Table table = getTable();
        List<Map<String, Object>> results = new ArrayList<>();

        try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
            for (Record record : records) {
                results.add(recordToMap(record));
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
