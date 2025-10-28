package com.gabrielgollo.myApp.domain.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Profile("!local")
@Service
public class SparkService {

    private final SparkSession sparkSession;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Value("${iceberg.catalog:glue_catalog}")
    private String catalogName;

    @Value("${iceberg.database:default}")
    private String database;

    @Value("${iceberg.table:tb_default}")
    private String table;


    public SparkService(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    public void insertData(Map<String, Object> data) {
        log.info("=== STARTING INSERTION ===");
        log.info("Table: {}.{}.{}", catalogName, database, table);
        log.info("Data: {}", data);

        try {
            log.info("Step 1: Building SQL...");
            String columns = String.join(", ", data.keySet());
            String values = data.values().stream()
                    .map(this::formatValueForSQL)
                    .collect(Collectors.joining(", "));

            String insertSQL = String.format(
                    "INSERT INTO %s.%s.%s (%s) VALUES (%s)",
                    catalogName, database, table, columns, values
            );

            log.info("SQL: {}", insertSQL);

            log.info("Step 2: Executing SQL...");
            Dataset<Row> result = sparkSession.sql(insertSQL);

            log.info("Forcing execution with count()...");
            long rowCount = result.count();
            log.info("Insert executed! {} rows affected", rowCount);

            log.info("=== INSERTION COMPLETED ===");

        } catch (Exception e) {
            log.error("=== ERROR IN INSERTION ===", e);
            throw new RuntimeException("Error inserting data: " + e.getMessage(), e);
        }
    }

    public void insertBatchData(String table, List<Map<String, Object>> dataList) {
        try {
            log.info("Inserting {} records into table {}", dataList.size(), table);

            List<Row> rows = new ArrayList<>();
            for (Map<String, Object> data : dataList) {
                rows.add(RowFactory.create(
                        data.get("transaction_id"),
                        data.get("status"),
                        data.get("valor"),
                        new java.sql.Timestamp(System.currentTimeMillis())
                ));
            }

            StructType schema = new StructType(new StructField[]{
                    new StructField("transaction_id", DataTypes.IntegerType, false, Metadata.empty()),
                    new StructField("status", DataTypes.StringType, false, Metadata.empty()),
                    new StructField("valor", DataTypes.DoubleType, false, Metadata.empty()),
                    new StructField("data_criacao", DataTypes.TimestampType, false, Metadata.empty())
            });

            Dataset<Row> df = sparkSession.createDataFrame(rows, schema);

            df.show();

            df.write()
                    .format("iceberg")
                    .mode("append")
                    .save(catalogName + "." + database + "." + table);

            log.info("Batch successfully inserted into table {}", table);

        } catch (Exception e) {
            log.error("Error inserting batch into table {}: {}", table, e.getMessage(), e);
            throw new RuntimeException("Error inserting batch data: " + e.getMessage(), e);
        }
    }

    private String formatValueForSQL(Object value) {
        if (value == null) {
            return "NULL";
        } else if (value instanceof String) {
            return "'" + value.toString().replace("'", "''") + "'";
        } else if (value instanceof Number) {
            return value.toString();
        } else if (value instanceof java.util.Date) {
            return "timestamp '" + new java.sql.Timestamp(((java.util.Date) value).getTime()) + "'";
        } else {
            return "'" + value.toString().replace("'", "''") + "'";
        }
    }

    public List<Map<String, Object>> queryTable() {
        try {
            String sql = String.format("SELECT * FROM %s.%s.%s", catalogName, database, table);
            Dataset<Row> dataset = sparkSession.sql(sql);
            return datasetToMap(dataset);
        } catch (Exception e) {
            log.error("Error querying table {}: {}", table, e.getMessage(), e);
            throw new RuntimeException("Error querying table: " + e.getMessage(), e);
        }
    }

    private List<Map<String, Object>> datasetToMap(Dataset<Row> dataset) {
        List<Map<String, Object>> result = new ArrayList<>();
        List<String> jsonList = dataset.toJSON().collectAsList();

        for (String json : jsonList) {
            try {
                result.add(objectMapper.readValue(json, Map.class));
            } catch (Exception e) {
                log.warn("Error converting JSON: {}", json, e);
            }
        }
        return result;
    }
}