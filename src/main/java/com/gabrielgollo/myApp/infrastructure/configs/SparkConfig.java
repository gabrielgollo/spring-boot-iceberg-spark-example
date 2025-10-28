package com.gabrielgollo.myApp.infrastructure.configs;

import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Profile("!local")
@Configuration
public class SparkConfig {

    private static final Logger logger = LoggerFactory.getLogger(SparkConfig.class);

    @Value("${spark.master:spark://localhost:7077}")
    private String sparkMaster;

    @Value("${iceberg.catalog:glue_catalog}")
    private String catalogName;

    @Value("${iceberg.warehouse}")
    private String warehousePath;

    @Value("${aws.region:us-east-1}")
    private String awsRegion;

    @Value("${spark.driver.host:localhost}")
    private String driverHost;

    @Value("${spark.driver.port:7071}")
    private String driverPort;

    @Bean
    public SparkSession sparkSession() {
        try {
            logger.info("Initializing SparkSession with SparkMaster: {}", sparkMaster);
            logger.info("Glue Catalog: {}", catalogName);
            logger.info("Warehouse path: {}", warehousePath);

            SparkSession spark = SparkSession.builder()
                    .appName("iceberg-spring-api")
                    .master(sparkMaster)

                    // Iceberg extensions
                    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")

                    // Glue Catalog
                    .config("spark.sql.catalog." + catalogName, "org.apache.iceberg.spark.SparkCatalog")
                    .config("spark.sql.catalog." + catalogName + ".catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
                    .config("spark.sql.catalog." + catalogName + ".warehouse", warehousePath)
                    .config("spark.sql.catalog." + catalogName + ".io-impl", "org.apache.iceberg.aws.s3.S3FileIO")

                    // Glue configs
                    .config("spark.sql.catalog." + catalogName + ".glue.region", awsRegion)

                    // Lock manager
//                    .config("spark.sql.catalog." + catalogName + ".lock-impl", "org.apache.iceberg.aws.dynamodb.DynamoDbLockManager")
//                    .config("spark.sql.catalog." + catalogName + ".lock.table", "iceberg_locks")

                    .config("spark.sql.defaultCatalog", catalogName)

                    // Cluster configs
                    .config("spark.driver.host", this.driverHost)
                    .config("spark.driver.port", this.driverPort)
                    .config("spark.network.timeout", "600s")
                    .config("spark.executor.heartbeatInterval", "60s")

                    // Serializer
//                    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//                    .config("spark.kryo.registrationRequired", "false")
//                    .config("spark.kryoserializer.buffer.max", "512m")
//                    .config("spark.kryo.unsafe", "true")

                    // AWS S3
                    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
                    .config("spark.hadoop.fs.s3a.endpoint", "s3." + awsRegion + ".amazonaws.com")
                    .config("spark.hadoop.fs.s3a.path.style.access", "false")

                    // Performance
                    .config("spark.sql.adaptive.enabled", "true")
                    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                    .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "134217728") // 128MB

                    .getOrCreate();

            testGlueConnection(spark);

            logger.info("SparkSession initialized successfully!");
            return spark;

        } catch (Exception e) {
            logger.error("Error initializing SparkSession", e);
            throw new RuntimeException("Failed to initialize SparkSession: " + e.getMessage(), e);
        }
    }

    private void testGlueConnection(SparkSession spark) {
        try {
            logger.info("Testing connection to Glue Catalog...");

            spark.sql("SHOW DATABASES IN " + catalogName).show();

            logger.info("Connection to Glue Catalog successful!");

        } catch (Exception e) {
            logger.warn("Warning: Could not verify databases in Glue Catalog: {}", e.getMessage());
            throw new RuntimeException("Error connecting to Glue Catalog: " + e.getMessage(), e);
        }
    }
}