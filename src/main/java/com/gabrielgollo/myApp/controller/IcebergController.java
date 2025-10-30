package com.gabrielgollo.myApp.controller;

import com.gabrielgollo.myApp.domain.services.IcebergService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/iceberg")
@Slf4j
public class IcebergController {
    private final IcebergService icebergService;
    public IcebergController(IcebergService icebergService) {
        this.icebergService = icebergService;
    }

    @GetMapping("/")
    public ResponseEntity<Map<String, Object>> scanAll(
            @RequestParam (required = false) String partitionFieldName,
            @RequestParam(required = false) String partitionValue
    ) {
        try {
            List<Map<String, Object>> results;

            if (partitionValue != null && partitionFieldName != null) {
                Map<String, Object> options = Map.of(
                        "partitionFieldName", partitionFieldName,
                        "partitionValue", partitionValue
                );
                results = icebergService.scanAll(options);
            } else {
                results = icebergService.scanAll();
            }

            return ResponseEntity.ok(Map.of("results", results));
        } catch (Exception e) {
            Map<String, Object> response = Map.of("message", "Error while scanning data: " + e.getMessage());
            return ResponseEntity.status(500).body(response);
        }
    }

    @PostMapping("/")
    public ResponseEntity<Map<String, Object>> insert(
            @RequestParam (required = false) String partitionFieldName,
            @RequestParam(required = false) String partitionValue,
            @RequestBody Map<String, Object> data
    ) {
        try {
            icebergService.insert(data);

            Map<String, Object> response = Map.of("message", "Data inserted successfully");
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error(e.getStackTrace().toString()+e.getMessage());

            Map<String, Object> response = Map.of("message", "Error inserting data: " + e.getMessage());
            return ResponseEntity.status(500).body(response);
        }
    }

    @PostMapping("/batch")
    public ResponseEntity<Map<String, Object>> insertBatches(
            @RequestParam (required = false) String partitionFieldName,
            @RequestBody List<Map<String, Object>> data
    ) {
        try {

            if(partitionFieldName != null){
                log.info("Using partition field name: " + partitionFieldName);
                Map<String, Object> options = Map.of("partitionFieldName", "order_date");
                icebergService.insertBatch(data, options);
            } else {
                log.info("No partition field name provided");
                icebergService.insertBatch(data);
            }

            Map<String, Object> response = Map.of("message", "Data inserted successfully");
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error(e.getStackTrace().toString()+e.getMessage());

            Map<String, Object> response = Map.of("message", "Error inserting data: " + e.getMessage());
            return ResponseEntity.status(500).body(response);
        }
    }

    @GetMapping("/filter")
    public ResponseEntity<Map<String, Object>> getByFieldAndValue(
            @RequestParam String field,
            @RequestParam String value,
            @RequestParam (required = false) String partitionFieldName,
            @RequestParam(required = false) String partitionValue
    ) {
        try {
            Map<String, Object> options = null;
            if (partitionValue != null && partitionFieldName != null) {
                options = Map.of(
                        "partitionFieldName", partitionFieldName,
                        "partitionValue", partitionValue
                );
            }

            List<Map<String, Object>> results = icebergService.findByFieldAndValues(field, List.of(value), options);
            Map<String, Object> response = Map.of("results", results);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            return ResponseEntity.status(500).body(null);
        }
    }
}
