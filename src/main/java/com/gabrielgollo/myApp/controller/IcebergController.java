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
    public ResponseEntity<Map<String, Object>> scanAll() {
        try {
            List<Map<String, Object>> results = icebergService.scanAll();

            Map<String, Object> response = Map.of("results", results);

            return ResponseEntity.ok(response);
        } catch (Exception e) {
            Map<String, Object> response = Map.of("message", "Error while scanning data: " + e.getMessage());
            return ResponseEntity.status(500).body(response);
        }
    }

    @PostMapping("/")
    public ResponseEntity<Map<String, Object>> insert(
            @RequestBody Map<String, Object> data) {
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
            @RequestBody List<Map<String, Object>> data) {
        try {
            icebergService.insertBatch(data);
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
            @RequestParam String value) {
        try {
            List<Map<String, Object>> results = icebergService.findByFieldAndValue(field, value);
            Map<String, Object> response = Map.of("results", results);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            return ResponseEntity.status(500).body(null);
        }
    }
}
