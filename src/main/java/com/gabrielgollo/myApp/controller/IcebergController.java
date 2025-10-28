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

    @GetMapping("/orders")
    public ResponseEntity<List<Map<String, Object>>> scanAll() {
        try {
            List<Map<String, Object>> results = icebergService.scanAll();
            return ResponseEntity.ok(results);
        } catch (Exception e) {
            return ResponseEntity.status(500).body(null);
        }
    }

    @PostMapping("/orders")
    public ResponseEntity<String> insert(
            @RequestBody Map<String, Object> data) {
        try {
            icebergService.insert(data);
            return ResponseEntity.ok("Data inserted successfully");
        } catch (Exception e) {
            log.error(e.getStackTrace().toString()+e.getMessage());
            return ResponseEntity.status(500).body("Error inserting data: " + e);
        }
    }

    @PostMapping("/orders/batch")
    public ResponseEntity<String> insertBatches(
            @RequestBody List<Map<String, Object>> data) {
        try {
            icebergService.insertBatch(data);
            return ResponseEntity.ok("Data inserted successfully");
        } catch (Exception e) {
            log.error(e.getStackTrace().toString()+e.getMessage());
            return ResponseEntity.status(500).body("Error inserting data: " + e);
        }
    }

    @GetMapping("/orders/filter")
    public ResponseEntity<List<Map<String, Object>>> getByFieldAndValue(
            @RequestParam String field,
            @RequestParam String value) {
        try {
            List<Map<String, Object>> results = icebergService.findByFieldAndValue(field, value);
            return ResponseEntity.ok(results);
        } catch (Exception e) {
            return ResponseEntity.status(500).body(null);
        }
    }
}
