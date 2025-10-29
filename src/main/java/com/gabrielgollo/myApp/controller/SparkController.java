package com.gabrielgollo.myApp.controller;

import com.gabrielgollo.myApp.domain.services.SparkService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Collections;
import java.util.List;
import java.util.Map;

@RestController
@Profile("!local")
@RequestMapping("/api/spark/iceberg")
@Slf4j
public class SparkController {

    private final SparkService sparkService;

    public SparkController(SparkService sparkService) {
        this.sparkService = sparkService;
    }

    @GetMapping("/")
    public ResponseEntity<Map<String, Object>> query() {
        try {
            List<Map<String, Object>> results = sparkService.queryTable();

            Map<String, Object> response = Map.of("results", results);
            return ResponseEntity.ok(response);
        } catch (Exception e) {

            return ResponseEntity.status(500).body(Map.of(
                    "message", "Error while querying data: " + e.getMessage()
            ));
        }
    }

    @PostMapping("/")
    public ResponseEntity<Map<String, Object>> insert(
            @RequestBody Map<String, Object> data) {
        try {
            sparkService.insertData(data);

            return ResponseEntity.ok(Map.of(
                    "message", "Data inserted successfully"
            ));
        } catch (Exception e) {
            log.error(e.getStackTrace().toString() + e.getMessage());
            return ResponseEntity.status(500).body(Map.of(
                    "message", "Error inserting data: " + e.getMessage()
            ));
        }
    }
}