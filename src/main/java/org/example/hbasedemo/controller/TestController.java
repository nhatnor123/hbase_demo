package org.example.hbasedemo.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.hbasedemo.service.HbaseService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1")
public class TestController {

    private final HbaseService hbaseService;

    @GetMapping("/test")
    public void put() {
        String tableName = "Table1";
        String rowKey = "row_key_1";
        String family = "Family1";
        String qualifier = "qualifier_1";
        String data = "data test ahihi";

        hbaseService.put(tableName, rowKey, family, qualifier, data);
        log.info("get: {}", hbaseService.get(tableName, rowKey, family, qualifier));
        log.info("scan: {}", hbaseService.scan(tableName, rowKey, family, qualifier));
        hbaseService.delete(tableName, rowKey, family, qualifier);
    }
}
