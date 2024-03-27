package org.example.hbasedemo.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class HbaseService {
    private final Configuration hbaseConfiguration;

    public void put(String tableName, String rowKey, String family, String qualifier, String data) {
        try {
            try (Connection connection = ConnectionFactory.createConnection(hbaseConfiguration)) {
                Table table = connection.getTable(TableName.valueOf(tableName));
                Put put = new Put(Bytes.toBytes(rowKey));
                put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(data));
                table.put(put);
            }
        } catch (Exception e) {
            log.error("HbaseService put, got error", e);
        }
    }

    public String get(String tableName, String rowKey, String family, String qualifier) {
        try {
            try (Connection connection = ConnectionFactory.createConnection(hbaseConfiguration)) {
                Table table = connection.getTable(TableName.valueOf(tableName));
                Result getResult = table.get(new Get(Bytes.toBytes(rowKey)));
                return Bytes.toString(getResult.getValue(Bytes.toBytes(family), Bytes.toBytes(qualifier)));
            }
        } catch (Exception e) {
            log.error("HbaseService get, got error", e);
            return null;
        }
    }

    public void delete(String tableName, String rowKey, String family, String qualifier) {
        try {
            try (Connection connection = ConnectionFactory.createConnection(hbaseConfiguration)) {
                Table table = connection.getTable(TableName.valueOf(tableName));
                Delete delete = new Delete(Bytes.toBytes(rowKey));
                delete.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
                table.delete(delete);
            }
        } catch (Exception e) {
            log.error("HbaseService delete, got error", e);
        }
    }

    public List<String> scan(String tableName, String rowPrefix, String family, String qualifier) {
        try {
            try (Connection connection = ConnectionFactory.createConnection(hbaseConfiguration)) {
                Table table = connection.getTable(TableName.valueOf(tableName));

                Filter filter1 = new PrefixFilter(Bytes.toBytes(rowPrefix));

                Scan scan = new Scan();
                scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
                scan.setFilter(filter1);

                ResultScanner scanner = table.getScanner(scan);
                List<String> response = new ArrayList<>();
                for (Result result : scanner) {
                    response.add(Bytes.toString((result.getValue(Bytes.toBytes(family), Bytes.toBytes(qualifier)))));
                }
                return response;
            }
        } catch (Exception e) {
            log.error("HbaseService scan, got error", e);
            return new ArrayList<>();
        }
    }


}
