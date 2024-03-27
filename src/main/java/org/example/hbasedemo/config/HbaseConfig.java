package org.example.hbasedemo.config;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class HbaseConfig {

    @Bean
    public org.apache.hadoop.conf.Configuration getHbaseConfig() {
        org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();
        config.addResource(new Path("hbase-site.xml"));
        return config;
    }

}
