package com.datawarehouse.hive.config;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;

import com.alibaba.druid.pool.DruidDataSource;
import org.springframework.stereotype.Component;

@Component
@Configuration
public class HiveDruidConfig {
    private String url;
    private String user;
    private String password;
    private String driverClassName;

    @Autowired
    private Environment env;

    @Bean(name = "hiveDruidDataSource")
    @Qualifier("hiveDruidDataSource")
    public DataSource dataSource() {
        DruidDataSource datasource = new DruidDataSource();
        datasource.setUrl(env.getProperty("hive.url"));
        datasource.setUsername(env.getProperty("hive.user"));
        datasource.setPassword(env.getProperty("hive.password"));
        datasource.setDriverClassName(env.getProperty("hive.driver-class-name"));
        return datasource;
    }

    @Bean(name = "hiveDruidTemplate")
    public JdbcTemplate hiveDruidTemplate(@Qualifier("hiveDruidDataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

}