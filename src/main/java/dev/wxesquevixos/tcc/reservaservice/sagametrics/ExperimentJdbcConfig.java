package dev.wxesquevixos.tcc.reservaservice.sagametrics;
import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

@Configuration
public class ExperimentJdbcConfig {

    @Bean
    public DataSource experimentDataSource(
            @Value("${experiment.datasource.url}") String url,
            @Value("${experiment.datasource.username}") String username,
            @Value("${experiment.datasource.password}") String password
    ) {
        DriverManagerDataSource ds = new DriverManagerDataSource();
        ds.setUrl(url);
        ds.setUsername(username);
        ds.setPassword(password);
        return ds;
    }

    @Bean
    public NamedParameterJdbcTemplate experimentJdbcTemplate(DataSource experimentDataSource) {
        return new NamedParameterJdbcTemplate(experimentDataSource);
    }
}