package ca.uhn.fhir.jpa.starter.interceptors;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.hl7.fhir.instance.model.api.IBaseResource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.PreparedStatementCallback;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.stereotype.Component;

import ca.uhn.fhir.interceptor.api.Hook;
import ca.uhn.fhir.interceptor.api.Interceptor;
import ca.uhn.fhir.interceptor.api.Pointcut;
import ca.uhn.fhir.jpa.starter.mappers.IResourceMapper;
import ca.uhn.fhir.jpa.starter.mappers.ResourceMapperRegistry;
import ca.uhn.fhir.rest.api.server.RequestDetails;

@Component
@Interceptor
public class PushDataInterceptor {
  private NamedParameterJdbcTemplate jdbcTemplate;
  private ResourceMapperRegistry mapperRegistry;

  public PushDataInterceptor(@Value("${mappingtable.jdbc}") String connectionString,
      @Value("${mappingtable.username}") String username, @Value("${mappingtable.password}") String password,
      @Value("${mappingtable.driver}") String driver, ResourceMapperRegistry mapperRegistry) {
    DriverManagerDataSource dataSource = new DriverManagerDataSource();
    dataSource.setUrl(connectionString);
    dataSource.setUsername(username);
    dataSource.setPassword(password);
    dataSource.setDriverClassName(driver);
    this.jdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
    this.mapperRegistry = mapperRegistry;
  }

  private String insertQuery(String resourceName, List<String> columns, boolean deleted) {
    List<String> columnValue = columns.stream().map(name -> String.format(":%s", name)).collect(Collectors.toList());
    return String.format("INSERT INTO %s (%s, fhir, deleted) VALUES (%s, true, %s)", resourceName,
        String.join(",", columns), String.join(",", columnValue), deleted);
  }

  private String upsertQuery(String resourceName, List<String> columns, boolean deleted) {
    String insertQuery = insertQuery(resourceName, columns, deleted);
    String setter = String.join(",",
        columns.stream().map(param -> String.format("%s = :%s", param, param)).collect(Collectors.toList()));
    String updateQuery = String.format("UPDATE SET %s, deleted = %s, fhir = true WHERE Patient.ts = :ts", setter,
        deleted);
    return String.format("%s ON CONFLICT (id) DO %s", insertQuery, updateQuery);
  }

  private void execute(String query, SqlParameterSource parameters) {
    jdbcTemplate.execute(query, parameters, new PreparedStatementCallback() {
      @Override
      public Object doInPreparedStatement(PreparedStatement ps)
          throws SQLException, DataAccessException {
        return ps.executeUpdate();
      }
    });
  }

  @Hook(Pointcut.STORAGE_PRESTORAGE_RESOURCE_CREATED)
  public void pushCreateData(IBaseResource resource, RequestDetails details) {
    String resourceName = details.getResourceName();
    /* Push to mapping table */
    IResourceMapper mapper = mapperRegistry.getMapper(resourceName);
    SqlParameterSource parameters = mapper.mapToTable(resource);
    if (!resource.getIdElement().hasIdPart()) {
      List<String> columnNameWithoutId = Arrays.stream(parameters.getParameterNames())
          .filter(param -> !param.equals("id")).collect(Collectors.toList());
      execute(insertQuery(resourceName, columnNameWithoutId, false), parameters);
    } else {
      execute(upsertQuery(resourceName, List.of(parameters.getParameterNames()), false), parameters);
    }
    throw new AbortDatabaseOperationException();
  }

  @Hook(Pointcut.STORAGE_PRESTORAGE_RESOURCE_UPDATED)
  public void pushUpdateData(IBaseResource resource, RequestDetails details) {
    String resourceName = details.getResourceName();
    /* Push to mapping table */
    IResourceMapper mapper = mapperRegistry.getMapper(resourceName);
    SqlParameterSource parameters = mapper.mapToTable(resource);
    execute(upsertQuery(resourceName, List.of(parameters.getParameterNames()), false), parameters);
    throw new AbortDatabaseOperationException();
  }

  @Hook(Pointcut.STORAGE_PRESTORAGE_RESOURCE_DELETED)
  public void pushDeleteData(IBaseResource resource, RequestDetails details) {
    String resourceName = details.getResourceName();
    /* Push to mapping table */
    IResourceMapper mapper = mapperRegistry.getMapper(resourceName);
    SqlParameterSource parameters = mapper.mapToTable(resource);
    execute(upsertQuery(resourceName, List.of(parameters.getParameterNames()), true), parameters);
    throw new AbortDatabaseOperationException();
  }
}
