package ca.uhn.fhir.jpa.starter.interceptors;

import java.sql.Timestamp;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.sql.DataSource;

import org.hl7.fhir.instance.model.api.IBaseResource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpMethod;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.stereotype.Component;

import ca.uhn.fhir.interceptor.api.Hook;
import ca.uhn.fhir.interceptor.api.Interceptor;
import ca.uhn.fhir.interceptor.api.Pointcut;
import ca.uhn.fhir.jpa.api.dao.DaoRegistry;
import ca.uhn.fhir.jpa.api.dao.IFhirResourceDao;
import ca.uhn.fhir.jpa.starter.mappers.IResourceMapper;
import ca.uhn.fhir.jpa.starter.mappers.ResourceMapperRegistry;

@Component
@Interceptor
public class PullDataInterceptor {
  private JdbcTemplate jdbcTemplate;
  private DaoRegistry daoRegistry;
  private ResourceMapperRegistry mapperRegistry;

  public PullDataInterceptor(@Value("${mappingtable.jdbc}") String connectionString,
      @Value("${mappingtable.username}") String username, @Value("${mappingtable.password}") String password,
      @Value("${mappingtable.driver}") String driver, DaoRegistry daoRegistry, ResourceMapperRegistry mapperRegistry) {
    DriverManagerDataSource dataSource = new DriverManagerDataSource();
    dataSource.setUrl(connectionString);
    dataSource.setUsername(username);
    dataSource.setPassword(password);
    dataSource.setDriverClassName(driver);
    this.jdbcTemplate = new JdbcTemplate(dataSource);
    this.daoRegistry = daoRegistry;
    this.mapperRegistry = mapperRegistry;
  }

  @Hook(Pointcut.SERVER_INCOMING_REQUEST_PRE_PROCESSED)
  public boolean pullDataBeforeRequest(HttpServletRequest req, HttpServletResponse resp) {
    String resourceName = req.getRequestURI().split("/")[2];


    try {
      /* 1 Pull mapping table */
      String selectSql = String.format("SELECT * FROM %s WHERE emr = true;", resourceName);
      SqlRowSet result = jdbcTemplate.queryForRowSet(selectSql);

      /* 2 Update Fhir Database using DAO and Mapper */
      IResourceMapper mapper = mapperRegistry.getMapper(resourceName);
      IFhirResourceDao<IBaseResource> dao = daoRegistry.getResourceDao(resourceName);
      Timestamp latest = null; /* Remember latest timestamp reflected */
      while (result.next()) {
        Timestamp timestamp = result.getTimestamp("ts");
        if (latest == null || latest.before(timestamp)) {
          latest = timestamp;
        }

        boolean deleteFlag = result.getBoolean("deleted");
        IBaseResource resource = mapper.mapToResource(result);
        if (deleteFlag) {
          dao.delete(resource.getIdElement());
        } else {
          dao.update(resource);
        }
      }

      if (req.getMethod().equals(HttpMethod.GET.toString())) {
        /* 3 Delete Mapping table */
        String deleteSql = String.format("DELETE FROM %s WHERE ts <= ? AND emr = true AND fhir = false", resourceName);
        jdbcTemplate.update(deleteSql, latest);
      }
      return true;
    } catch (Exception e) {
      e.printStackTrace();
      return false;
    }
  }
}
