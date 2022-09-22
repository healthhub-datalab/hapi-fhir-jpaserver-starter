package ca.uhn.fhir.jpa.starter.mappers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.hl7.fhir.instance.model.api.IBaseResource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.util.Pair;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.stereotype.Component;

@Component
public class MappingTableManager {
  private ResourceMapperRegistry registry;
  private JdbcTemplate jdbcTemplate;

  public MappingTableManager(@Value("${mappingtable.jdbc}") String connectionString,
  @Value("${mappingtable.username}") String username, @Value("${mappingtable.password}") String password,
  @Value("${mappingtable.driver}") String driver, ResourceMapperRegistry registry) {
    DriverManagerDataSource dataSource = new DriverManagerDataSource();
    dataSource.setUrl(connectionString);
    dataSource.setUsername(username);
    dataSource.setPassword(password);
    dataSource.setDriverClassName(driver);
    this.jdbcTemplate = new JdbcTemplate(dataSource);
    this.registry = registry;
  }

  public List<Pair<IBaseResource, Boolean>> pullResource(String resourceName) throws IOException {
    String selectStatement = String.format("SELECT id, deleted, version FROM %s WHERE emr = true;", resourceName);
    SqlRowSet result = jdbcTemplate.queryForRowSet(selectStatement);
    List<Pair<IBaseResource, Boolean>> ret = new ArrayList<>();
    while (result.next()) {
      String id = result.getString("id");
      Long version = result.getLong("version");
      Boolean deleted = result.getBoolean("deleted");

      IResourceMapper mapper = registry.getMapper(resourceName);
      IBaseResource resource = mapper.pullResource(id, version, jdbcTemplate);
      ret.add(Pair.of(resource, deleted));

      String unsetDirtyStatement = String.format("UPDATE %s SET emr = false WHERE id = ? AND version = ?", resourceName);
      jdbcTemplate.update(unsetDirtyStatement, id, version);
    }
    return ret;
  }

  public void deleteClean(String resourceName) {
    String deleteStatement = String.format("DELETE FROM %s WHERE emr = false AND fhir = false", resourceName);
    jdbcTemplate.update(deleteStatement);
  }

  public void pushResource(String resourceName, IBaseResource resource, boolean deleted) {
    IResourceMapper mapper = registry.getMapper(resourceName);
    mapper.pushResource(resource, deleted, jdbcTemplate);
  }
}
