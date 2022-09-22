package ca.uhn.fhir.jpa.starter.mappers;

import java.io.IOException;

import org.hl7.fhir.instance.model.api.IBaseResource;
import org.springframework.jdbc.core.JdbcTemplate;

public interface IResourceMapper {
  public String getResourceName();
  public IBaseResource pullResource(String id, Long version, JdbcTemplate template) throws IOException;
  public void pushResource(IBaseResource resource, boolean deleted, JdbcTemplate template);
}
