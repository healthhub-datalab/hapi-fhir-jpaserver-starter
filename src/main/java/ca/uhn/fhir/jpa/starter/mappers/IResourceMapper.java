package ca.uhn.fhir.jpa.starter.mappers;

import java.io.IOException;
import java.sql.SQLException;

import org.hl7.fhir.instance.model.api.IBaseResource;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.support.rowset.SqlRowSet;

public interface IResourceMapper {
  public IBaseResource mapToResource(SqlRowSet rs) throws SQLException, IOException;
  public String getResourceName();
  public SqlParameterSource mapToTable(IBaseResource resource);
}
