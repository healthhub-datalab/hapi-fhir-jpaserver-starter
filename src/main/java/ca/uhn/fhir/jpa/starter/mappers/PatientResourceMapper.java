package ca.uhn.fhir.jpa.starter.mappers;

import java.io.IOException;
import java.io.StringWriter;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Patient;
import org.json.simple.JSONObject;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.stereotype.Component;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.jpa.api.dao.DaoRegistry;

@Component
public class PatientResourceMapper implements IResourceMapper {
  private final FhirContext fhirContext;

  public PatientResourceMapper(DaoRegistry daoRegistry) {
    this.fhirContext = daoRegistry.getSystemDao().getContext();
  }

  @Override
  public String getResourceName() {
    return "Patient";
  }

  @Override
  public IBaseResource mapToResource(SqlRowSet table) throws SQLException, IOException {
    String patientID = table.getString("id");
    String patientGender = table.getString("PatientGender");
    String patientDateOfBirth = table.getString("PatientDateOfBirth");
    String patientMaritalStatus = table.getString("PatientMaritalStatus");

    JSONObject jsonObj = new JSONObject();
    jsonObj.put("resourceType", "Patient");
    jsonObj.put("id", patientID);
    jsonObj.put("gender", patientGender.toLowerCase());
    jsonObj.put("birthDate", patientDateOfBirth.substring(0, 10));
    JSONObject maritalObject = new JSONObject();
    maritalObject.put("text", patientMaritalStatus);
    jsonObj.put("maritalStatus", maritalObject);
    JSONObject metaObject = new JSONObject();
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    Date lastUpdate = table.getDate("ts");
    metaObject.put("lastUpdated", format.format(lastUpdate));
    jsonObj.put("meta", metaObject);

    StringWriter stringWriter = new StringWriter();
    jsonObj.writeJSONString(stringWriter);
    String fhirText = stringWriter.getBuffer().toString();
    return fhirContext.newJsonParser().parseResource(Patient.class, fhirText);
  }

  @Override
  public SqlParameterSource mapToTable(IBaseResource resource) {
    MapSqlParameterSource namedParameters = new MapSqlParameterSource();
    Patient patient = (Patient) resource;
    String id = patient.getIdElement() == null ? null : patient.getIdElement().getIdPart();
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    String birthDate = format.format(patient.getBirthDate());
    namedParameters.addValue("id", id);
    namedParameters.addValue("PatientGender", patient.getGender().getDisplay());
    namedParameters.addValue("PatientDateOfBirth", birthDate);
    namedParameters.addValue("PatientMaritalStatus", patient.getMaritalStatus().getText());
    namedParameters.addValue("ts", patient.getMeta().getLastUpdated());

    return namedParameters;
  }

}
