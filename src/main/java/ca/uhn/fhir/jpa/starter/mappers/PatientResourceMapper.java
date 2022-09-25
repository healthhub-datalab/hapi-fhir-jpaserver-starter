package ca.uhn.fhir.jpa.starter.mappers;

import java.io.IOException;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.List;

import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Patient;
import org.json.simple.JSONObject;
import org.springframework.jdbc.core.JdbcTemplate;
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
  public IBaseResource pullResource(String id, int version, JdbcTemplate template) throws IOException {
    String selectStatement = String.format("SELECT * FROM Patient WHERE id = '%s' AND version = %s", id, version);
    SqlRowSet table = template.queryForRowSet(selectStatement);
    if (!table.next()) {
      // Opimistic locking failed
    }

    String patientID = table.getString("id");
    String patientGender = table.getString("PatientGender");
    String patientDateOfBirth = table.getString("PatientDateOfBirth");
    String patientMaritalStatus = table.getString("PatientMaritalStatus");
    int mappingVersion = table.getInt("version");

    JSONObject jsonObj = new JSONObject();
    jsonObj.put("resourceType", "Patient");
    jsonObj.put("id", patientID);
    jsonObj.put("gender", patientGender.toLowerCase());
    jsonObj.put("birthDate", patientDateOfBirth.substring(0, 10));
    JSONObject maritalObject = new JSONObject();
    maritalObject.put("text", patientMaritalStatus);
    jsonObj.put("maritalStatus", maritalObject);
    JSONObject extensionObject = new JSONObject();
    extensionObject.put("url", "mappingVersion");
    extensionObject.put("valueInteger", mappingVersion);
    jsonObj.put("extension", List.of(extensionObject));

    StringWriter stringWriter = new StringWriter();
    jsonObj.writeJSONString(stringWriter);
    String fhirText = stringWriter.getBuffer().toString();
    return fhirContext.newJsonParser().parseResource(Patient.class, fhirText);
  }

  @Override
  public void pushResource(IBaseResource resource, boolean deleted, JdbcTemplate template) {
    Patient patient = (Patient) resource;
    String id = patient.getIdElement().hasIdPart() ? patient.getIdElement().getIdPart() : null;
    String gender = patient.getGender().getDisplay();
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    String birthDate = format.format(patient.getBirthDate());
    String maritalStatus = patient.getMaritalStatus().getText();
    String version = patient.getExtension().stream()
        .filter(ext -> ext.getUrl().equals("mappingVersion"))
        .map(ext -> ext.getValue().primitiveValue())
        .findFirst().orElse("");
    int ver = 0;
    try {
      ver = Integer.parseInt(version);
    } catch (NumberFormatException e) {
      e.printStackTrace();
    }

    if (!resource.getIdElement().hasIdPart()) {
      /* Newly created resource */
      String insertStatement = "INSERT INTO Patient (PatientGender, PatientDateOfBirth, PatientMaritalStatus, fhir, deleted) VALUES (?, ?, ?, true, false);";
      template.update(insertStatement, gender, birthDate, maritalStatus);
    } else {
      /* Upsert */
      String insertStatement = "INSERT INTO Patient (id, PatientGender, PatientDateOfBirth, PatientMaritalStatus, fhir, deleted) VALUES (?, ?, ?, ?, true, ?)";
      String updateStatement = "UPDATE SET PatientGender = ?, PatientDateOfBirth = ?, PatientMaritalStatus = ?, deleted = ?, fhir = true WHERE Patient.version = ?";
      String upsertStatement = String.format("%s ON CONFLICT (id) DO %s;", insertStatement, updateStatement);
      template.update(upsertStatement, id, gender, birthDate, maritalStatus, deleted, gender, birthDate, maritalStatus, deleted, ver);
    }
  }

}
