CREATE TABLE Patient (
  id VARCHAR(50),
  PatientGender VARCHAR(50),
  PatientDateOfBirth VARCHAR(50),
  PatientRace VARCHAR(50),
  PatientMaritalStatus VARCHAR(50),
  PatientLanguage VARCHAR(50),
  PatientPopulationPercentageBelowPoverty decimal,
  deleted BOOLEAN DEFAULT false,
  emr BOOLEAN DEFAULT false,
  fhir BOOLEAN DEFAULT false,
	version SERIAL
);

ALTER SEQUENCE public.patient_version_seq CYCLE;

CREATE OR REPLACE FUNCTION update_modified_column()
RETURNS TRIGGER AS $$
BEGIN                                          
  if (NEW.emr = true) OR
      (NEW.fhir = true) then
    NEW.version := nextval(pg_get_serial_sequence('public.patient', 'version'));
  end if;
  RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_modtime BEFORE UPDATE ON Patient FOR EACH ROW EXECUTE PROCEDURE  update_modified_column();

CREATE UNIQUE INDEX idx_patient_id ON Patient(id);
