
qry00a_check_null_epens <- "SELECT COUNT (*) AS n_null_epens FROM STP_Credential_2019
  WHERE STP_Credential_2019.ENCRYPTED_TRUE_PEN =''"

qry00b_check_unique_epens <- "
  SELECT COUNT (DISTINCT ENCRYPTED_TRUE_PEN) AS n_null_epens
  FROM STP_Credential_2019"

qry00c_CreateIDinSTPCredential <- "
  ALTER TABLE [STP_Credential_2019] 
  ADD ID2 INT"

qry00d_SetPKeyinSTPCredential <- "
  ALTER TABLE [STP_Credential_2019] 
  ADD CONSTRAINT STP_Credential_PK_ID PRIMARY KEY (ID)"

qry01_ExtractAllID_into_STP_Credential_Record_Type <- "
  CREATE TABLE [STP_Credential_Record_Type] (
  [ID] int NOT NULL,
  [ENCRYPTED_TRUE_PEN] varchar(50),
  [RecordStatus] smallint,
  [MinEnrolment] smallint,
  [FirstEnrolment] smallint);
  
  INSERT INTO STP_Credential_Record_Type (ID, ENCRYPTED_TRUE_PEN)
  SELECT STP_Credential_2019.ID, STP_Credential_2019.ENCRYPTED_TRUE_PEN
  FROM STP_Credential_2019;"
