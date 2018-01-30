create KEYSPACE IF NOT EXISTS ipagoo WITH REPLICATION ={ 'class' : 'SimpleStrategy', 'replication_factor' : 3 };


DROP TABLE IF EXISTS ipagoo.SubProductReference;
DROP TABLE IF EXISTS ipagoo.Services;
DROP TABLE IF EXISTS ipagoo.Questionarie;
DROP TABLE IF EXISTS ipagoo.Products;
DROP TABLE IF EXISTS ipagoo.ProductReference;
DROP TABLE IF EXISTS ipagoo.PartyQuestionarie;
DROP TABLE IF EXISTS ipagoo.PartyPersonalDetails;
DROP TABLE IF EXISTS ipagoo.PartyNotificationPreferences;
DROP TABLE IF EXISTS ipagoo.PartyNonPersonalDetails;
DROP TABLE IF EXISTS ipagoo.PartyContract;
DROP TABLE IF EXISTS ipagoo.PartyChannel;
DROP TABLE IF EXISTS ipagoo.Party;
DROP TABLE IF EXISTS ipagoo.PartyAddress;
DROP TABLE IF EXISTS ipagoo.PartyIDs;
DROP TABLE IF EXISTS ipagoo.OperationFeeReference;
DROP TABLE IF EXISTS ipagoo.InterPartyRelationship;
DROP TABLE IF EXISTS ipagoo.Instructions;
DROP TABLE IF EXISTS ipagoo.InstructionReference;
DROP TABLE IF EXISTS ipagoo.InformationAssets;
DROP TABLE IF EXISTS ipagoo.InformationAssetType;
DROP TABLE IF EXISTS ipagoo.ExchangeRate;
DROP TABLE IF EXISTS ipagoo.Entitlements;
DROP TABLE IF EXISTS ipagoo.ContractChannel;
DROP TABLE IF EXISTS ipagoo.Contract;
DROP TABLE IF EXISTS ipagoo.ChannelReference;
DROP TABLE IF EXISTS ipagoo.BrandReference;
DROP TABLE IF EXISTS ipagoo.Alerts;
DROP TABLE IF EXISTS ipagoo.AccountTransactionLog;


//***********************************************************
//
// Party
//
//***********************************************************
CREATE TABLE ipagoo.Party (
  PartyId bigint,
  FirstName varchar,
  LastName varchar,
  BusinessName varchar,
  TradingName varchar,
  Title int,
  Salutation varchar,
  IDList text,
  Addresses text,
  Party_InfoAssets text,
  PRIMARY KEY (PartyId),
);
CREATE TABLE ipagoo.PartyAddress (
  PartyId bigint,
  AddressId bigint,
  Address varchar,
  PostalCode varchar,
  City varchar,
  County varchar,
  Country varchar,
  GeoCoodinates varchar,
  AddressType varchar,
  StartDate date,
  PRIMARY KEY (PartyId, AddressId),
);
CREATE TABLE ipagoo.PartyIDs (
  PartyId bigint,
  TIN varchar,
  TINCountry varchar,
  NINO varchar,
  IdCardNumber varchar,
  IdCardCountry varchar,
  GIIN varchar,
  CompanyRegistrationNumber varchar,
  PassportNumber varchar,
  PassportCountry varchar,
  PRIMARY KEY (PartyId),
);
CREATE TABLE ipagoo.PartyNonPersonalDetails (
  PartyId bigint,
  NPPId bigint,
  SICCodes text,
  BusinessRegistrationCountry varchar,
  BusinessRegistrationDate date,
  TradingStartDate date,
  IncorporationDetail text,
  OtherAddresses text,
  Website varchar,
  NPPContactDetails text,
  StockMarket text,
  Turnover text,
  PRIMARY KEY (PartyId, NPPId)
);
CREATE TABLE ipagoo.PartyNotificationPreferences (
  PartyId bigint,
  PartyNotificationPreferencesId bigint,
  NotificationChannelId bigint,
  Preferences text,
  PRIMARY KEY (PartyId, PartyNotificationPreferencesId)
);
CREATE TABLE ipagoo.PartyPersonalDetails (
  PartyId bigint,
  PPId bigint,
  EmploymentDetails text,
  Citizenships text,
  TaxResidency text,
  Email text,
  Telephone text,
  StaffIndicator tinyint,
  Staff text,
  Gender int,
  DateOfBirth date,
  PRIMARY KEY (PartyId, PPId)
);
//***********************************************************
//
// Channel Reference
//
//***********************************************************
CREATE TABLE ipagoo.ChannelReference (
  ChannelId bigint,
  ChannelDescription varchar,
  PRIMARY KEY (ChannelId),
);
CREATE TABLE ipagoo.PartyChannel (
  PartyId bigint,
  ChannelId bigint,
  Role text,
  PRIMARY KEY (PartyId, ChannelId)
);
//***********************************************************
//
// Product Catalog Reference
//
//***********************************************************
CREATE TABLE ipagoo.BrandReference (
  BrandProductTypeId bigint,
  Features text,
  Operation text,
  PRIMARY KEY (BrandProductTypeId)
);
CREATE TABLE ipagoo.ProductReference (
  ProductTypeId bigint,
  ProductFamily varchar,
  ProductFeatures text,
  ProductOperations text,
  MLE varchar,
  SLE varchar,
  PRIMARY KEY (ProductTypeId)
);
CREATE TABLE ipagoo.SubProductReference (
  SubProductTypeId bigint,
  ProductFeatures text,
  ProductOperations text,
  PRIMARY KEY (SubProductTypeId)
);
//***********************************************************
//
// Products / Services
//
//***********************************************************
CREATE TABLE ipagoo.Products (
  ProductId bigint,
  PartyId bigint,
  BrandProductTypeId bigint,
  ProductTypeId bigint,
  SubProductTypeId bigint,
  PPHR varchar,
  ContractId bigint,
  MLE varchar,
  SLE varchar,
  ProductFamily varchar,
  ProductFeatures text,
  ProductOperations text,
  ProductAddress text,
  PRIMARY KEY (ProductId)
);
CREATE TABLE ipagoo.Services (
  ServiceId bigint,
  PartyId bigint,
  ContractId bigint,
  PSHR varchar,
  ServiceFeatures text,
  ServiceOperations text,
  PRIMARY KEY (ServiceId)
);
//***********************************************************
//
// Contract
//
//***********************************************************
CREATE TABLE ipagoo.Contract (
  ContractId bigint,
  ProductIds text,
  ServiceIds text,
  ContractInfoAssets text,
  ContractOperations text,
  Channels text,
  PRIMARY KEY (ContractId)
);
CREATE TABLE ipagoo.PartyContract (
  PartyContractId bigint,
  PartyId bigint,
  ContractId bigint,
  Role text,
  ContractFeatures text,
  Entitlements text,
  PRIMARY KEY (PartyContractId)
);
//***********************************************************
//
// Exchange Rates
//
//***********************************************************
CREATE TABLE ipagoo.ExchangeRate (
  ExchageRateId bigint,
  CurrencyId1 varchar,
  CurrencySymbol1 varchar,
  CurrencyId2 varchar,
  CurrencySymbol2 varchar,
  SalePrice double,
  PurchasePrice double,
  CreatedTime timestamp,
  UpdatedTime timestamp,
  PRIMARY KEY (ExchageRateId)
);
//***********************************************************
//
// Information Assets
//
//***********************************************************
CREATE TABLE ipagoo.InformationAssetType (
  InformationAssetTypeId bigint,
  Description varchar,
  PRIMARY KEY (InformationAssetTypeId)
);
CREATE TABLE ipagoo.InformationAssets (
  InformationAssetId bigint,
  InformationAssetTypeId bigint,
  AssetText varchar,
  AssetTags text,
  PRIMARY KEY (InformationAssetId)
);
//***********************************************************
//
// Questionarie
//
//***********************************************************
CREATE TABLE ipagoo.Questionarie (
  QuestionarieTypeId bigint,
  Form text,
  PRIMARY KEY (QuestionarieTypeId)
);
CREATE TABLE ipagoo.PartyQuestionarie (
  PartyId bigint,
  PartyQuestionarieId bigint,
  QuestionarieTypeId bigint,
  Questionarie text,
  PRIMARY KEY (PartyId, PartyQuestionarieId)
);
//***********************************************************
//
// Account Identification
//
//***********************************************************
CREATE TABLE ipagoo.IBANAccount (
  IBAN varchar,
  InternalAccountId bigint,
  PRIMARY KEY (IBAN)
);
CREATE TABLE ipagoo.SortCodeAccount (
  SortCodeAccount varchar,
  InternalAccountId bigint,
  PRIMARY KEY (SortCodeAccount)
);
CREATE TABLE ipagoo.AccountIBAN (
  InternalAccountId bigint,
  IBAN varchar,
  PRIMARY KEY (InternalAccountId)
);
//***********************************************************
//
// Account Transactional Log
//
//***********************************************************
CREATE TABLE ipagoo.AccountTransactionLog (
  AccountId varchar,
  ActualBalance double,
  LedgerBalance double,
  LastTransactionId varchar,
  TransactionLogId varchar,
  TransactionStoreAction varchar,
  TransactionType varchar,
  TransactionCode varchar,
  TransactionDescription varchar,
  TransactionTimestamp timestamp,
  TransactionTimenanos time,
  TransactionTimezone varchar,
  TransactionAmount double ,
  TransactionCurrency varchar,
  ExchangeRate double,
  ChequeNumber varchar,
  TellerId varchar,
  CashFlag varchar,
  PRIMARY KEY (AccountId, TransactionTimestamp, TransactionLogId)
);

INSERT INTO AccountTransactionLog (AccountId, TransactionTimestamp, TransactionLogId, ActualBalance)
 VALUES ('1234IPAGO', '2017-10-06 13:30:54.234+0000', '1', 1);
INSERT INTO AccountTransactionLog (AccountId, TransactionTimestamp, TransactionLogId, ActualBalance)
 VALUES ('1234IPAGO', '2017-10-12 00:01:00.000+0000', '1-1507722392350-1000004', 0.1);
INSERT INTO AccountTransactionLog (AccountId, TransactionTimestamp, TransactionLogId, ActualBalance)
 VALUES ('1234IPAGO', '2017-10-12 00:01:00.000+0000', '1-1507722392350-0999951', -666);
INSERT INTO AccountTransactionLog (AccountId, TransactionTimestamp, TransactionLogId, ActualBalance)
 VALUES ('1234IPAGO', '2017-10-12 00:00:59.000+0000', '1', -2000);
INSERT INTO AccountTransactionLog (AccountId, TransactionTimestamp, TransactionLogId, ActualBalance)
 VALUES ('1234IPAGO', '2017-10-05 13:30:54.234+0000', '1', 0);
INSERT INTO AccountTransactionLog (AccountId, TransactionTimestamp, TransactionLogId, ActualBalance)
 VALUES ('1234IPAGO', '2017-10-07 14:59:54.234+0000', '1', 3);
INSERT INTO AccountTransactionLog (AccountId, TransactionTimestamp, TransactionLogId, ActualBalance)
 VALUES ('1234IPAGO', '2017-10-07 13:30:54.234+0000', '1', 2);