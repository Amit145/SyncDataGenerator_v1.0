CREATE DATABASE "Model_1";

CREATE SCHEMA "";

CREATE TABLE ""."Hub Account"(
	"Account Hash Key" "CHAR"(18) NOT NULL, 
	"Load Date" "CHAR"(18) NULL, 
	"Record Source" "CHAR"(18) NULL, 
	"Account Id" "CHAR"(18) NULL, 
	CONSTRAINT "XPKHub Account" PRIMARY KEY ("Account Hash Key")
);

CREATE UNIQUE INDEX ""."XPKHub Account" ON ""."Hub Account"("Account Hash Key" ASC);

CREATE TABLE ""."Hub Consent"(
	"Consent Hash Key" "CHAR"(18) NOT NULL, 
	"Load Date" "CHAR"(18) NULL, 
	"Record Source" "CHAR"(18) NULL, 
	"Consent Id" "CHAR"(18) NULL, 
	CONSTRAINT "XPKHub Consent" PRIMARY KEY ("Consent Hash Key")
);

CREATE UNIQUE INDEX ""."XPKHub Consent" ON ""."Hub Consent"("Consent Hash Key" ASC);

CREATE TABLE ""."Hub Contact"(
	"Contact Hash Key" "CHAR"(18) NOT NULL, 
	"Load Date" "CHAR"(18) NULL, 
	"Record Source" "CHAR"(18) NULL, 
	"Contact Id" "CHAR"(18) NULL, 
	CONSTRAINT "XPKHub Contact" PRIMARY KEY ("Contact Hash Key")
);

CREATE UNIQUE INDEX ""."XPKHub Contact" ON ""."Hub Contact"("Contact Hash Key" ASC);

CREATE TABLE ""."Hub Customer"(
	"Customer Hash Key" "CHAR"(18) NOT NULL, 
	"Load Date" "CHAR"(18) NULL, 
	"Record Source" "CHAR"(18) NULL, 
	"Customer Id" "CHAR"(18) NULL, 
	CONSTRAINT "XPKHub Customer" PRIMARY KEY ("Customer Hash Key")
);

CREATE UNIQUE INDEX ""."XPKHub Customer" ON ""."Hub Customer"("Customer Hash Key" ASC);

CREATE TABLE ""."Hub Home"(
	"Home Hash Key" "CHAR"(18) NOT NULL, 
	"Load Date" "CHAR"(18) NULL, 
	"Record Source" "CHAR"(18) NULL, 
	"Home Id" "CHAR"(18) NULL, 
	CONSTRAINT "XPKHub Home" PRIMARY KEY ("Home Hash Key")
);

CREATE UNIQUE INDEX ""."XPKHub Home" ON ""."Hub Home"("Home Hash Key" ASC);

CREATE TABLE ""."Hub Home Address"(
	"Home Address Hash Key" "CHAR"(18) NOT NULL, 
	"Load Date" "CHAR"(18) NULL, 
	"Record Source" "CHAR"(18) NULL, 
	"Home Address Id" "CHAR"(18) NULL, 
	CONSTRAINT "XPKHub Home Address" PRIMARY KEY ("Home Address Hash Key")
);

CREATE UNIQUE INDEX ""."XPKHub Home Address" ON ""."Hub Home Address"("Home Address Hash Key" ASC);

CREATE TABLE ""."Hub Identities"(
	"Identities Hash Key" "CHAR"(18) NOT NULL, 
	"Load Date" "CHAR"(18) NULL, 
	"Record Source" "CHAR"(18) NULL, 
	"Identities Id" "CHAR"(18) NULL, 
	CONSTRAINT "XPKHub Identities" PRIMARY KEY ("Identities Hash Key")
);

CREATE UNIQUE INDEX ""."XPKHub Identities" ON ""."Hub Identities"("Identities Hash Key" ASC);

CREATE TABLE ""."Hub Lead"(
	"Lead Hash Key" "CHAR"(18) NOT NULL, 
	"Load Date" "CHAR"(18) NULL, 
	"Record Source" "CHAR"(18) NULL, 
	"Lead Id" "CHAR"(18) NULL, 
	CONSTRAINT "XPKHub Lead" PRIMARY KEY ("Lead Hash Key")
);

CREATE UNIQUE INDEX ""."XPKHub Lead" ON ""."Hub Lead"("Lead Hash Key" ASC);

CREATE TABLE ""."Hub Legal Person"(
	"Legal Person Hash Key" "CHAR"(18) NOT NULL, 
	"Load Date" "CHAR"(18) NULL, 
	"Record Source" "CHAR"(18) NULL, 
	"Legal Person Id" "CHAR"(18) NULL, 
	CONSTRAINT "XPKHub Legal Person" PRIMARY KEY ("Legal Person Hash Key")
);

CREATE UNIQUE INDEX ""."XPKHub Legal Person" ON ""."Hub Legal Person"("Legal Person Hash Key" ASC);

CREATE TABLE ""."Hub Marketing Engagement"(
	"Marketing Engagement Hash Key" "CHAR"(18) NOT NULL, 
	"Load Date" "CHAR"(18) NULL, 
	"Record Source" "CHAR"(18) NULL, 
	"Marketing Engagement Id" "CHAR"(18) NULL,
	CONSTRAINT "XPKHub Marketing Engagement" PRIMARY KEY ("Marketing Engagement Hash Key")
);

CREATE UNIQUE INDEX ""."XPKHub Marketing Engagement" ON ""."Hub Marketing Engagement"("Marketing Engagement Hash Key" ASC);

CREATE TABLE ""."Hub Marketing Preference"(
	"Marketing Preference Hash Key" "CHAR"(18) NOT NULL, 
	"Load Date" "CHAR"(18) NULL, 
	"Record Source" "CHAR"(18) NULL, 
	"Marketing Preference Id" "CHAR"(18) NULL, 
	CONSTRAINT "XPKHub Marketing Preference" PRIMARY KEY ("Marketing Preference Hash Key")
);

CREATE UNIQUE INDEX ""."XPKHub Marketing Preference" ON ""."Hub Marketing Preference"("Marketing Preference Hash Key" ASC);

CREATE TABLE ""."Hub Motor"(
	"Motor Hash Key" "CHAR"(18) NOT NULL, 
	"Load Date" "CHAR"(18) NULL, 
	"Record Source" "CHAR"(18) NULL, 
	"Motor Id" "CHAR"(18) NULL, 
	CONSTRAINT "XPKHub Motor" PRIMARY KEY ("Motor Hash Key")
);

CREATE UNIQUE INDEX ""."XPKHub Motor" ON ""."Hub Motor"("Motor Hash Key" ASC);

CREATE TABLE ""."Hub Natural Person"(
	"Natural Person Hash Key" "CHAR"(18) NOT NULL, 
	"Load Date" "CHAR"(18) NULL, 
	"Record Source" "CHAR"(18) NULL, 
	"Natural Person Id" "CHAR"(18) NULL, 
	CONSTRAINT "XPKHub Natural Person" PRIMARY KEY ("Natural Person Hash Key")
);

CREATE UNIQUE INDEX ""."XPKHub Natural Person" ON ""."Hub Natural Person"("Natural Person Hash Key" ASC);

CREATE TABLE ""."Hub Person"(
	"Person Hash Key" "CHAR"(18) NOT NULL, 
	"Load Date" "CHAR"(18) NULL, 
	"Record Source" "CHAR"(18) NULL, 
	"Person Id" "CHAR"(18) NULL, 
	CONSTRAINT "XPKHub Person" PRIMARY KEY ("Person Hash Key")
);

CREATE UNIQUE INDEX ""."XPKHub Person" ON ""."Hub Person"("Person Hash Key" ASC);

CREATE TABLE ""."Hub Policy"(
	"Policy Hash Key" "CHAR"(18) NOT NULL, 
	"Load Date" "CHAR"(18) NULL, 
	"Record Source" "CHAR"(18) NULL, 
	"Policy Id" "CHAR"(18) NULL, 
	CONSTRAINT "XPKHub Policy" PRIMARY KEY ("Policy Hash Key")
);

CREATE UNIQUE INDEX ""."XPKHub Policy" ON ""."Hub Policy"("Policy Hash Key" ASC);

CREATE TABLE ""."Hub Product"(
	"Product Hash Key" "CHAR"(18) NOT NULL, 
	"Load Date" "CHAR"(18) NULL, 
	"Record Source" "CHAR"(18) NULL, 
	"Product Id" "CHAR"(18) NULL, 
	CONSTRAINT "XPKHub Product" PRIMARY KEY ("Product Hash Key")
);

CREATE UNIQUE INDEX ""."XPKHub Product" ON ""."Hub Product"("Product Hash Key" ASC);

CREATE TABLE ""."Hub Quote"(
	"Quote Hash Key" "CHAR"(18) NOT NULL, 
	"Load Date" "CHAR"(18) NULL, 
	"Record Source" "CHAR"(18) NULL, 
	"Quote Id" "CHAR"(18) NULL, 
	CONSTRAINT "XPKHub Quote" PRIMARY KEY ("Quote Hash Key")
);

CREATE UNIQUE INDEX ""."XPKHub Quote" ON ""."Hub Quote"("Quote Hash Key" ASC);

CREATE TABLE ""."Link Customer Lead"(
	"Customer Lead Hash Key" "CHAR"(18) NOT NULL, 
	"Load Date" "CHAR"(18) NULL, 
	"Record Source" "CHAR"(18) NULL, 
	"Lead Hash Key" "CHAR"(18) NULL, 
	"Customer Hash Key" "CHAR"(18) NULL, 
	CONSTRAINT "XPKLink Customer Lead" PRIMARY KEY ("Customer Lead Hash Key")
);

CREATE INDEX ""."XIF1Link Customer Lead" ON ""."Link Customer Lead"("Customer Hash Key" ASC);

CREATE INDEX ""."XIF2Link Customer Lead" ON ""."Link Customer Lead"("Lead Hash Key" ASC);

CREATE UNIQUE INDEX ""."XPKLink Customer Lead" ON ""."Link Customer Lead"("Customer Lead Hash Key" ASC);

CREATE TABLE ""."Link Customer Person"(
	"Customer Person Hash Key" "CHAR"(18) NOT NULL, 
	"Load Date" "CHAR"(18) NULL, 
	"Record Source" "CHAR"(18) NULL, 
	"Customer Hash Key" "CHAR"(18) NULL, 
	"Person Hash Key" "CHAR"(18) NULL, 
	CONSTRAINT "XPKLink Customer Person" PRIMARY KEY ("Customer Person Hash Key")
);

CREATE INDEX ""."XIF1Link Customer Person" ON ""."Link Customer Person"("Customer Hash Key" ASC);

CREATE INDEX ""."XIF2Link Customer Person" ON ""."Link Customer Person"("Person Hash Key" ASC);

CREATE UNIQUE INDEX ""."XPKLink Customer Person" ON ""."Link Customer Person"("Customer Person Hash Key" ASC);

CREATE TABLE ""."Link Person Account"(
	"Person Account Hash Key" "CHAR"(18) NOT NULL, 
	"Record Source" "CHAR"(18) NULL, 
	"Account Hash Key" "CHAR"(18) NULL, 
	"Load Date" "CHAR"(18) NULL, 
	"Person Hash Key" "CHAR"(18) NULL, 
	CONSTRAINT "XPKLink Person Account" PRIMARY KEY ("Person Account Hash Key")
);

CREATE INDEX ""."XIF2Link Person Account" ON ""."Link Person Account"("Account Hash Key" ASC);

CREATE INDEX ""."XIF3Link Person Account" ON ""."Link Person Account"("Person Hash Key" ASC);

CREATE UNIQUE INDEX ""."XPKLink Person Account" ON ""."Link Person Account"("Person Account Hash Key" ASC);

CREATE TABLE ""."Link Person Consent"(
	"Person Consent Hash Key" "CHAR"(18) NOT NULL, 
	"Load Date" "CHAR"(18) NULL, 
	"Record Source" "CHAR"(18) NULL, 
	"Person Hash Key" "CHAR"(18) NULL, 
	"Consent Hash Key" "CHAR"(18) NULL, 
	CONSTRAINT "XPKLink Person Consent" PRIMARY KEY ("Person Consent Hash Key")
);

CREATE INDEX ""."XIF1Link Person Consent" ON ""."Link Person Consent"("Person Hash Key" ASC);

CREATE INDEX ""."XIF2Link Person Consent" ON ""."Link Person Consent"("Consent Hash Key" ASC);

CREATE UNIQUE INDEX ""."XPKLink Person Consent" ON ""."Link Person Consent"("Person Consent Hash Key" ASC);

CREATE TABLE ""."Link Person Contact"(
	"Person Contact Hash Key" "CHAR"(18) NOT NULL, 
	"Person Hash Key" "CHAR"(18) NULL, 
	"Contact Hash Key" "CHAR"(18) NULL, 
	"Load Date" "CHAR"(18) NULL, 
	"Record Source" "CHAR"(18) NULL, 
	CONSTRAINT "XPKLink Person Contact" PRIMARY KEY ("Person Contact Hash Key")
);

CREATE INDEX ""."XIF1Link Person Contact" ON ""."Link Person Contact"("Person Hash Key" ASC);

CREATE INDEX ""."XIF2Link Person Contact" ON ""."Link Person Contact"("Contact Hash Key" ASC);

CREATE UNIQUE INDEX ""."XPKLink Person Contact" ON ""."Link Person Contact"("Person Contact Hash Key" ASC);

CREATE TABLE ""."Link Person Home Address"(
	"Person Home Address Hash Key" "CHAR"(18) NOT NULL, 
	"Load Date" "CHAR"(18) NULL, 
	"Record Source" "CHAR"(18) NULL, 
	"Person Hash Key" "CHAR"(18) NULL, 
	"Home Address Hash Key" "CHAR"(18) NULL, 
	CONSTRAINT "XPKLink Person Home Address" PRIMARY KEY ("Person Home Address Hash Key")
);

CREATE INDEX ""."XIF1Link Person Home Address" ON ""."Link Person Home Address"("Person Hash Key" ASC);

CREATE INDEX ""."XIF2Link Person Home Address" ON ""."Link Person Home Address"("Home Address Hash Key" ASC);

CREATE UNIQUE INDEX ""."XPKLink Person Home Address" ON ""."Link Person Home Address"("Person Home Address Hash Key" ASC);

CREATE TABLE ""."Link Person Identities"(
	"Person Identities Hash Key" "CHAR"(18) NOT NULL, 
	"Load Date" "CHAR"(18) NULL, 
	"Record Source" "CHAR"(18) NULL, 
	"Person Hash Key" "CHAR"(18) NULL, 
	"Identities Hash Key" "CHAR"(18) NULL, 
	CONSTRAINT "XPKLink Person Identities" PRIMARY KEY ("Person Identities Hash Key")
);

CREATE INDEX ""."XIF1Link Person Identities" ON ""."Link Person Identities"("Person Hash Key" ASC);

CREATE INDEX ""."XIF2Link Person Identities" ON ""."Link Person Identities"("Identities Hash Key" ASC);

CREATE UNIQUE INDEX ""."XPKLink Person Identities" ON ""."Link Person Identities"("Person Identities Hash Key" ASC);

CREATE TABLE ""."Link Person Lead"(
	"Person Lead Hash Key" "CHAR"(18) NOT NULL, 
	"Load Date" "CHAR"(18) NULL, 
	"Record Source" "CHAR"(18) NULL, 
	"Person Hash Key" "CHAR"(18) NULL, 
	"Lead Hash Key" "CHAR"(18) NULL, 
	CONSTRAINT "XPKLink Person Lead" PRIMARY KEY ("Person Lead Hash Key")
);

CREATE INDEX ""."XIF1Link Person Lead" ON ""."Link Person Lead"("Person Hash Key" ASC);

CREATE INDEX ""."XIF2Link Person Lead" ON ""."Link Person Lead"("Lead Hash Key" ASC);

CREATE UNIQUE INDEX ""."XPKLink Person Lead" ON ""."Link Person Lead"("Person Lead Hash Key" ASC);

CREATE TABLE ""."Link Person Legal Person"(
	"Person Legal Person Hash Key" "CHAR"(18) NOT NULL, 
	"Load Date" "CHAR"(18) NULL, 
	"Record Source" "CHAR"(18) NULL, 
	"Person Hash Key" "CHAR"(18) NULL, 
	"Legal Person Hash Key" "CHAR"(18) NULL, 
	CONSTRAINT "XPKLink Person Legal Person" PRIMARY KEY ("Person Legal Person Hash Key")
);

CREATE INDEX ""."XIF1Link Person Legal Person" ON ""."Link Person Legal Person"("Person Hash Key" ASC);

CREATE INDEX ""."XIF2Link Person Legal Person" ON ""."Link Person Legal Person"("Legal Person Hash Key" ASC);

CREATE UNIQUE INDEX ""."XPKLink Person Legal Person" ON ""."Link Person Legal Person"("Person Legal Person Hash Key" ASC);

CREATE TABLE ""."Link Person Marketing Engagement"(
	"Person Marketing Engagement Hash Key" "CHAR"(18) NOT NULL, 
	"Load Date" "CHAR"(18) NULL, 
	"Record Source" "CHAR"(18) NULL, 
	"Person Hash Key" "CHAR"(18) NULL, 
	"Marketing Engagement Hash Key" "CHAR"(18) NULL, 
	CONSTRAINT "XPKLink Person Marketing Engagement" PRIMARY KEY ("Person Marketing Engagement Hash Key")
);

CREATE INDEX ""."XIF1Link Person Marketing Engagement" ON ""."Link Person Marketing Engagement"("Person Hash Key" ASC);

CREATE INDEX ""."XIF2Link Person Marketing Engagement" ON ""."Link Person Marketing Engagement"("Marketing Engagement Hash Key" ASC);

CREATE UNIQUE INDEX ""."XPKLink Person Marketing Engagement" ON ""."Link Person Marketing Engagement"("Person Marketing Engagement Hash Key" ASC);

CREATE TABLE ""."Link Person Marketing Preference"(
	"Person Marketing Preference Hash Key" "CHAR"(18) NOT NULL, 
	"Load Date" "CHAR"(18) NULL, 
	"Record Source" "CHAR"(18) NULL, 
	"Person Hash Key" "CHAR"(18) NULL, 
	"Marketing Preference Hash Key" "CHAR"(18) NULL, 
	CONSTRAINT "XPKLink Person Marketing Preference" PRIMARY KEY ("Person Marketing Preference Hash Key")
);

CREATE INDEX ""."XIF1Link Person Marketing Preference" ON ""."Link Person Marketing Preference"("Person Hash Key" ASC);

CREATE INDEX ""."XIF2Link Person Marketing Preference" ON ""."Link Person Marketing Preference"("Marketing Preference Hash Key" ASC);

CREATE UNIQUE INDEX ""."XPKLink Person Marketing Preference" ON ""."Link Person Marketing Preference"("Person Marketing Preference Hash Key" ASC);

CREATE TABLE ""."Link Person Natural Person"(
	"Person Natural Person Hash Key" "CHAR"(18) NOT NULL, 
	"Load Date" "CHAR"(18) NULL, 
	"Record Source" "CHAR"(18) NULL, 
	"Person Hash Key" "CHAR"(18) NULL, 
	"Natural Person Hash Key" "CHAR"(18) NULL, 
	CONSTRAINT "XPKLink Person Natural Person" PRIMARY KEY ("Person Natural Person Hash Key")
);

CREATE INDEX ""."XIF1Link Person Natural Person" ON ""."Link Person Natural Person"("Person Hash Key" ASC);

CREATE INDEX ""."XIF2Link Person Natural Person" ON ""."Link Person Natural Person"("Natural Person Hash Key" ASC);

CREATE UNIQUE INDEX ""."XPKLink Person Natural Person" ON ""."Link Person Natural Person"("Person Natural Person Hash Key" ASC);

CREATE TABLE ""."Link Policy Customer"(
	"Policy Customer Hash Key" "CHAR"(18) NOT NULL, 
	"Load Date" "CHAR"(18) NULL, 
	"Record Source" "CHAR"(18) NULL, 
	"Customer Hash Key" "CHAR"(18) NULL, 
	"Policy Hash Key" "CHAR"(18) NULL, 
	CONSTRAINT "XPKLink Policy Customer" PRIMARY KEY ("Policy Customer Hash Key")
);

CREATE INDEX ""."XIF1Link Policy Customer" ON ""."Link Policy Customer"("Policy Hash Key" ASC);

CREATE INDEX ""."XIF2Link Policy Customer" ON ""."Link Policy Customer"("Customer Hash Key" ASC);

CREATE UNIQUE INDEX ""."XPKLink Policy Customer" ON ""."Link Policy Customer"("Policy Customer Hash Key" ASC);

CREATE TABLE ""."Link Policy Product"(
	"Policy Customer Hash Key" "CHAR"(18) NOT NULL, 
	"Load Date" "CHAR"(18) NULL, 
	"Record Source" "CHAR"(18) NULL, 
	"Policy Hash Key" "CHAR"(18) NULL, 
	"Product Hash Key" "CHAR"(18) NULL, 
	CONSTRAINT "XPKLink Policy Product" PRIMARY KEY ("Policy Customer Hash Key")
);

CREATE INDEX ""."XIF1Link Policy Product" ON ""."Link Policy Product"("Policy Hash Key" ASC);

CREATE INDEX ""."XIF2Link Policy Product" ON ""."Link Policy Product"("Product Hash Key" ASC);

CREATE UNIQUE INDEX ""."XPKLink Policy Product" ON ""."Link Policy Product"("Policy Customer Hash Key" ASC);

CREATE TABLE ""."Link Product Home"(
	"Product Home Hash Key" "CHAR"(18) NOT NULL, 
	"Load Date" "CHAR"(18) NULL, 
	"Record Source" "CHAR"(18) NULL, 
	"Product Hash Key" "CHAR"(18) NULL, 
	"Home Hash Key" "CHAR"(18) NULL, 
	CONSTRAINT "XPKLink Product Home" PRIMARY KEY ("Product Home Hash Key")
);

CREATE INDEX ""."XIF1Link Product Home" ON ""."Link Product Home"("Product Hash Key" ASC);

CREATE INDEX ""."XIF2Link Product Home" ON ""."Link Product Home"("Home Hash Key" ASC);

CREATE UNIQUE INDEX ""."XPKLink Product Home" ON ""."Link Product Home"("Product Home Hash Key" ASC);

CREATE TABLE ""."Link Product Motor"(
	"Product Motor Hash Key" "CHAR"(18) NOT NULL, 
	"Load Date" "CHAR"(18) NULL, 
	"Record Source" "CHAR"(18) NULL, 
	"Product Hash Key" "CHAR"(18) NULL, 
	"Motor Hash Key" "CHAR"(18) NULL, 
	CONSTRAINT "XPKLink Product Motor" PRIMARY KEY ("Product Motor Hash Key")
);

CREATE INDEX ""."XIF1Link Product Motor" ON ""."Link Product Motor"("Product Hash Key" ASC);

CREATE INDEX ""."XIF2Link Product Motor" ON ""."Link Product Motor"("Motor Hash Key" ASC);

CREATE UNIQUE INDEX ""."XPKLink Product Motor" ON ""."Link Product Motor"("Product Motor Hash Key" ASC);

CREATE TABLE ""."Link Quote Person"(
	"Quote Person Hash Key" "CHAR"(18) NOT NULL, 
	"Load Date" "CHAR"(18) NULL, 
	"Record Source" "CHAR"(18) NULL, 
	"Quote Hash Key" "CHAR"(18) NULL, 
	"Person Hash Key" "CHAR"(18) NULL, 
	CONSTRAINT "XPKLink Quote Person" PRIMARY KEY ("Quote Person Hash Key")
);

CREATE INDEX ""."XIF1Link Quote Person" ON ""."Link Quote Person"("Quote Hash Key" ASC);

CREATE INDEX ""."XIF2Link Quote Person" ON ""."Link Quote Person"("Person Hash Key" ASC);

CREATE UNIQUE INDEX ""."XPKLink Quote Person" ON ""."Link Quote Person"("Quote Person Hash Key" ASC);

CREATE TABLE ""."Link Quote Product"(
	"Quote Product Hash Key" "CHAR"(18) NOT NULL, 
	"Load Date" "CHAR"(18) NULL, 
	"Record Source" "CHAR"(18) NULL, 
	"Quote Hash Key" "CHAR"(18) NULL, 
	"Product Hash Key" "CHAR"(18) NULL, 
	CONSTRAINT "XPKLink Quote Product" PRIMARY KEY ("Quote Product Hash Key")
);

CREATE INDEX ""."XIF1Link Quote Product" ON ""."Link Quote Product"("Quote Hash Key" ASC);

CREATE INDEX ""."XIF2Link Quote Product" ON ""."Link Quote Product"("Product Hash Key" ASC);

CREATE UNIQUE INDEX ""."XPKLink Quote Product" ON ""."Link Quote Product"("Quote Product Hash Key" ASC);

CREATE TABLE ""."Sat Account"(
	"Account Hash Key" "CHAR"(18) NOT NULL, 
	"Load Date" "CHAR"(18) NOT NULL, 
	"Account Number" "CHAR"(18) NULL,
	"Account Type" "CHAR"(18) NULL, 
	"Account Last Access" "CHAR"(18) NULL, 
	"Account Last Change" "CHAR"(18) NULL, 
	"Account Creation Type" "CHAR"(18) NULL, 
	"Account Status" "CHAR"(18) NULL, 
	CONSTRAINT "XPKSat Account" PRIMARY KEY ("Account Hash Key", "Load Date")
);

CREATE INDEX ""."XIF1Sat Account" ON ""."Sat Account"("Account Hash Key" ASC);

CREATE UNIQUE INDEX ""."XPKSat Account" ON ""."Sat Account"("Account Hash Key" ASC, "Load Date" ASC);

CREATE TABLE ""."Sat Consent"(
	"Consent Hash Key" "CHAR"(18) NOT NULL, 
	"Load Date" "CHAR"(18) NOT NULL, 
	"Opt In Validated" "CHAR"(18) NULL, 
	"Opt In Legitimate Interest" "CHAR"(18) NULL, 
	CONSTRAINT "XPKSat Consent" PRIMARY KEY ("Consent Hash Key", "Load Date")
);

CREATE INDEX ""."XIF1Sat Consent" ON ""."Sat Consent"("Consent Hash Key" ASC);

CREATE UNIQUE INDEX ""."XPKSat Consent" ON ""."Sat Consent"("Consent Hash Key" ASC, "Load Date" ASC);

CREATE TABLE ""."Sat Contact"(
	"Load Date" "CHAR"(18) NOT NULL, 
	"Contact Hash Key" "CHAR"(18) NOT NULL, 
	"Personal Email" "CHAR"(18) NULL, 
	"Work Email" "CHAR"(18) NULL, 
	"Work Phone" "CHAR"(18) NULL, 
	"Home Phone" "CHAR"(18) NULL, 
	CONSTRAINT "XPKSat Contact" PRIMARY KEY ("Contact Hash Key", "Load Date")
);

CREATE INDEX ""."XIF1Sat Contact" ON ""."Sat Contact"("Contact Hash Key" ASC);

CREATE UNIQUE INDEX ""."XPKSat Contact" ON ""."Sat Contact"("Load Date" ASC, "Contact Hash Key" ASC);

CREATE TABLE ""."Sat Customer"(
	"Load Date" "CHAR"(18) NOT NULL, 
	"Customer Hash Key" "CHAR"(18) NOT NULL, 
	"Customer Number" "CHAR"(18) NULL, 
	"Customer Status" "CHAR"(18) NULL, 
	"Customer Status Reason" "CHAR"(18) NULL, 
	"Customer Since" "CHAR"(18) NULL, 
	"Customer Rating" "CHAR"(18) NULL, 
	"Customer Segment" "CHAR"(18) NULL, 
	"Line Of Business" "CHAR"(18) NULL, 
	"NPS Score" "CHAR"(18) NULL, 
	CONSTRAINT "XPKSat Customer" PRIMARY KEY ("Customer Hash Key", "Load Date")
);

CREATE INDEX ""."XIF1Sat Customer" ON ""."Sat Customer"("Customer Hash Key" ASC);

CREATE UNIQUE INDEX ""."XPKSat Customer" ON ""."Sat Customer"("Load Date" ASC, "Customer Hash Key" ASC);

CREATE TABLE ""."Sat Home"(
	"Home Hash Key" "CHAR"(18) NOT NULL, 
	"Load Date" "CHAR"(18) NOT NULL, 
	"Wall Construction" "CHAR"(18) NULL, 
	"Home Risk Address" "CHAR"(18) NULL, 
	"Roof Construction" "CHAR"(18) NULL, 
	"Home Type" "CHAR"(18) NULL, 
	"Home State" "CHAR"(18) NULL, 
	"Is Existing Home Customer" "CHAR"(18) NULL, 
	CONSTRAINT "XPKSat Home" PRIMARY KEY ("Home Hash Key", "Load Date")
);

CREATE INDEX ""."XIF1Sat Home" ON ""."Sat Home"("Home Hash Key" ASC);

CREATE UNIQUE INDEX ""."XPKSat Home" ON ""."Sat Home"("Home Hash Key" ASC, "Load Date" ASC);

CREATE TABLE ""."Sat Home Address"(
	"Home Address Hash Key" "CHAR"(18) NOT NULL, 
	"Load Date" "CHAR"(18) NOT NULL, 
	"Street" "CHAR"(18) NULL, 
	"Postcode" "CHAR"(18) NULL, 
	"City" "CHAR"(18) NULL, 
	"State" "CHAR"(18) NULL, 
	"Country" "CHAR"(18) NULL, 
	CONSTRAINT "XPKSat Home Address" PRIMARY KEY ("Home Address Hash Key", "Load Date")
);

CREATE INDEX ""."XIF1Sat Home Address" ON ""."Sat Home Address"("Home Address Hash Key" ASC);

CREATE UNIQUE INDEX ""."XPKSat Home Address" ON ""."Sat Home Address"("Home Address Hash Key" ASC, "Load Date" ASC);

CREATE TABLE ""."Sat Identities"(
	"Identities Hash Key" "CHAR"(18) NOT NULL, 
	"Load Date" "CHAR"(18) NOT NULL, 
	"ECID" "CHAR"(18) NULL, 
	"Hashed Email" "CHAR"(18) NULL, 
	CONSTRAINT "XPKSat Identities" PRIMARY KEY ("Identities Hash Key", "Load Date")
);

CREATE INDEX ""."XIF1Sat Identities" ON ""."Sat Identities"("Identities Hash Key" ASC);

CREATE UNIQUE INDEX ""."XPKSat Identities" ON ""."Sat Identities"("Identities Hash Key" ASC, "Load Date" ASC);

CREATE TABLE ""."Sat Lead"(
	"Lead Hash Key" "CHAR"(18) NOT NULL, 
	"Load Date" "CHAR"(18) NOT NULL, 
	"Interested Level" "CHAR"(18) NULL, 
	"Preferred Contact Method" "CHAR"(18) NULL, 
	"Person Score" "CHAR"(18) NULL, 
	"Person Status" "CHAR"(18) NULL, 
	"Converted Date" "CHAR"(18) NULL, 
	CONSTRAINT "XPKSat Lead" PRIMARY KEY ("Lead Hash Key", "Load Date")
);

CREATE INDEX ""."XIF1Sat Lead" ON ""."Sat Lead"("Lead Hash Key" ASC);

CREATE UNIQUE INDEX ""."XPKSat Lead" ON ""."Sat Lead"("Lead Hash Key" ASC, "Load Date" ASC);

CREATE TABLE ""."Sat Legal Person"(
	"Legal Person Hash Key" "CHAR"(18) NOT NULL, 
	"Load Date" "CHAR"(18) NOT NULL, 
	"Job Title" "CHAR"(18) NULL, 
	"Converted Date" "CHAR"(18) NULL, 
	"Person Status" "CHAR"(18) NULL, 
	"Person Score" "CHAR"(18) NULL, 
	"Company Name" "CHAR"(18) NULL,
	"Date of Constitution" "CHAR"(18) NULL,
	"Source Type" "CHAR"(18) NULL, 
	"Source Id" "CHAR"(18) NULL, 
	CONSTRAINT "XPKSat Legal Person" PRIMARY KEY ("Legal Person Hash Key", "Load Date")
);

CREATE INDEX ""."XIF1Sat Legal Person" ON ""."Sat Legal Person"("Legal Person Hash Key" ASC);

CREATE UNIQUE INDEX ""."XPKSat Legal Person" ON ""."Sat Legal Person"("Legal Person Hash Key" ASC, "Load Date" ASC);

CREATE TABLE ""."Sat Marketing Engagement"(
	"Marketing Engagement Hash Key" "CHAR"(18) NOT NULL, 
	"Load Date" "CHAR"(18) NOT NULL, 
	"Promotion Code" "CHAR"(18) NULL, 
	"Opened Email" "CHAR"(18) NULL, 
	"Marketing Status" "CHAR"(18) NULL, 
	CONSTRAINT "XPKSat Marketing Engagement" PRIMARY KEY ("Load Date", "Marketing Engagement Hash Key")
);

CREATE INDEX ""."XIF1Sat Marketing Engagement" ON ""."Sat Marketing Engagement"("Marketing Engagement Hash Key" ASC);

CREATE UNIQUE INDEX ""."XPKSat Marketing Engagement" ON ""."Sat Marketing Engagement"("Marketing Engagement Hash Key" ASC, "Load Date" ASC);

CREATE TABLE ""."Sat Marketing Preference"(
	"Marketing Preference Hash Key" "CHAR"(18) NOT NULL, 
	"Load Date" "CHAR"(18) NOT NULL, 
	"SMS" "CHAR"(18) NULL, 
	"Email" "CHAR"(18) NULL, 
	"Email Subscriptions" "CHAR"(18) NULL, 
	"Call" "CHAR"(18) NULL, 
	"Any" "CHAR"(18) NULL, 
	"Commercial Email" "CHAR"(18) NULL, 
	"Postal Mail" "CHAR"(18) NULL, 
	CONSTRAINT "XPKSat Marketing Preference" PRIMARY KEY ("Load Date", "Marketing Preference Hash Key")
);

CREATE INDEX ""."XIF1Sat Marketing Preference" ON ""."Sat Marketing Preference"("Marketing Preference Hash Key" ASC);

CREATE UNIQUE INDEX ""."XPKSat Marketing Preference" ON ""."Sat Marketing Preference"("Marketing Preference Hash Key" ASC, "Load Date" ASC);

CREATE TABLE ""."Sat Motor"(
	"Motor Hash Key" "CHAR"(18) NOT NULL, 
	"Load Date" "CHAR"(18) NOT NULL, 
	"Auto Decline Vehicle" "CHAR"(18) NULL, 
	"Body Type" "CHAR"(18) NULL, 
	"Fuel Type" "CHAR"(18) NULL, 
	"License Status" "CHAR"(18) NULL, 
	"Is Existing Motor Customer" "CHAR"(18) NULL, 
	"Motor Lapsed Policies" "CHAR"(18) NULL, 
	"Motor Risk Address" "CHAR"(18) NULL, 
	"Risk Class Code" "CHAR"(18) NULL, 
	"Variant" "CHAR"(18) NULL, 
	"Vehicle Owner Type" "CHAR"(18) NULL, 
	"Vehicle RegState" "CHAR"(18) NULL, 
	"Vehicle Class" "CHAR"(18) NULL, 
	"Vehicle Model" "CHAR"(18) NULL, 
	"Vehicle Type" "CHAR"(18) NULL, 
	"Motor Sum Insrd" "CHAR"(18) NULL, 
	"Vehicle Year" "CHAR"(18) NULL, 
	"Vehicle Age" "CHAR"(18) NULL, 
	CONSTRAINT "XPKSat Motor" PRIMARY KEY ("Load Date", "Motor Hash Key")
);

CREATE INDEX ""."XIF1Sat Motor" ON ""."Sat Motor"("Motor Hash Key" ASC);

CREATE UNIQUE INDEX ""."XPKSat Motor" ON ""."Sat Motor"("Motor Hash Key" ASC, "Load Date" ASC);

CREATE TABLE ""."Sat Natural Person"(
	"Load Date" "CHAR"(18) NOT NULL, 
	"Natural Person Hash Key" "CHAR"(18) NOT NULL, 
	"First Name" "CHAR"(18) NULL, 
	"Last Name" "CHAR"(18) NULL, 
	"Full Name" "CHAR"(18) NULL, 
	"Courtesy Title" "CHAR"(18) NULL, 
	"Occupation" "CHAR"(18) NULL, 
	"Birth Date" "CHAR"(18) NULL, 
	"Birth Year" "CHAR"(18) NULL, 
	"Nationality" "CHAR"(18) NULL, 
	"Gender" "CHAR"(18) NULL, 
	"Marital Status" "CHAR"(18) NULL, 
	"Assesed Disability Degree" "CHAR"(18) NULL, 
	"Preferred Language" "CHAR"(18) NULL, 
	"Role" "CHAR"(18) NULL, 
	"Job Title" "CHAR"(18) NULL, 
	CONSTRAINT "XPKSat Natural Person" PRIMARY KEY ("Load Date", "Natural Person Hash Key")
);

CREATE INDEX ""."XIF1Sat Natural Person" ON ""."Sat Natural Person"("Natural Person Hash Key" ASC);

CREATE UNIQUE INDEX ""."XPKSat Natural Person" ON ""."Sat Natural Person"("Load Date" ASC, "Natural Person Hash Key" ASC);

CREATE TABLE ""."Sat Person"(
	"Person Hash Key" "CHAR"(18) NOT NULL, 
	"Load Date" "CHAR"(18) NOT NULL, 
	"Tenant Id" "CHAR"(18) NULL, 
	"Is Lead" "CHAR"(18) NULL, 
	"Type" "CHAR"(18) NULL, 
	"Operational Paperless Consent" "CHAR"(18) NULL, 
	"Source Id" "CHAR"(18) NULL, 
	"Source Type" "CHAR"(18) NULL, 
	CONSTRAINT "XPKSat Person" PRIMARY KEY ("Load Date", "Person Hash Key")
);

CREATE INDEX ""."XIF8Sat Person" ON ""."Sat Person"("Person Hash Key" ASC);

CREATE UNIQUE INDEX ""."XPKSat Person" ON ""."Sat Person"("Person Hash Key" ASC, "Load Date" ASC);

CREATE TABLE ""."Sat Policy"(
	"Policy Hash Key" "CHAR"(18) NOT NULL, 
	"Load Date" "CHAR"(18) NOT NULL, 
	"Cover Option" "CHAR"(18) NULL, 
	"Declined Claims" "CHAR"(18) NULL, 
	"Fraud Flag" "CHAR"(18) NULL, 
	"Gross Revenue" "CHAR"(18) NULL,
	"Net Revenue" "CHAR"(18) NULL,
	"Number of Active Claim" "CHAR"(18) NULL, 
	"Number of Previous Claim" "CHAR"(18) NULL, 
	"Policy Cicle" "CHAR"(18) NULL, 
	"Policy End Date" "CHAR"(18) NULL, 
	"Policy Length" "CHAR"(18) NULL, 
	"Policy Number" "CHAR"(18) NULL, 
	"Policy Start Date" "CHAR"(18) NULL, 
	"Policy Status" "CHAR"(18) NULL, 
	"Renewal Amount Current Period" "CHAR"(18) NULL, 
	"Renewal Amount Next Period" "CHAR"(18) NULL, 
	"Renewal Date" "CHAR"(18) NULL, 
	"Sales Channel" "CHAR"(18) NULL, 
	CONSTRAINT "XPKSat Policy" PRIMARY KEY ("Load Date", "Policy Hash Key")
);

CREATE INDEX ""."XIF5Sat Policy" ON ""."Sat Policy"("Policy Hash Key" ASC);

CREATE UNIQUE INDEX ""."XPKSat Policy" ON ""."Sat Policy"("Policy Hash Key" ASC, "Load Date" ASC);

CREATE TABLE ""."Sat Product"(
	"Product Hash Key" "CHAR"(18) NOT NULL, 
	"Load Date" "CHAR"(18) NOT NULL, 
	"Type" "CHAR"(18) NULL, 
	CONSTRAINT "XPKSat Product" PRIMARY KEY ("Load Date", "Product Hash Key")
);

CREATE INDEX ""."XIF1Sat Product" ON ""."Sat Product"("Product Hash Key" ASC);

CREATE UNIQUE INDEX ""."XPKSat Product" ON ""."Sat Product"("Product Hash Key" ASC, "Load Date" ASC);

CREATE TABLE ""."Sat Quote"(
	"Quote Hash Key" "CHAR"(18) NOT NULL, 
	"Load Date" "CHAR"(18) NOT NULL, 
	"Gross Revenue" "CHAR"(18) NULL,
	"Net Revenue" "CHAR"(18) NULL, 
	"Quote Number" "CHAR"(18) NULL, 
	"Quote Status" "CHAR"(18) NULL, 
	"Renewal Amt Current Period" "CHAR"(18) NULL, 
	"Renewal Amt Next Period" "CHAR"(18) NULL, 
	CONSTRAINT "XPKSat Quote" PRIMARY KEY ("Load Date", "Quote Hash Key")
);

CREATE INDEX ""."XIF3Sat Quote" ON ""."Sat Quote"("Quote Hash Key" ASC);

CREATE UNIQUE INDEX ""."XPKSat Quote" ON ""."Sat Quote"("Quote Hash Key" ASC, "Load Date" ASC);

ALTER TABLE ""."Link Customer Lead" ADD CONSTRAINT "XIF1Link Customer Lead" FOREIGN KEY ("Customer Hash Key") REFERENCES ""."Hub Customer" ("Customer Hash Key");

ALTER TABLE ""."Link Customer Lead" ADD CONSTRAINT "XIF2Link Customer Lead" FOREIGN KEY ("Lead Hash Key") REFERENCES ""."Hub Lead" ("Lead Hash Key");

ALTER TABLE ""."Link Customer Person" ADD CONSTRAINT "XIF1Link Customer Person" FOREIGN KEY ("Customer Hash Key") REFERENCES ""."Hub Customer" ("Customer Hash Key");

ALTER TABLE ""."Link Customer Person" ADD CONSTRAINT "XIF2Link Customer Person" FOREIGN KEY ("Person Hash Key") REFERENCES ""."Hub Person" ("Person Hash Key");

ALTER TABLE ""."Link Person Account" ADD CONSTRAINT "XIF2Link Person Account" FOREIGN KEY ("Account Hash Key") REFERENCES ""."Hub Account" ("Account Hash Key");

ALTER TABLE ""."Link Person Account" ADD CONSTRAINT "XIF3Link Person Account" FOREIGN KEY ("Person Hash Key") REFERENCES ""."Hub Person" ("Person Hash Key");

ALTER TABLE ""."Link Person Consent" ADD CONSTRAINT "XIF1Link Person Consent" FOREIGN KEY ("Person Hash Key") REFERENCES ""."Hub Person" ("Person Hash Key");

ALTER TABLE ""."Link Person Consent" ADD CONSTRAINT "XIF2Link Person Consent" FOREIGN KEY ("Consent Hash Key") REFERENCES ""."Hub Consent" ("Consent Hash Key");

ALTER TABLE ""."Link Person Contact" ADD CONSTRAINT "XIF1Link Person Contact" FOREIGN KEY ("Person Hash Key") REFERENCES ""."Hub Person" ("Person Hash Key");

ALTER TABLE ""."Link Person Contact" ADD CONSTRAINT "XIF2Link Person Contact" FOREIGN KEY ("Contact Hash Key") REFERENCES ""."Hub Contact" ("Contact Hash Key");

ALTER TABLE ""."Link Person Home Address" ADD CONSTRAINT "XIF1Link Person Home Address" FOREIGN KEY ("Person Hash Key") REFERENCES ""."Hub Person" ("Person Hash Key");

ALTER TABLE ""."Link Person Home Address" ADD CONSTRAINT "XIF2Link Person Home Address" FOREIGN KEY ("Home Address Hash Key") REFERENCES ""."Hub Home Address" ("Home Address Hash Key");

ALTER TABLE ""."Link Person Identities" ADD CONSTRAINT "XIF1Link Person Identities" FOREIGN KEY ("Person Hash Key") REFERENCES ""."Hub Person" ("Person Hash Key");

ALTER TABLE ""."Link Person Identities" ADD CONSTRAINT "XIF2Link Person Identities" FOREIGN KEY ("Identities Hash Key") REFERENCES ""."Hub Identities" ("Identities Hash Key");

ALTER TABLE ""."Link Person Lead" ADD CONSTRAINT "XIF1Link Person Lead" FOREIGN KEY ("Person Hash Key") REFERENCES ""."Hub Person" ("Person Hash Key");

ALTER TABLE ""."Link Person Lead" ADD CONSTRAINT "XIF2Link Person Lead" FOREIGN KEY ("Lead Hash Key") REFERENCES ""."Hub Lead" ("Lead Hash Key");

ALTER TABLE ""."Link Person Legal Person" ADD CONSTRAINT "XIF1Link Person Legal Person" FOREIGN KEY ("Person Hash Key") REFERENCES ""."Hub Person" ("Person Hash Key");

ALTER TABLE ""."Link Person Legal Person" ADD CONSTRAINT "XIF2Link Person Legal Person" FOREIGN KEY ("Legal Person Hash Key") REFERENCES ""."Hub Legal Person" ("Legal Person Hash Key");

ALTER TABLE ""."Link Person Marketing Engagement" ADD CONSTRAINT "XIF1Link Person Marketing Engagement" FOREIGN KEY ("Person Hash Key") REFERENCES ""."Hub Person" ("Person Hash Key");

ALTER TABLE ""."Link Person Marketing Engagement" ADD CONSTRAINT "XIF2Link Person Marketing Engagement" FOREIGN KEY ("Marketing Engagement Hash Key") REFERENCES ""."Hub Marketing Engagement" ("Marketing Engagement Hash Key");

ALTER TABLE ""."Link Person Marketing Preference" ADD CONSTRAINT "XIF1Link Person Marketing Preference" FOREIGN KEY ("Person Hash Key") REFERENCES ""."Hub Person" ("Person Hash Key");

ALTER TABLE ""."Link Person Marketing Preference" ADD CONSTRAINT "XIF2Link Person Marketing Preference" FOREIGN KEY ("Marketing Preference Hash Key") REFERENCES ""."Hub Marketing Preference" ("Marketing Preference Hash Key");

ALTER TABLE ""."Link Person Natural Person" ADD CONSTRAINT "XIF1Link Person Natural Person" FOREIGN KEY ("Person Hash Key") REFERENCES ""."Hub Person" ("Person Hash Key");

ALTER TABLE ""."Link Person Natural Person" ADD CONSTRAINT "XIF2Link Person Natural Person" FOREIGN KEY ("Natural Person Hash Key") REFERENCES ""."Hub Natural Person" ("Natural Person Hash Key");

ALTER TABLE ""."Link Policy Customer" ADD CONSTRAINT "XIF1Link Policy Customer" FOREIGN KEY ("Policy Hash Key") REFERENCES ""."Hub Policy" ("Policy Hash Key");

ALTER TABLE ""."Link Policy Customer" ADD CONSTRAINT "XIF2Link Policy Customer" FOREIGN KEY ("Customer Hash Key") REFERENCES ""."Hub Customer" ("Customer Hash Key");

ALTER TABLE ""."Link Policy Product" ADD CONSTRAINT "XIF1Link Policy Product" FOREIGN KEY ("Policy Hash Key") REFERENCES ""."Hub Policy" ("Policy Hash Key");

ALTER TABLE ""."Link Policy Product" ADD CONSTRAINT "XIF2Link Policy Product" FOREIGN KEY ("Product Hash Key") REFERENCES ""."Hub Product" ("Product Hash Key");

ALTER TABLE ""."Link Product Home" ADD CONSTRAINT "XIF1Link Product Home" FOREIGN KEY ("Product Hash Key") REFERENCES ""."Hub Product" ("Product Hash Key");

ALTER TABLE ""."Link Product Home" ADD CONSTRAINT "XIF2Link Product Home" FOREIGN KEY ("Home Hash Key") REFERENCES ""."Hub Home" ("Home Hash Key");

ALTER TABLE ""."Link Product Motor" ADD CONSTRAINT "XIF1Link Product Motor" FOREIGN KEY ("Product Hash Key") REFERENCES ""."Hub Product" ("Product Hash Key");

ALTER TABLE ""."Link Product Motor" ADD CONSTRAINT "XIF2Link Product Motor" FOREIGN KEY ("Motor Hash Key") REFERENCES ""."Hub Motor" ("Motor Hash Key");

ALTER TABLE ""."Link Quote Person" ADD CONSTRAINT "XIF1Link Quote Person" FOREIGN KEY ("Quote Hash Key") REFERENCES ""."Hub Quote" ("Quote Hash Key");

ALTER TABLE ""."Link Quote Person" ADD CONSTRAINT "XIF2Link Quote Person" FOREIGN KEY ("Person Hash Key") REFERENCES ""."Hub Person" ("Person Hash Key");

ALTER TABLE ""."Link Quote Product" ADD CONSTRAINT "XIF1Link Quote Product" FOREIGN KEY ("Quote Hash Key") REFERENCES ""."Hub Quote" ("Quote Hash Key");

ALTER TABLE ""."Link Quote Product" ADD CONSTRAINT "XIF2Link Quote Product" FOREIGN KEY ("Product Hash Key") REFERENCES ""."Hub Product" ("Product Hash Key");

ALTER TABLE ""."Sat Account" ADD CONSTRAINT "XIF1Sat Account" FOREIGN KEY ("Account Hash Key") REFERENCES ""."Hub Account" ("Account Hash Key");

ALTER TABLE ""."Sat Consent" ADD CONSTRAINT "XIF1Sat Consent" FOREIGN KEY ("Consent Hash Key") REFERENCES ""."Hub Consent" ("Consent Hash Key");

ALTER TABLE ""."Sat Contact" ADD CONSTRAINT "XIF1Sat Contact" FOREIGN KEY ("Contact Hash Key") REFERENCES ""."Hub Contact" ("Contact Hash Key");

ALTER TABLE ""."Sat Customer" ADD CONSTRAINT "XIF1Sat Customer" FOREIGN KEY ("Customer Hash Key") REFERENCES ""."Hub Customer" ("Customer Hash Key");

ALTER TABLE ""."Sat Home" ADD CONSTRAINT "XIF1Sat Home" FOREIGN KEY ("Home Hash Key") REFERENCES ""."Hub Home" ("Home Hash Key");

ALTER TABLE ""."Sat Home Address" ADD CONSTRAINT "XIF1Sat Home Address" FOREIGN KEY ("Home Address Hash Key") REFERENCES ""."Hub Home Address" ("Home Address Hash Key");

ALTER TABLE ""."Sat Identities" ADD CONSTRAINT "XIF1Sat Identities" FOREIGN KEY ("Identities Hash Key") REFERENCES ""."Hub Identities" ("Identities Hash Key");

ALTER TABLE ""."Sat Lead" ADD CONSTRAINT "XIF1Sat Lead" FOREIGN KEY ("Lead Hash Key") REFERENCES ""."Hub Lead" ("Lead Hash Key");

ALTER TABLE ""."Sat Legal Person" ADD CONSTRAINT "XIF1Sat Legal Person" FOREIGN KEY ("Legal Person Hash Key") REFERENCES ""."Hub Legal Person" ("Legal Person Hash Key");

ALTER TABLE ""."Sat Marketing Engagement" ADD CONSTRAINT "XIF1Sat Marketing Engagement" FOREIGN KEY ("Marketing Engagement Hash Key") REFERENCES ""."Hub Marketing Engagement" ("Marketing Engagement Hash Key");

ALTER TABLE ""."Sat Marketing Preference" ADD CONSTRAINT "XIF1Sat Marketing Preference" FOREIGN KEY ("Marketing Preference Hash Key") REFERENCES ""."Hub Marketing Preference" ("Marketing Preference Hash Key");

ALTER TABLE ""."Sat Motor" ADD CONSTRAINT "XIF1Sat Motor" FOREIGN KEY ("Motor Hash Key") REFERENCES ""."Hub Motor" ("Motor Hash Key");

ALTER TABLE ""."Sat Natural Person" ADD CONSTRAINT "XIF1Sat Natural Person" FOREIGN KEY ("Natural Person Hash Key") REFERENCES ""."Hub Natural Person" ("Natural Person Hash Key");

ALTER TABLE ""."Sat Person" ADD CONSTRAINT "XIF8Sat Person" FOREIGN KEY ("Person Hash Key") REFERENCES ""."Hub Person" ("Person Hash Key");

ALTER TABLE ""."Sat Policy" ADD CONSTRAINT "XIF5Sat Policy" FOREIGN KEY ("Policy Hash Key") REFERENCES ""."Hub Policy" ("Policy Hash Key");

ALTER TABLE ""."Sat Product" ADD CONSTRAINT "XIF1Sat Product" FOREIGN KEY ("Product Hash Key") REFERENCES ""."Hub Product" ("Product Hash Key");

ALTER TABLE ""."Sat Quote" ADD CONSTRAINT "XIF3Sat Quote" FOREIGN KEY ("Quote Hash Key") REFERENCES ""."Hub Quote" ("Quote Hash Key");

