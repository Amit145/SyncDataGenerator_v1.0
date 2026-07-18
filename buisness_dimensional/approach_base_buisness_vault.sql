------------------------------------------------------------BV_Natural_person_master-------------------------------------------



 select 
  concat(crm.CRM_PERSON_HASH_KEY,'||', sap.SAP_PERSON_HASH_KEY) AS master_natural_person_hash_key,
    concat(crm.CRM_person_id,'||', sap.SAP_person_id) AS global_natural_person_identifier,
   crm.tenant_id,
   crm.is_lead,
   CASE WHEN sap.person_type IS NOT NULL  THEN sap.person_type ELSE crm.type END AS person_type, 
   crm.operational_paperless_consent,
   crm.source_id,
   crm.source_type,
   case when sap.first_name is not null then sap.first_name else crm.first_name end  as first_name,
   sap.middle_name,
   case when sap.last_name is not null then sap.last_name else crm.last_name end  as last_name,
   sap.date_of_birth,
   case when sap.gender is not null then sap.gender else crm.gender end  as gender,
   case when sap.occupation is not null then sap.occupation else crm.occupation end  as occupation,
   crm.courtesy_title,
   crm.role,
   crm.nationality,
   crm.marital_status,
   crm.assesed_disability_degree,
   crm.preferred_language,
   crm.job_title,
   case when sap.email_address is not null then sap.email_address else crm.personal_email end  as personal_email, 
   crm.work_email,
   crm.work_phone,
   case when sap.phone_number is not null then sap.phone_number else crm.home_phone end  as home_phone,
    CURRENT_DATE AS EFFECTIVE_FROM,
    TO_DATE('9999-12-31') AS EFFECTIVE_TO,
    'Y' AS CURRENT_ACTIVE_FLAG
from 
(select hp.person_hash_key as CRM_person_hash_key,hp.person_id as CRM_person_id,*
from allianz_coe.bvault_silver_raw.hub_person hp
inner join allianz_coe.bvault_silver_raw.sat_person sp
on sp.person_hash_key=hp.person_hash_key
inner join allianz_coe.bvault_silver_raw.link_person_natural_person lnp
on lnp.person_hash_key=hp.person_hash_key
inner Join allianz_coe.bvault_silver_raw.hub_natural_person h
on h.natural_person_hash_key=lnp.natural_person_hash_key
inner join allianz_coe.bvault_silver_raw.sat_natural_person snp
on snp.natural_person_hash_key=h.natural_person_hash_key
inner join allianz_coe.bvault_silver_raw.link_person_contact lpc
on lpc.person_hash_key=hp.person_hash_key
inner Join allianz_coe.bvault_silver_raw.hub_contact hc
on hc.contact_hash_key=lpc.contact_hash_key
inner join allianz_coe.bvault_silver_raw.sat_contact sc
on sc.contact_hash_key=hc.contact_hash_key

) crm
inner join
(select hp.person_hash_key as SAP_person_hash_key,hp.person_id as SAP_person_id,*
from allianz_coe.bvault_silver_raw.hub_person hp
inner join allianz_coe.bvault_silver_raw.sat_person_sap sp
on sp.person_hash_key=hp.person_hash_key
inner join allianz_coe.bvault_silver_raw.link_person_natural_person lnp
on lnp.person_hash_key=hp.person_hash_key
inner Join allianz_coe.bvault_silver_raw.hub_natural_person h
on h.natural_person_hash_key=lnp.natural_person_hash_key
inner join allianz_coe.bvault_silver_raw.sat_natural_person_sap snp
on snp.natural_person_hash_key=h.natural_person_hash_key
)sap
on concat(crm.first_name,crm.last_name, crm.birth_date)=concat(sap.first_name,sap.last_name, sap.date_of_birth)
;




--------------------------------------------------------------xref_natural_person_master---------------------------------------------




SELECT
   concat(crm.CRM_PERSON_HASH_KEY,'||', sap.SAP_PERSON_HASH_KEY) AS master_natural_person_hash_key,
   concat(crm.CRM_person_id,'||', sap.SAP_person_id) AS global_natural_person_identifier,
	crm.CRM_record_source as rawdv_source_name,
    crm.CRM_natural_person_id as rawdv_source_business_key,
    crm.CRM_natural_person_hash_key as rawdv_hashkey,
   CURRENT_TIMESTAMP
from 
(select hp.person_hash_key as CRM_person_hash_key,  hp.person_id as CRM_person_id, hp.record_source as CRM_record_source,h.natural_person_id as CRM_natural_person_id,h.natural_person_hash_key as CRM_natural_person_hash_key,snp.first_name,snp.last_name, snp.birth_date
from allianz_coe.bvault_silver_raw.hub_person hp
inner join allianz_coe.bvault_silver_raw.sat_person sp
on sp.person_hash_key=hp.person_hash_key
inner join allianz_coe.bvault_silver_raw.link_person_natural_person lnp
on lnp.person_hash_key=hp.person_hash_key
inner Join allianz_coe.bvault_silver_raw.hub_natural_person h
on h.natural_person_hash_key=lnp.natural_person_hash_key
inner join allianz_coe.bvault_silver_raw.sat_natural_person snp
on snp.natural_person_hash_key=h.natural_person_hash_key
) crm
inner join
(select hp.person_hash_key as SAP_person_hash_key,hp.person_id as SAP_person_id,hp.record_source as SAP_record_source,h.natural_person_id as SAP_natural_person_id,h.natural_person_hash_key as SAP_natural_person_hash_key,snp.first_name,snp.last_name, snp.date_of_birth
from allianz_coe.bvault_silver_raw.hub_person hp
inner join allianz_coe.bvault_silver_raw.sat_person_sap sp
on sp.person_hash_key=hp.person_hash_key
inner join allianz_coe.bvault_silver_raw.link_person_natural_person lnp
on lnp.person_hash_key=hp.person_hash_key
inner Join allianz_coe.bvault_silver_raw.hub_natural_person h
on h.natural_person_hash_key=lnp.natural_person_hash_key
inner join allianz_coe.bvault_silver_raw.sat_natural_person_sap snp
on snp.natural_person_hash_key=h.natural_person_hash_key
)sap
on concat(crm.first_name,crm.last_name, crm.birth_date)=concat(sap.first_name,sap.last_name, sap.date_of_birth)


 UNION ALL


 SELECT
   concat(crm.CRM_PERSON_HASH_KEY,'||', sap.SAP_PERSON_HASH_KEY) AS master_natural_person_hash_key,
   concat(crm.CRM_person_id,'||', sap.SAP_person_id) AS global_natural_person_identifier,
	sap.SAP_record_source as rawdv_source_name,
    sap.SAP_natural_person_id as rawdv_source_business_key,
    sap.SAP_natural_person_hash_key as rawdv_hashkey,
   CURRENT_TIMESTAMP
from 
(select hp.person_hash_key as CRM_person_hash_key,hp.person_id as CRM_person_id,hp.record_source as CRM_record_source,h.natural_person_id as CRM_natural_person_id,h.natural_person_hash_key as CRM_natural_person_hash_key,snp.first_name,snp.last_name, snp.birth_date
from allianz_coe.bvault_silver_raw.hub_person hp
inner join allianz_coe.bvault_silver_raw.sat_person sp
on sp.person_hash_key=hp.person_hash_key
inner join allianz_coe.bvault_silver_raw.link_person_natural_person lnp
on lnp.person_hash_key=hp.person_hash_key
inner Join allianz_coe.bvault_silver_raw.hub_natural_person h
on h.natural_person_hash_key=lnp.natural_person_hash_key
inner join allianz_coe.bvault_silver_raw.sat_natural_person snp
on snp.natural_person_hash_key=h.natural_person_hash_key
) crm
inner join
(select hp.person_hash_key as SAP_person_hash_key,hp.person_id as SAP_person_id,hp.record_source as SAP_record_source,h.natural_person_id as SAP_natural_person_id,h.natural_person_hash_key as SAP_natural_person_hash_key,snp.first_name,snp.last_name, snp.date_of_birth
from allianz_coe.bvault_silver_raw.hub_person hp
inner join allianz_coe.bvault_silver_raw.sat_person_sap sp
on sp.person_hash_key=hp.person_hash_key
inner join allianz_coe.bvault_silver_raw.link_person_natural_person lnp
on lnp.person_hash_key=hp.person_hash_key
inner Join allianz_coe.bvault_silver_raw.hub_natural_person h
on h.natural_person_hash_key=lnp.natural_person_hash_key
inner join allianz_coe.bvault_silver_raw.sat_natural_person_sap snp
on snp.natural_person_hash_key=h.natural_person_hash_key
)sap
on concat(crm.first_name,crm.last_name, crm.birth_date)=concat(sap.first_name,sap.last_name, sap.date_of_birth)
;



-------------------------------------------------------------bv_legal_person_master-------------------------------------------------------



 select 
  concat(crm.CRM_PERSON_HASH_KEY,'||', sap.SAP_PERSON_HASH_KEY) AS master_legal_person_hash_key,
    concat(crm.CRM_person_id,'||', sap.SAP_person_id) AS global_legal_person_identifier,
   crm.tenant_id,
   crm.is_lead,
   CASE WHEN sap.person_type IS NOT NULL THEN sap.person_type ELSE crm.type END AS person_type, 
   crm.operational_paperless_consent,
   crm.source_id,
   crm.source_type,
   case when sap.organization is not null then sap.organization else crm.company_name end  as Organization,
   case when sap.org_establishment_date is not null then sap.org_establishment_date else crm.date_of_constitution end  as org_establishment_date,
   case when sap.email_address is not null then sap.email_address else crm.work_email end  as email_address, 
   case when sap.phone_number is not null then sap.phone_number else crm.work_phone end  as phone_number,
    CURRENT_DATE AS EFFECTIVE_FROM,
    TO_DATE('9999-12-31') AS EFFECTIVE_TO,
    'Y' AS CURRENT_ACTIVE_FLAG
from 
(select hp.person_hash_key as CRM_person_hash_key,hp.person_id as CRM_person_id,*
from allianz_coe.bvault_silver_raw.hub_person hp
inner join allianz_coe.bvault_silver_raw.sat_person sp
on sp.person_hash_key=hp.person_hash_key
inner join allianz_coe.bvault_silver_raw.link_person_legal_person lnp
on lnp.person_hash_key=hp.person_hash_key
inner Join allianz_coe.bvault_silver_raw.hub_legal_person h
on h.legal_person_hash_key=lnp.legal_person_hash_key
inner join allianz_coe.bvault_silver_raw.sat_legal_person snp
on snp.legal_person_hash_key=h.legal_person_hash_key
inner join allianz_coe.bvault_silver_raw.link_person_contact lpc
on lpc.person_hash_key=hp.person_hash_key
inner Join allianz_coe.bvault_silver_raw.hub_contact hc
on hc.contact_hash_key=lpc.contact_hash_key
inner join allianz_coe.bvault_silver_raw.sat_contact sc
on sc.contact_hash_key=hc.contact_hash_key

) crm
inner join
(select hp.person_hash_key as SAP_person_hash_key,hp.person_id as SAP_person_id,*
from allianz_coe.bvault_silver_raw.hub_person hp
inner join allianz_coe.bvault_silver_raw.sat_person_sap sp
on sp.person_hash_key=hp.person_hash_key
inner join allianz_coe.bvault_silver_raw.link_person_legal_person lnp
on lnp.person_hash_key=hp.person_hash_key
inner Join allianz_coe.bvault_silver_raw.hub_legal_person h
on h.legal_person_hash_key=lnp.legal_person_hash_key
inner join allianz_coe.bvault_silver_raw.sat_legal_person_sap snp
on snp.legal_person_hash_key=h.legal_person_hash_key
)sap
on concat(crm.company_name,to_date((crm.date_of_constitution),'y-M-d'))=concat(sap.organization,sap.org_establishment_date)
;



------------------------------------------------------------xref_legal_person_master-------------------------------------------------

SELECT
   concat(crm.CRM_PERSON_HASH_KEY,'||', sap.SAP_PERSON_HASH_KEY) AS master_legal_person_hash_key,
   concat(crm.CRM_person_id,'||', sap.SAP_person_id) AS global_legal_person_identifier,
	crm.CRM_record_source as rawdv_source_name,
    crm.CRM_legal_person_id as rawdv_source_business_key,
    crm.CRM_legal_person_hash_key as rawdv_hashkey,
   CURRENT_TIMESTAMP
from 
(select hp.person_hash_key as CRM_person_hash_key,  hp.person_id as CRM_person_id,hp.record_source as CRM_record_source,h.legal_person_id as CRM_legal_person_id,h.legal_person_hash_key as CRM_legal_person_hash_key, snp.company_name,snp.date_of_constitution
from allianz_coe.bvault_silver_raw.hub_person hp
inner join allianz_coe.bvault_silver_raw.sat_person sp
on sp.person_hash_key=hp.person_hash_key
inner join allianz_coe.bvault_silver_raw.link_person_legal_person lnp
on lnp.person_hash_key=hp.person_hash_key
inner Join allianz_coe.bvault_silver_raw.hub_legal_person h
on h.legal_person_hash_key=lnp.legal_person_hash_key
inner join allianz_coe.bvault_silver_raw.sat_legal_person snp
on snp.legal_person_hash_key=h.legal_person_hash_key
) crm
inner join
(select hp.person_hash_key as SAP_person_hash_key,hp.person_id as SAP_person_id,hp.record_source as SAP_record_source,h.legal_person_id as SAP_legal_person_id,h.legal_person_hash_key as SAP_legal_person_hash_key, snp.organization,snp.org_establishment_date
from allianz_coe.bvault_silver_raw.hub_person hp
inner join allianz_coe.bvault_silver_raw.sat_person_sap sp
on sp.person_hash_key=hp.person_hash_key
inner join allianz_coe.bvault_silver_raw.link_person_legal_person lnp
on lnp.person_hash_key=hp.person_hash_key
inner Join allianz_coe.bvault_silver_raw.hub_legal_person h
on h.legal_person_hash_key=lnp.legal_person_hash_key
inner join allianz_coe.bvault_silver_raw.sat_legal_person_sap snp
on snp.legal_person_hash_key=h.legal_person_hash_key
)sap
on concat(crm.company_name,to_date((crm.date_of_constitution),'y-M-d'))=concat(sap.organization,sap.org_establishment_date)


 UNION ALL


 SELECT
   concat(crm.CRM_PERSON_HASH_KEY,'||', sap.SAP_PERSON_HASH_KEY) AS master_legal_person_hash_key,
   concat(crm.CRM_person_id,'||', sap.SAP_person_id) AS global_legal_person_identifier,
	sap.SAP_record_source as rawdv_source_name,
    sap.SAP_legal_person_id as rawdv_source_business_key,
    sap.SAP_legal_person_hash_key as rawdv_hashkey,
   CURRENT_TIMESTAMP
from 
(select hp.person_hash_key as CRM_person_hash_key,  hp.person_id as CRM_person_id,hp.record_source as CRM_record_source,h.legal_person_id as CRM_legal_person_id,h.legal_person_hash_key as CRM_legal_person_hash_key, snp.company_name,snp.date_of_constitution
from allianz_coe.bvault_silver_raw.hub_person hp
inner join allianz_coe.bvault_silver_raw.sat_person sp
on sp.person_hash_key=hp.person_hash_key
inner join allianz_coe.bvault_silver_raw.link_person_legal_person lnp
on lnp.person_hash_key=hp.person_hash_key
inner Join allianz_coe.bvault_silver_raw.hub_legal_person h
on h.legal_person_hash_key=lnp.legal_person_hash_key
inner join allianz_coe.bvault_silver_raw.sat_legal_person snp
on snp.legal_person_hash_key=h.legal_person_hash_key
) crm
inner join
(select hp.person_hash_key as SAP_person_hash_key,hp.person_id as SAP_person_id,SAP_person_id,hp.record_source as SAP_record_source,h.legal_person_id as SAP_legal_person_id,h.legal_person_hash_key as SAP_legal_person_hash_key, snp.organization,snp.org_establishment_date
from allianz_coe.bvault_silver_raw.hub_person hp
inner join allianz_coe.bvault_silver_raw.sat_person_sap sp
on sp.person_hash_key=hp.person_hash_key
inner join allianz_coe.bvault_silver_raw.link_person_legal_person lnp
on lnp.person_hash_key=hp.person_hash_key
inner Join allianz_coe.bvault_silver_raw.hub_legal_person h
on h.legal_person_hash_key=lnp.legal_person_hash_key
inner join allianz_coe.bvault_silver_raw.sat_legal_person_sap snp
on snp.legal_person_hash_key=h.legal_person_hash_key
)sap
on concat(crm.company_name,to_date((crm.date_of_constitution),'y-M-d'))=concat(sap.organization,sap.org_establishment_date)
;


---------------------------------------------------------bv_address_master-------------------------------------------------------------------------------------------


SELECT
     CONCAT(crm.CRM_ADDRESS_HASH_KEY,'||', sap.SAP_ADDRESS_HASH_KEY) 				AS master_address_hash_key,
     CONCAT(crm.CRM_Address_id,'||', sap.SAP_Address_id) 							AS global_address_identifier,
     CONCAT(crm.CRM_person_id,'||', sap.SAP_person_id) 								AS global_person_identifier,
     crm.type 																		AS ADDRESS_TYPE,
     sap.ADDRESS_LINE_1 															AS ADDRESS_LINE_2,
     CONCAT(sap.ADDRESS_LINE_2,',',crm.STREET) 										AS ADDRESS_LINE_2,
     CASE WHEN crm.CITY IS NOT NULL THEN crm.CITY ELSE sap.CITY END  				AS CITY,
     CASE WHEN crm.STATE IS NOT NULL THEN crm.STATE ELSE sap.STATE END  			AS STATE,
     CASE WHEN crm.POSTCODE IS NOT NULL THEN crm.POSTCODE ELSE sap.ZIPCODE END  	AS POSTAL_CODE,
     CASE WHEN crm.COUNTRY IS NOT NULL THEN crm.COUNTRY ELSE sap.COUNTRY END  		AS COUNTRY,
     crm.REGION 																	AS REGION,
     CURRENT_DATE 																	AS EFFECTIVE_FROM,
     TO_DATE('9999-12-31') 															AS EFFECTIVE_TO,
     'Y' 																			AS CURRENT_ACTIVE_FLAG
FROM 
	(
		SELECT
			hc.address_hash_key AS CRM_address_hash_key,
			hc.address_id 		AS CRM_address_id, 
			hp.person_id 		AS CRM_person_id,
			*
		FROM 
			allianz_coe.bvault_silver_raw.hub_persON hp
		LEFT JOIN allianz_coe.bvault_silver_raw.sat_persON sp
			ON sp.person_hash_key=hp.person_hash_key
		LEFT JOIN allianz_coe.bvault_silver_raw.link_person_natural_persON lnp
			ON lnp.person_hash_key=hp.person_hash_key
		LEFT JOIN allianz_coe.bvault_silver_raw.hub_natural_persON h
			ON h.natural_person_hash_key=lnp.natural_person_hash_key
		LEFT JOIN allianz_coe.bvault_silver_raw.sat_natural_persON snp
			ON snp.natural_person_hash_key=h.natural_person_hash_key
		LEFT JOIN allianz_coe.bvault_silver_raw.link_person_legal_persON llp
			ON llp.person_hash_key=hp.person_hash_key
		LEFT JOIN allianz_coe.bvault_silver_raw.hub_legal_persON hl
			ON hl.legal_person_hash_key=llp.legal_person_hash_key
		LEFT JOIN allianz_coe.bvault_silver_raw.sat_legal_persON slp
			ON slp.legal_person_hash_key=hl.legal_person_hash_key
		INNER JOIN allianz_coe.bvault_silver_raw.link_person_home_address lpa
			ON lpa.person_hash_key=hp.person_hash_key
		INNER JOIN allianz_coe.bvault_silver_raw.hub_home_address hc
			ON hc.address_hash_key=lpa.home_address_hash_key
		INNER JOIN allianz_coe.bvault_silver_raw.sat_home_address sc
			ON sc.home_address_hash_key=hc.address_hash_key

	) crm

INNER JOIN

	(
		SELECT 
			hc.address_hash_key AS SAP_address_hash_key,
			hc.address_id 		AS SAP_address_id,
			hp.person_id 		AS SAP_person_id,
			*
		FROM 
			allianz_coe.bvault_silver_raw.hub_persON hp
		LEFT JOIN allianz_coe.bvault_silver_raw.sat_person_sap sp
			ON sp.person_hash_key=hp.person_hash_key
		LEFT JOIN allianz_coe.bvault_silver_raw.link_person_natural_persON lnp
			ON lnp.person_hash_key=hp.person_hash_key
		LEFT JOIN allianz_coe.bvault_silver_raw.hub_natural_persON h
			ON h.natural_person_hash_key=lnp.natural_person_hash_key
		LEFT JOIN allianz_coe.bvault_silver_raw.sat_natural_person_sap snp
			ON snp.natural_person_hash_key=h.natural_person_hash_key
		LEFT JOIN allianz_coe.bvault_silver_raw.link_person_legal_persON llp
			ON llp.person_hash_key=hp.person_hash_key
		LEFT JOIN allianz_coe.bvault_silver_raw.hub_legal_persON hl
			ON hl.legal_person_hash_key=llp.legal_person_hash_key
		LEFT JOIN allianz_coe.bvault_silver_raw.sat_legal_person_sap slp
			ON slp.legal_person_hash_key=hl.legal_person_hash_key
		INNER JOIN allianz_coe.bvault_silver_raw.link_person_home_address lpa
			ON lpa.person_hash_key=hp.person_hash_key
		INNER JOIN allianz_coe.bvault_silver_raw.hub_home_address hc
			ON hc.address_hash_key=lpa.home_address_hash_key
		INNER JOIN allianz_coe.bvault_silver_raw.sat_address_sap sc
			ON sc.address_hash_key=hc.address_hash_key
	) sap
	
ON  ( 
		CONCAT(crm.first_name,crm.last_name, crm.birth_date) = CONCAT(sap.first_name,sap.last_name, sap.date_of_birth)
		OR
		CONCAT(crm.company_name,TO_DATE((crm.date_of_constitution),'y-M-d')) = CONCAT(sap.organization,sap.org_establishment_date)
	)

;


---------------------------------------------------xref_address_master------------------------------------------------------------------------




SELECT
    CONCAT(crm.CRM_ADDRESS_HASH_KEY,'||', sap.SAP_ADDRESS_HASH_KEY)  	AS master_address_hash_key,
    CONCAT(crm.CRM_Address_id,'||', sap.SAP_Address_id) 				AS global_address_identifier,
	crm.CRM_record_source 												AS rawdv_source_name,
    crm.CRM_address_id 													AS rawdv_source_business_key,
    crm.CRM_address_hash_key 											AS rawdv_hashkey,
    CURRENT_TIMESTAMP
FROM 
	(
		SELECT
			hc.record_source 		AS CRM_record_source,
			hc.address_hash_key 	AS CRM_address_hash_key,
			hc.address_id 			AS CRM_address_id, 
			hp.person_id 			AS CRM_person_id,
			*
		FROM 
			allianz_coe.bvault_silver_raw.hub_persON hp
		LEFT JOIN allianz_coe.bvault_silver_raw.sat_persON sp
			ON sp.person_hash_key=hp.person_hash_key
		LEFT JOIN allianz_coe.bvault_silver_raw.link_person_natural_persON lnp
			ON lnp.person_hash_key=hp.person_hash_key
		LEFT JOIN allianz_coe.bvault_silver_raw.hub_natural_persON h
			ON h.natural_person_hash_key=lnp.natural_person_hash_key
		LEFT JOIN allianz_coe.bvault_silver_raw.sat_natural_persON snp
			ON snp.natural_person_hash_key=h.natural_person_hash_key
		LEFT JOIN allianz_coe.bvault_silver_raw.link_person_legal_persON llp
			ON llp.person_hash_key=hp.person_hash_key
		LEFT JOIN allianz_coe.bvault_silver_raw.hub_legal_persON hl
			ON hl.legal_person_hash_key=llp.legal_person_hash_key
		LEFT JOIN allianz_coe.bvault_silver_raw.sat_legal_persON slp
			ON slp.legal_person_hash_key=hl.legal_person_hash_key
		INNER JOIN allianz_coe.bvault_silver_raw.link_person_home_address lpa
			ON lpa.person_hash_key=hp.person_hash_key
		INNER JOIN allianz_coe.bvault_silver_raw.hub_home_address hc
			ON hc.address_hash_key=lpa.home_address_hash_key
		INNER JOIN allianz_coe.bvault_silver_raw.sat_home_address sc
			ON sc.home_address_hash_key=hc.address_hash_key

	) crm

INNER JOIN

	(
		SELECT 
			hc.record_source 		AS CRM_record_source,
			hc.address_hash_key 	AS SAP_address_hash_key,
			hc.address_id 			AS SAP_address_id,
			hp.person_id 			AS SAP_person_id,
			*
		FROM 
			allianz_coe.bvault_silver_raw.hub_persON hp
		LEFT JOIN allianz_coe.bvault_silver_raw.sat_person_sap sp
			ON sp.person_hash_key=hp.person_hash_key
		LEFT JOIN allianz_coe.bvault_silver_raw.link_person_natural_persON lnp
			ON lnp.person_hash_key=hp.person_hash_key
		LEFT JOIN allianz_coe.bvault_silver_raw.hub_natural_persON h
			ON h.natural_person_hash_key=lnp.natural_person_hash_key
		LEFT JOIN allianz_coe.bvault_silver_raw.sat_natural_person_sap snp
			ON snp.natural_person_hash_key=h.natural_person_hash_key
		LEFT JOIN allianz_coe.bvault_silver_raw.link_person_legal_persON llp
			ON llp.person_hash_key=hp.person_hash_key
		LEFT JOIN allianz_coe.bvault_silver_raw.hub_legal_persON hl
			ON hl.legal_person_hash_key=llp.legal_person_hash_key
		LEFT JOIN allianz_coe.bvault_silver_raw.sat_legal_person_sap slp
			ON slp.legal_person_hash_key=hl.legal_person_hash_key
		INNER JOIN allianz_coe.bvault_silver_raw.link_person_home_address lpa
			ON lpa.person_hash_key=hp.person_hash_key
		INNER JOIN allianz_coe.bvault_silver_raw.hub_home_address hc
			ON hc.address_hash_key=lpa.home_address_hash_key
		INNER JOIN allianz_coe.bvault_silver_raw.sat_address_sap sc
			ON sc.address_hash_key=hc.address_hash_key
	) sap
	
ON  ( 
		CONCAT(crm.first_name,crm.last_name, crm.birth_date) = CONCAT(sap.first_name,sap.last_name, sap.date_of_birth)
		OR
		CONCAT(crm.company_name,TO_DATE((crm.date_of_constitution),'y-M-d')) = CONCAT(sap.organization,sap.org_establishment_date)
	)


UNION ALL


SELECT
    CONCAT(crm.CRM_ADDRESS_HASH_KEY,'||', sap.SAP_ADDRESS_HASH_KEY) 	AS master_address_hash_key,
    CONCAT(crm.CRM_Address_id,'||', sap.SAP_Address_id) 				AS global_address_identifier,
	sap.SAP_record_source 												AS rawdv_source_name,
    sap.SAP_address_id 													AS rawdv_source_business_key,
    sap.SAP_address_hash_key 											AS rawdv_hashkey,
    CURRENT_TIMESTAMP
FROM 
	(
		SELECT
			hc.record_source 		AS CRM_record_source,
			hc.address_hash_key 	AS CRM_address_hash_key,
			hc.address_id 			AS CRM_address_id, 
			hp.person_id 			AS CRM_person_id,
			*
		FROM 
			allianz_coe.bvault_silver_raw.hub_persON hp
		LEFT JOIN allianz_coe.bvault_silver_raw.sat_persON sp
			ON sp.person_hash_key=hp.person_hash_key
		LEFT JOIN allianz_coe.bvault_silver_raw.link_person_natural_persON lnp
			ON lnp.person_hash_key=hp.person_hash_key
		LEFT JOIN allianz_coe.bvault_silver_raw.hub_natural_persON h
			ON h.natural_person_hash_key=lnp.natural_person_hash_key
		LEFT JOIN allianz_coe.bvault_silver_raw.sat_natural_persON snp
			ON snp.natural_person_hash_key=h.natural_person_hash_key
		LEFT JOIN allianz_coe.bvault_silver_raw.link_person_legal_persON llp
			ON llp.person_hash_key=hp.person_hash_key
		LEFT JOIN allianz_coe.bvault_silver_raw.hub_legal_persON hl
			ON hl.legal_person_hash_key=llp.legal_person_hash_key
		LEFT JOIN allianz_coe.bvault_silver_raw.sat_legal_persON slp
			ON slp.legal_person_hash_key=hl.legal_person_hash_key
		INNER JOIN allianz_coe.bvault_silver_raw.link_person_home_address lpa
			ON lpa.person_hash_key=hp.person_hash_key
		INNER JOIN allianz_coe.bvault_silver_raw.hub_home_address hc
			ON hc.address_hash_key=lpa.home_address_hash_key
		INNER JOIN allianz_coe.bvault_silver_raw.sat_home_address sc
			ON sc.home_address_hash_key=hc.address_hash_key

	) crm

INNER JOIN

	(
		SELECT 
			hc.record_source 		AS CRM_record_source,
			hc.address_hash_key 	AS SAP_address_hash_key,
			hc.address_id 			AS SAP_address_id,
			hp.person_id 			AS SAP_person_id,
			*
		FROM 
			allianz_coe.bvault_silver_raw.hub_persON hp
		LEFT JOIN allianz_coe.bvault_silver_raw.sat_person_sap sp
			ON sp.person_hash_key=hp.person_hash_key
		LEFT JOIN allianz_coe.bvault_silver_raw.link_person_natural_persON lnp
			ON lnp.person_hash_key=hp.person_hash_key
		LEFT JOIN allianz_coe.bvault_silver_raw.hub_natural_persON h
			ON h.natural_person_hash_key=lnp.natural_person_hash_key
		LEFT JOIN allianz_coe.bvault_silver_raw.sat_natural_person_sap snp
			ON snp.natural_person_hash_key=h.natural_person_hash_key
		LEFT JOIN allianz_coe.bvault_silver_raw.link_person_legal_persON llp
			ON llp.person_hash_key=hp.person_hash_key
		LEFT JOIN allianz_coe.bvault_silver_raw.hub_legal_persON hl
			ON hl.legal_person_hash_key=llp.legal_person_hash_key
		LEFT JOIN allianz_coe.bvault_silver_raw.sat_legal_person_sap slp
			ON slp.legal_person_hash_key=hl.legal_person_hash_key
		INNER JOIN allianz_coe.bvault_silver_raw.link_person_home_address lpa
			ON lpa.person_hash_key=hp.person_hash_key
		INNER JOIN allianz_coe.bvault_silver_raw.hub_home_address hc
			ON hc.address_hash_key=lpa.home_address_hash_key
		INNER JOIN allianz_coe.bvault_silver_raw.sat_address_sap sc
			ON sc.address_hash_key=hc.address_hash_key
	) sap
	
ON  ( 
		CONCAT(crm.first_name,crm.last_name, crm.birth_date) = CONCAT(sap.first_name,sap.last_name, sap.date_of_birth)
		OR
		CONCAT(crm.company_name,TO_DATE((crm.date_of_constitution),'y-M-d')) = CONCAT(sap.organization,sap.org_establishment_date)
	)
	
;


----------------------------------------------------bv_Product_master--------------------------------------------------------------

select 
        concat(crm.CRM_PRODUCT_HASH_KEY,'||', sap.SAP_PRODUCT_HASH_KEY) AS master_product_hash_key,
        concat(crm.CRM_product_id,'||', sap.SAP_product_id) AS global_product_identifier,
        sap.product_type,
        sap.product_sub_type,
        case when sap.product_name is not null then sap.product_name else crm.product_name end  AS product_name,
        crm.product_launch_date,
        crm.product_status,
        sap.line_of_business as product_line_of_business,
        crm.underwriting_group,
        crm.regulatory_approval_code,
        CURRENT_DATE AS EFFECTIVE_FROM,
        TO_DATE('9999-12-31') AS EFFECTIVE_TO,
        'Y' AS CURRENT_ACTIVE_FLAG
from 
(select hc.product_hash_key as CRM_PRODUCT_HASH_KEY,hc.product_id as CRM_PRODUCT_ID,* from 
allianz_coe.bvault_silver_raw.hub_product hc
inner join allianz_coe.bvault_silver_raw.sat_product sc
on sc.product_hash_key=hc.product_hash_key
)crm
inner join
(select hc.product_hash_key as SAP_PRODUCT_HASH_KEY,hc.product_id as SAP_PRODUCT_ID,* from 
allianz_coe.bvault_silver_raw.hub_product hc
inner join allianz_coe.bvault_silver_raw.sat_product_sap sc
on sc.product_hash_key=hc.product_hash_key
)sap 
on concat(crm.type,crm.product_variant,crm.product_name)=concat(sap.product_type, sap.product_sub_type,sap.product_name);



------------------------------------------------------xref_product_master------------------------------------------------------------


select 
        concat(crm.CRM_PRODUCT_HASH_KEY,'||', sap.SAP_PRODUCT_HASH_KEY) AS master_product_hash_key,
        concat(crm.CRM_product_id,'||', sap.SAP_product_id) AS global_product_identifier,
        crm.CRM_record_source as rawdv_source_name,
        crm.CRM_product_id as rawdv_source_business_key,
        crm.CRM_product_hash_key as rawdv_hashkey,
        CURRENT_TIMESTAMP
from 
(select hc.product_hash_key as CRM_PRODUCT_HASH_KEY,hc.product_id as CRM_PRODUCT_ID, hc.record_source as CRM_record_source, sc.type,sc.product_variant,sc.product_name 
from allianz_coe.bvault_silver_raw.hub_product hc
inner join allianz_coe.bvault_silver_raw.sat_product sc
on sc.product_hash_key=hc.product_hash_key
)crm
inner join
(select hc.product_hash_key as SAP_PRODUCT_HASH_KEY,hc.product_id as SAP_PRODUCT_ID, hc.record_source as SAP_record_source, sc.product_type, sc.product_sub_type,sc.product_name  
from allianz_coe.bvault_silver_raw.hub_product hc
inner join allianz_coe.bvault_silver_raw.sat_product_sap sc
on sc.product_hash_key=hc.product_hash_key
)sap 
on concat(crm.type,crm.product_variant,crm.product_name)=concat(sap.product_type, sap.product_sub_type,sap.product_name)


UNION all

select 
        concat(crm.CRM_PRODUCT_HASH_KEY,'||', sap.SAP_PRODUCT_HASH_KEY) AS master_product_hash_key,
        concat(crm.CRM_product_id,'||', sap.SAP_product_id) AS global_product_identifier,
        sap.SAP_record_source as rawdv_source_name,
        sap.SAP_product_id as rawdv_source_business_key,
        sap.SAP_product_hash_key as rawdv_hashkey,
        CURRENT_TIMESTAMP
        
from 
(select hc.product_hash_key as CRM_PRODUCT_HASH_KEY,hc.product_id as CRM_PRODUCT_ID,hc.record_source as CRM_record_source, sc.type,sc.product_variant,sc.product_name 
from allianz_coe.bvault_silver_raw.hub_product hc
inner join allianz_coe.bvault_silver_raw.sat_product sc
on sc.product_hash_key=hc.product_hash_key
)crm
inner join
(select hc.product_hash_key as SAP_PRODUCT_HASH_KEY,hc.product_id as SAP_PRODUCT_ID,hc.record_source as SAP_record_source, sc.product_type, sc.product_sub_type,sc.product_name 
from allianz_coe.bvault_silver_raw.hub_product hc
inner join allianz_coe.bvault_silver_raw.sat_product_sap sc
on sc.product_hash_key=hc.product_hash_key
)sap 
on concat(crm.type,crm.product_variant,crm.product_name)=concat(sap.product_type, sap.product_sub_type,sap.product_name);


------------------------------------------------------bv_motor_master-------------------------------------------------------------


select 
          concat(crm.CRM_MOTOR_HASH_KEY,'||', sap.SAP_MOTOR_HASH_KEY) AS master_motor_hash_key,
          concat(crm.CRM_Motor_id,'||', sap.SAP_Motor_id) AS global_motor_identifier,
          concat(crm.CRM_product_id,'||', sap.SAP_product_id) AS global_product_identifier,
          concat(crm.CRM_policy_id,'||', sap.SAP_policy_id) AS policy_identifier,
          case when sap.motor_class is not null then sap.motor_class else crm.vehicle_class end  as motor_class,
          case when sap.motor_model is not null then sap.motor_class else crm.vehicle_model end  as motor_model,
          case when sap.motor_type is not null then sap.motor_type else crm.vehicle_type end  as motor_type,
          crm.variant as Motor_Variant,
          crm.body_type as body_type,
          case when sap.fuel_type is not null then sap.fuel_type else crm.fuel_type end as fuel_type,
          sap.gear_type as gear_type,
          sap.body_colour as body_colour,
          sap.motor_parked_location as motor_parked_location,
          crm.vehicle_regstate as motor_registration_state,
          case when sap.manufacturing_date is not null then sap.manufacturing_date else crm.vehicle_year end as motor_manufacturing_date,
          crm.vehicle_age as Motor_Age,
          crm.risk_class_code as  Motor_Risk_Class_Code,
          crm.vehicle_owner_type as  Motor_Owner_Type,
          crm.driver_experience_years as  Driver_Experience_Years,
          crm.License_Status  as  License_Status,
          crm.motor_sum_insrd  as  Motor_Sum_Insured,
          crm.Is_Existing_Motor_Customer as  Is_Existing_Motor_Customer,
          crm.Motor_Lapsed_Policies as  Motor_Lapsed_Policies,
        CURRENT_DATE AS EFFECTIVE_FROM,
        TO_DATE('9999-12-31') AS EFFECTIVE_TO,
        'Y' AS CURRENT_ACTIVE_FLAG
from 
(
select motor.motor_hash_key as CRM_motor_hash_key,motor.insured_object_motor_id as CRM_motor_id, hpro.product_id as CRM_product_id, hpo.policy_id as CRM_policy_id,*
from allianz_coe.bvault_silver_raw.hub_person hp
left join allianz_coe.bvault_silver_raw.sat_person sp
on sp.person_hash_key=hp.person_hash_key
left join allianz_coe.bvault_silver_raw.link_person_natural_person lnp
on lnp.person_hash_key=hp.person_hash_key
left Join allianz_coe.bvault_silver_raw.hub_natural_person h
on h.natural_person_hash_key=lnp.natural_person_hash_key
left join allianz_coe.bvault_silver_raw.sat_natural_person snp
on snp.natural_person_hash_key=h.natural_person_hash_key
left join allianz_coe.bvault_silver_raw.link_person_legal_person llp
on llp.person_hash_key=hp.person_hash_key
left Join allianz_coe.bvault_silver_raw.hub_legal_person hl
on hl.legal_person_hash_key=llp.legal_person_hash_key
left join allianz_coe.bvault_silver_raw.sat_legal_person slp
on slp.legal_person_hash_key=hl.legal_person_hash_key
inner join allianz_coe.bvault_silver_raw.link_customer_person lcp
on lcp.person_hash_key=hp.person_hash_key
inner join allianz_coe.bvault_silver_raw.link_policy_customer lpc
on lpc.customer_hash_key=lcp.customer_hash_key
inner join allianz_coe.bvault_silver_raw.hub_policy hpo
on hpo.policy_hash_key=lpc.policy_hash_key
inner join  allianz_coe.bvault_silver_raw.link_policy_product lpp
on lpp.policy_hash_key=hpo.policy_hash_key
inner join  allianz_coe.bvault_silver_raw.hub_product hpro
on hpro.product_hash_key=lpp.product_hash_key
inner join allianz_coe.bvault_silver_raw.link_product_motor lpm
on lpm.product_hash_key=hpro.product_hash_key
inner join allianz_coe.bvault_silver_raw.hub_motor motor
on motor.motor_hash_key = lpm.motor_hash_key
inner join allianz_coe.bvault_silver_raw.sat_motor smotor
on smotor.motor_hash_key=motor.motor_hash_key


--inner join allianz_coe.bvault_silver_raw.link_policy_insured_object lpio
--on lpio.policy_hash_key=hpo.policy_hash_key
--inner join allianz_coe.bvault_silver_raw.link_insured_object_home lioh
-- on lioh.insured_object_hash_key=lpio.insured_object_hash_key
-- inner join allianz_coe.bvault_silver_raw.hub_motor motor
-- on motor.motor_hash_key = lioh.motor_hash_key
-- inner join allianz_coe.bvault_silver_raw.sat_motor smotor
-- on smotor.motor_hash_key=motor.motor_hash_key

) crm
inner join
(select motor.motor_hash_key as SAP_motor_hash_key,motor.insured_object_motor_id as SAP_motor_id,hpro.product_id as SAP_product_id, hpo.policy_id as SAP_policy_id,*
from allianz_coe.bvault_silver_raw.hub_person hp
left join allianz_coe.bvault_silver_raw.sat_person_sap sp
on sp.person_hash_key=hp.person_hash_key
left join allianz_coe.bvault_silver_raw.link_person_natural_person lnp
on lnp.person_hash_key=hp.person_hash_key
left Join allianz_coe.bvault_silver_raw.hub_natural_person h
on h.natural_person_hash_key=lnp.natural_person_hash_key
left join allianz_coe.bvault_silver_raw.sat_natural_person_sap snp
on snp.natural_person_hash_key=h.natural_person_hash_key
left join allianz_coe.bvault_silver_raw.link_person_legal_person llp
on llp.person_hash_key=hp.person_hash_key
left Join allianz_coe.bvault_silver_raw.hub_legal_person hl
on hl.legal_person_hash_key=llp.legal_person_hash_key
left join allianz_coe.bvault_silver_raw.sat_legal_person_sap slp
on slp.legal_person_hash_key=hl.legal_person_hash_key
inner join allianz_coe.bvault_silver_raw.link_customer_person lcp
on lcp.person_hash_key=hp.person_hash_key
inner join allianz_coe.bvault_silver_raw.link_policy_customer lpc
on lpc.customer_hash_key=lcp.customer_hash_key
inner join allianz_coe.bvault_silver_raw.hub_policy hpo
on hpo.policy_hash_key=lpc.policy_hash_key
inner join  allianz_coe.bvault_silver_raw.link_policy_product lpp
on lpp.policy_hash_key=hpo.policy_hash_key
inner join  allianz_coe.bvault_silver_raw.hub_product hpro
on hpro.product_hash_key=lpp.product_hash_key
inner join allianz_coe.bvault_silver_raw.link_product_motor lpm
on lpm.product_hash_key=hpro.product_hash_key
inner join allianz_coe.bvault_silver_raw.hub_motor motor
on motor.motor_hash_key = lpm.motor_hash_key
inner join allianz_coe.bvault_silver_raw.sat_motor_sap smotor
on smotor.motor_hash_key=motor.motor_hash_key

-- inner join allianz_coe.bvault_silver_raw.link_policy_insured_object lpio
-- on lpio.policy_hash_key=hpo.policy_hash_key
-- inner join allianz_coe.bvault_silver_raw.link_insured_object_home lioh
-- on lioh.insured_object_hash_key=lpio.insured_object_hash_key
-- inner join allianz_coe.bvault_silver_raw.hub_motor motor
-- on motor.motor_hash_key = lioh.motor_hash_key
-- inner join allianz_coe.bvault_silver_raw.sat_motor_sap smotor
-- on smotor.motor_hash_key=motor.motor_hash_key
)sap
on (concat(crm.first_name,crm.last_name, crm.birth_date)=concat(sap.first_name,sap.last_name, sap.date_of_birth) 
    OR 
    concat(crm.company_name,to_date((crm.date_of_constitution),'y-M-d'))=concat(sap.organization,sap.org_establishment_date)
)
;








------------------------------------------------------xref_motor_master-----------------------------------------------------------





select  concat(crm.CRM_motor_HASH_KEY,'||', sap.SAP_motor_HASH_KEY) AS master_motor_hash_key,
        concat(crm.CRM_motor_id,'||', sap.SAP_motor_id) AS global_motor_identifier,
        crm.CRM_record_source as rawdv_source_name,
        crm.CRM_motor_id as rawdv_source_business_key,
        crm.CRM_motor_HASH_KEY as rawdv_hash_key,
        CURRENT_TIMESTAMP
from 
(select  motor.motor_hash_key as CRM_motor_hash_key,motor.insured_object_motor_id as CRM_motor_id, motor.record_source as CRM_record_source,*
from allianz_coe.bvault_silver_raw.hub_person hp
left join allianz_coe.bvault_silver_raw.sat_person sp
on sp.person_hash_key=hp.person_hash_key
left join allianz_coe.bvault_silver_raw.link_person_natural_person lnp
on lnp.person_hash_key=hp.person_hash_key
left Join allianz_coe.bvault_silver_raw.hub_natural_person h
on h.natural_person_hash_key=lnp.natural_person_hash_key
left join allianz_coe.bvault_silver_raw.sat_natural_person snp
on snp.natural_person_hash_key=h.natural_person_hash_key
left join allianz_coe.bvault_silver_raw.link_person_legal_person llp
on llp.person_hash_key=hp.person_hash_key
left Join allianz_coe.bvault_silver_raw.hub_legal_person hl
on hl.legal_person_hash_key=llp.legal_person_hash_key
left join allianz_coe.bvault_silver_raw.sat_legal_person slp
on slp.legal_person_hash_key=hl.legal_person_hash_key
inner join allianz_coe.bvault_silver_raw.link_customer_person lcp
on lcp.person_hash_key=hp.person_hash_key
inner join allianz_coe.bvault_silver_raw.link_policy_customer lpc
on lpc.customer_hash_key=lcp.customer_hash_key
inner join  allianz_coe.bvault_silver_raw.link_policy_product lpp
on lpp.policy_hash_key=lpc.policy_hash_key
inner join allianz_coe.bvault_silver_raw.link_product_motor lpm
on lpm.product_hash_key=lpp.product_hash_key
inner join allianz_coe.bvault_silver_raw.hub_motor motor
on motor.motor_hash_key = lpm.motor_hash_key
inner join allianz_coe.bvault_silver_raw.sat_motor smotor
on smotor.motor_hash_key=motor.motor_hash_key

) crm

inner join

(select  motor.motor_hash_key as SAP_motor_hash_key,motor.insured_object_motor_id as SAP_motor_id,motor.record_source as SAP_record_source,*
from allianz_coe.bvault_silver_raw.hub_person hp
left join allianz_coe.bvault_silver_raw.sat_person_sap sp
on sp.person_hash_key=hp.person_hash_key
left join allianz_coe.bvault_silver_raw.link_person_natural_person lnp
on lnp.person_hash_key=hp.person_hash_key
left Join allianz_coe.bvault_silver_raw.hub_natural_person h
on h.natural_person_hash_key=lnp.natural_person_hash_key
left join allianz_coe.bvault_silver_raw.sat_natural_person_sap snp
on snp.natural_person_hash_key=h.natural_person_hash_key
left join allianz_coe.bvault_silver_raw.link_person_legal_person llp
on llp.person_hash_key=hp.person_hash_key
left Join allianz_coe.bvault_silver_raw.hub_legal_person hl
on hl.legal_person_hash_key=llp.legal_person_hash_key
left join allianz_coe.bvault_silver_raw.sat_legal_person_sap slp
on slp.legal_person_hash_key=hl.legal_person_hash_key
inner join allianz_coe.bvault_silver_raw.link_customer_person lcp
on lcp.person_hash_key=hp.person_hash_key
inner join allianz_coe.bvault_silver_raw.link_policy_customer lpc
on lpc.customer_hash_key=lcp.customer_hash_key
inner join  allianz_coe.bvault_silver_raw.link_policy_product lpp
on lpp.policy_hash_key=lpc.policy_hash_key
inner join allianz_coe.bvault_silver_raw.link_product_motor lpm
on lpm.product_hash_key=lpp.product_hash_key
inner join allianz_coe.bvault_silver_raw.hub_motor motor
on motor.motor_hash_key = lpm.motor_hash_key
inner join allianz_coe.bvault_silver_raw.sat_motor_sap smotor
on smotor.motor_hash_key=motor.motor_hash_key
)sap
on (concat(crm.first_name,crm.last_name, crm.birth_date)=concat(sap.first_name,sap.last_name, sap.date_of_birth) 
    OR 
    concat(crm.company_name,to_date((crm.date_of_constitution),'y-M-d'))=concat(sap.organization,sap.org_establishment_date)
)


UNION all




select  concat(crm.CRM_motor_HASH_KEY,'||', sap.SAP_motor_HASH_KEY) AS master_motor_hash_key,
        concat(crm.CRM_motor_id,'||', sap.SAP_motor_id) AS global_motor_identifier,
        sap.SAP_record_source as rawdv_source_name,
        sap.SAP_motor_id as rawdv_source_business_key,
        sap.SAP_motor_HASH_KEY as rawdv_hash_key,
        CURRENT_TIMESTAMP
from 
(select  motor.motor_hash_key as CRM_motor_hash_key,motor.insured_object_motor_id as CRM_motor_id, motor.record_source as CRM_record_source,*
from allianz_coe.bvault_silver_raw.hub_person hp
left join allianz_coe.bvault_silver_raw.sat_person sp
on sp.person_hash_key=hp.person_hash_key
left join allianz_coe.bvault_silver_raw.link_person_natural_person lnp
on lnp.person_hash_key=hp.person_hash_key
left Join allianz_coe.bvault_silver_raw.hub_natural_person h
on h.natural_person_hash_key=lnp.natural_person_hash_key
left join allianz_coe.bvault_silver_raw.sat_natural_person snp
on snp.natural_person_hash_key=h.natural_person_hash_key
left join allianz_coe.bvault_silver_raw.link_person_legal_person llp
on llp.person_hash_key=hp.person_hash_key
left Join allianz_coe.bvault_silver_raw.hub_legal_person hl
on hl.legal_person_hash_key=llp.legal_person_hash_key
left join allianz_coe.bvault_silver_raw.sat_legal_person slp
on slp.legal_person_hash_key=hl.legal_person_hash_key
inner join allianz_coe.bvault_silver_raw.link_customer_person lcp
on lcp.person_hash_key=hp.person_hash_key
inner join allianz_coe.bvault_silver_raw.link_policy_customer lpc
on lpc.customer_hash_key=lcp.customer_hash_key
inner join  allianz_coe.bvault_silver_raw.link_policy_product lpp
on lpp.policy_hash_key=lpc.policy_hash_key
inner join allianz_coe.bvault_silver_raw.link_product_motor lpm
on lpm.product_hash_key=lpp.product_hash_key
inner join allianz_coe.bvault_silver_raw.hub_motor motor
on motor.motor_hash_key = lpm.motor_hash_key
inner join allianz_coe.bvault_silver_raw.sat_motor smotor
on smotor.motor_hash_key=motor.motor_hash_key

) crm

inner join

(select  motor.motor_hash_key as SAP_motor_hash_key,motor.insured_object_motor_id as SAP_motor_id, motor.record_source as SAP_record_source,*
from allianz_coe.bvault_silver_raw.hub_person hp
left join allianz_coe.bvault_silver_raw.sat_person_sap sp
on sp.person_hash_key=hp.person_hash_key
left join allianz_coe.bvault_silver_raw.link_person_natural_person lnp
on lnp.person_hash_key=hp.person_hash_key
left Join allianz_coe.bvault_silver_raw.hub_natural_person h
on h.natural_person_hash_key=lnp.natural_person_hash_key
left join allianz_coe.bvault_silver_raw.sat_natural_person_sap snp
on snp.natural_person_hash_key=h.natural_person_hash_key
left join allianz_coe.bvault_silver_raw.link_person_legal_person llp
on llp.person_hash_key=hp.person_hash_key
left Join allianz_coe.bvault_silver_raw.hub_legal_person hl
on hl.legal_person_hash_key=llp.legal_person_hash_key
left join allianz_coe.bvault_silver_raw.sat_legal_person_sap slp
on slp.legal_person_hash_key=hl.legal_person_hash_key
inner join allianz_coe.bvault_silver_raw.link_customer_person lcp
on lcp.person_hash_key=hp.person_hash_key
inner join allianz_coe.bvault_silver_raw.link_policy_customer lpc
on lpc.customer_hash_key=lcp.customer_hash_key
inner join  allianz_coe.bvault_silver_raw.link_policy_product lpp
on lpp.policy_hash_key=lpc.policy_hash_key
inner join allianz_coe.bvault_silver_raw.link_product_motor lpm
on lpm.product_hash_key=lpp.product_hash_key
inner join allianz_coe.bvault_silver_raw.hub_motor motor
on motor.motor_hash_key = lpm.motor_hash_key
inner join allianz_coe.bvault_silver_raw.sat_motor_sap smotor
on smotor.motor_hash_key=motor.motor_hash_key
)sap
on (concat(crm.first_name,crm.last_name, crm.birth_date)=concat(sap.first_name,sap.last_name, sap.date_of_birth) 
    OR 
    concat(crm.company_name,to_date((crm.date_of_constitution),'y-M-d'))=concat(sap.organization,sap.org_establishment_date)
)

;







-------------------------------------------------------bv_home_master----------------------------------------------------------



select  concat(crm.CRM_HOME_HASH_KEY,'||', sap.SAP_HOME_HASH_KEY) AS master_home_hash_key,
        concat(crm.CRM_home_id,'||', sap.SAP_home_id) AS global_home_identifier,
        concat(crm.CRM_product_id,'||', sap.SAP_product_id) AS global_product_identifier,
        concat(crm.CRM_policy_id,'||', sap.SAP_policy_id) AS policy_identifier,
        case when CRM.home_type is not null then crm.home_type else sap.home_type end as Home_Type,
       sap.home_location as Home_Location,
       crm.home_state as Home_State,
       case when CRM.wall_construction is not null then crm.wall_construction else sap.wall_type end as Wall_Construction_Type,
       case when CRM.roof_construction is not null then crm.roof_construction else sap.roof_material end as Roof_Construction_Type,
       crm.Is_Existing_Home_Customer as Is_Existing_Home_Customer,
        CURRENT_DATE AS EFFECTIVE_FROM,
        TO_DATE('9999-12-31') AS EFFECTIVE_TO,
        'Y' AS CURRENT_ACTIVE_FLAG
from 
(select  home.home_hash_key as CRM_home_hash_key,home.insured_object_home_id as CRM_home_id,hpro.product_id as CRM_product_id, hpo.policy_id as CRM_policy_id, *
from allianz_coe.bvault_silver_raw.hub_person hp
left join allianz_coe.bvault_silver_raw.sat_person sp
on sp.person_hash_key=hp.person_hash_key
left join allianz_coe.bvault_silver_raw.link_person_natural_person lnp
on lnp.person_hash_key=hp.person_hash_key
left Join allianz_coe.bvault_silver_raw.hub_natural_person h
on h.natural_person_hash_key=lnp.natural_person_hash_key
left join allianz_coe.bvault_silver_raw.sat_natural_person snp
on snp.natural_person_hash_key=h.natural_person_hash_key
left join allianz_coe.bvault_silver_raw.link_person_legal_person llp
on llp.person_hash_key=hp.person_hash_key
left Join allianz_coe.bvault_silver_raw.hub_legal_person hl
on hl.legal_person_hash_key=llp.legal_person_hash_key
left join allianz_coe.bvault_silver_raw.sat_legal_person slp
on slp.legal_person_hash_key=hl.legal_person_hash_key
inner join allianz_coe.bvault_silver_raw.link_customer_person lcp
on lcp.person_hash_key=hp.person_hash_key
inner join allianz_coe.bvault_silver_raw.link_policy_customer lpc
on lpc.customer_hash_key=lcp.customer_hash_key
inner join allianz_coe.bvault_silver_raw.hub_policy hpo
on hpo.policy_hash_key=lpc.policy_hash_key
inner join  allianz_coe.bvault_silver_raw.link_policy_product lpp
on lpp.policy_hash_key=hpo.policy_hash_key
inner join  allianz_coe.bvault_silver_raw.hub_product hpro
on hpro.product_hash_key=lpp.product_hash_key
inner join allianz_coe.bvault_silver_raw.link_product_home lpm
on lpm.product_hash_key=hpro.product_hash_key
inner join allianz_coe.bvault_silver_raw.hub_home home
on home.home_hash_key = lpm.home_hash_key
inner join allianz_coe.bvault_silver_raw.sat_home shome
on shome.home_hash_key=home.home_hash_key



-- inner join allianz_coe.bvault_silver_raw.link_policy_insured_object lpio
-- on lpio.policy_hash_key=hpo.policy_hash_key
-- inner join allianz_coe.bvault_silver_raw.link_insured_object_home lioh
-- on lioh.insured_object_hash_key=lpio.insured_object_hash_key
-- inner join allianz_coe.bvault_silver_raw.hub_home home
-- on home.home_hash_key = lioh.home_hash_key
-- inner join allianz_coe.bvault_silver_raw.sat_home shome
-- on shome.home_hash_key=home.home_hash_key


) crm
inner join
(select  home.home_hash_key as SAP_home_hash_key,home.insured_object_home_id as SAP_home_id,hpro.product_id as SAP_product_id, hpo.policy_id as SAP_policy_id,*
from allianz_coe.bvault_silver_raw.hub_person hp
left join allianz_coe.bvault_silver_raw.sat_person_sap sp
on sp.person_hash_key=hp.person_hash_key
left join allianz_coe.bvault_silver_raw.link_person_natural_person lnp
on lnp.person_hash_key=hp.person_hash_key
left Join allianz_coe.bvault_silver_raw.hub_natural_person h
on h.natural_person_hash_key=lnp.natural_person_hash_key
left join allianz_coe.bvault_silver_raw.sat_natural_person_sap snp
on snp.natural_person_hash_key=h.natural_person_hash_key
left join allianz_coe.bvault_silver_raw.link_person_legal_person llp
on llp.person_hash_key=hp.person_hash_key
left Join allianz_coe.bvault_silver_raw.hub_legal_person hl
on hl.legal_person_hash_key=llp.legal_person_hash_key
left join allianz_coe.bvault_silver_raw.sat_legal_person_sap slp
on slp.legal_person_hash_key=hl.legal_person_hash_key
inner join allianz_coe.bvault_silver_raw.link_customer_person lcp
on lcp.person_hash_key=hp.person_hash_key
inner join allianz_coe.bvault_silver_raw.link_policy_customer lpc
on lpc.customer_hash_key=lcp.customer_hash_key
inner join allianz_coe.bvault_silver_raw.hub_policy hpo
on hpo.policy_hash_key=lpc.policy_hash_key
inner join  allianz_coe.bvault_silver_raw.link_policy_product lpp
on lpp.policy_hash_key=hpo.policy_hash_key
inner join  allianz_coe.bvault_silver_raw.hub_product hpro
on hpro.product_hash_key=lpp.product_hash_key
inner join allianz_coe.bvault_silver_raw.link_product_home lpm
on lpm.product_hash_key=hpro.product_hash_key
inner join allianz_coe.bvault_silver_raw.hub_home home
on home.home_hash_key = lpm.home_hash_key
inner join allianz_coe.bvault_silver_raw.sat_home_sap shome
on shome.home_hash_key=home.home_hash_key




-- inner join allianz_coe.bvault_silver_raw.link_policy_insured_object lpio
-- on lpio.policy_hash_key=hpo.policy_hash_key
-- inner join allianz_coe.bvault_silver_raw.link_insured_object_home lioh
-- on lioh.insured_object_hash_key=lpio.insured_object_hash_key
-- inner join allianz_coe.bvault_silver_raw.hub_home home
-- on home.home_hash_key = lioh.home_hash_key
-- inner join allianz_coe.bvault_silver_raw.sat_home_sap shome
-- on shome.home_hash_key=home.home_hash_key
)sap
on (concat(crm.first_name,crm.last_name, crm.birth_date)=concat(sap.first_name,sap.last_name, sap.date_of_birth) 
    OR 
    concat(crm.company_name,to_date((crm.date_of_constitution),'y-M-d'))=concat(sap.organization,sap.org_establishment_date)
)
;








-------------------------------------------------------xref_home_master------------------------------------------------------







select  concat(crm.CRM_HOME_HASH_KEY,'||', sap.SAP_HOME_HASH_KEY) AS master_home_hash_key,
        concat(crm.CRM_home_id,'||', sap.SAP_home_id) AS global_home_identifier,
        crm.CRM_record_source as rawdv_source_name,
        crm.CRM_home_id as rawdv_source_business_key,
        crm.CRM_HOME_HASH_KEY as rawdv_hash_key,
        CURRENT_TIMESTAMP
from 
(select  home.home_hash_key as CRM_home_hash_key,home.insured_object_home_id as CRM_home_id, home.record_source as CRM_record_source,*
from allianz_coe.bvault_silver_raw.hub_person hp
left join allianz_coe.bvault_silver_raw.sat_person sp
on sp.person_hash_key=hp.person_hash_key
left join allianz_coe.bvault_silver_raw.link_person_natural_person lnp
on lnp.person_hash_key=hp.person_hash_key
left Join allianz_coe.bvault_silver_raw.hub_natural_person h
on h.natural_person_hash_key=lnp.natural_person_hash_key
left join allianz_coe.bvault_silver_raw.sat_natural_person snp
on snp.natural_person_hash_key=h.natural_person_hash_key
left join allianz_coe.bvault_silver_raw.link_person_legal_person llp
on llp.person_hash_key=hp.person_hash_key
left Join allianz_coe.bvault_silver_raw.hub_legal_person hl
on hl.legal_person_hash_key=llp.legal_person_hash_key
left join allianz_coe.bvault_silver_raw.sat_legal_person slp
on slp.legal_person_hash_key=hl.legal_person_hash_key
inner join allianz_coe.bvault_silver_raw.link_customer_person lcp
on lcp.person_hash_key=hp.person_hash_key
inner join allianz_coe.bvault_silver_raw.link_policy_customer lpc
on lpc.customer_hash_key=lcp.customer_hash_key
inner join  allianz_coe.bvault_silver_raw.link_policy_product lpp
on lpp.policy_hash_key=lpc.policy_hash_key
inner join allianz_coe.bvault_silver_raw.link_product_home lpm
on lpm.product_hash_key=lpp.product_hash_key
inner join allianz_coe.bvault_silver_raw.hub_home home
on home.home_hash_key = lpm.home_hash_key
inner join allianz_coe.bvault_silver_raw.sat_home shome
on shome.home_hash_key=home.home_hash_key

) crm

inner join

(select  home.home_hash_key as SAP_home_hash_key,home.insured_object_home_id as SAP_home_id,home.record_source as SAP_record_source,*
from allianz_coe.bvault_silver_raw.hub_person hp
left join allianz_coe.bvault_silver_raw.sat_person_sap sp
on sp.person_hash_key=hp.person_hash_key
left join allianz_coe.bvault_silver_raw.link_person_natural_person lnp
on lnp.person_hash_key=hp.person_hash_key
left Join allianz_coe.bvault_silver_raw.hub_natural_person h
on h.natural_person_hash_key=lnp.natural_person_hash_key
left join allianz_coe.bvault_silver_raw.sat_natural_person_sap snp
on snp.natural_person_hash_key=h.natural_person_hash_key
left join allianz_coe.bvault_silver_raw.link_person_legal_person llp
on llp.person_hash_key=hp.person_hash_key
left Join allianz_coe.bvault_silver_raw.hub_legal_person hl
on hl.legal_person_hash_key=llp.legal_person_hash_key
left join allianz_coe.bvault_silver_raw.sat_legal_person_sap slp
on slp.legal_person_hash_key=hl.legal_person_hash_key
inner join allianz_coe.bvault_silver_raw.link_customer_person lcp
on lcp.person_hash_key=hp.person_hash_key
inner join allianz_coe.bvault_silver_raw.link_policy_customer lpc
on lpc.customer_hash_key=lcp.customer_hash_key
inner join  allianz_coe.bvault_silver_raw.link_policy_product lpp
on lpp.policy_hash_key=lpc.policy_hash_key
inner join allianz_coe.bvault_silver_raw.link_product_home lpm
on lpm.product_hash_key=lpp.product_hash_key
inner join allianz_coe.bvault_silver_raw.hub_home home
on home.home_hash_key = lpm.home_hash_key
inner join allianz_coe.bvault_silver_raw.sat_home_sap shome
on shome.home_hash_key=home.home_hash_key
)sap
on (concat(crm.first_name,crm.last_name, crm.birth_date)=concat(sap.first_name,sap.last_name, sap.date_of_birth) 
    OR 
    concat(crm.company_name,to_date((crm.date_of_constitution),'y-M-d'))=concat(sap.organization,sap.org_establishment_date)
)


UNION all




select  concat(crm.CRM_HOME_HASH_KEY,'||', sap.SAP_HOME_HASH_KEY) AS master_home_hash_key,
        concat(crm.CRM_home_id,'||', sap.SAP_home_id) AS global_home_identifier,
        sap.SAP_record_source as rawdv_source_name,
        sap.SAP_home_id as rawdv_source_business_key,
        sap.SAP_HOME_HASH_KEY as rawdv_hash_key,
        CURRENT_TIMESTAMP
from 
(select  home.home_hash_key as CRM_home_hash_key,home.insured_object_home_id as CRM_home_id, home.record_source as CRM_record_source,*
from allianz_coe.bvault_silver_raw.hub_person hp
left join allianz_coe.bvault_silver_raw.sat_person sp
on sp.person_hash_key=hp.person_hash_key
left join allianz_coe.bvault_silver_raw.link_person_natural_person lnp
on lnp.person_hash_key=hp.person_hash_key
left Join allianz_coe.bvault_silver_raw.hub_natural_person h
on h.natural_person_hash_key=lnp.natural_person_hash_key
left join allianz_coe.bvault_silver_raw.sat_natural_person snp
on snp.natural_person_hash_key=h.natural_person_hash_key
left join allianz_coe.bvault_silver_raw.link_person_legal_person llp
on llp.person_hash_key=hp.person_hash_key
left Join allianz_coe.bvault_silver_raw.hub_legal_person hl
on hl.legal_person_hash_key=llp.legal_person_hash_key
left join allianz_coe.bvault_silver_raw.sat_legal_person slp
on slp.legal_person_hash_key=hl.legal_person_hash_key
inner join allianz_coe.bvault_silver_raw.link_customer_person lcp
on lcp.person_hash_key=hp.person_hash_key
inner join allianz_coe.bvault_silver_raw.link_policy_customer lpc
on lpc.customer_hash_key=lcp.customer_hash_key
inner join  allianz_coe.bvault_silver_raw.link_policy_product lpp
on lpp.policy_hash_key=lpc.policy_hash_key
inner join allianz_coe.bvault_silver_raw.link_product_home lpm
on lpm.product_hash_key=lpp.product_hash_key
inner join allianz_coe.bvault_silver_raw.hub_home home
on home.home_hash_key = lpm.home_hash_key
inner join allianz_coe.bvault_silver_raw.sat_home shome
on shome.home_hash_key=home.home_hash_key

) crm

inner join

(select  home.home_hash_key as SAP_home_hash_key,home.insured_object_home_id as SAP_home_id, home.record_source as SAP_record_source,*
from allianz_coe.bvault_silver_raw.hub_person hp
left join allianz_coe.bvault_silver_raw.sat_person_sap sp
on sp.person_hash_key=hp.person_hash_key
left join allianz_coe.bvault_silver_raw.link_person_natural_person lnp
on lnp.person_hash_key=hp.person_hash_key
left Join allianz_coe.bvault_silver_raw.hub_natural_person h
on h.natural_person_hash_key=lnp.natural_person_hash_key
left join allianz_coe.bvault_silver_raw.sat_natural_person_sap snp
on snp.natural_person_hash_key=h.natural_person_hash_key
left join allianz_coe.bvault_silver_raw.link_person_legal_person llp
on llp.person_hash_key=hp.person_hash_key
left Join allianz_coe.bvault_silver_raw.hub_legal_person hl
on hl.legal_person_hash_key=llp.legal_person_hash_key
left join allianz_coe.bvault_silver_raw.sat_legal_person_sap slp
on slp.legal_person_hash_key=hl.legal_person_hash_key
inner join allianz_coe.bvault_silver_raw.link_customer_person lcp
on lcp.person_hash_key=hp.person_hash_key
inner join allianz_coe.bvault_silver_raw.link_policy_customer lpc
on lpc.customer_hash_key=lcp.customer_hash_key
inner join  allianz_coe.bvault_silver_raw.link_policy_product lpp
on lpp.policy_hash_key=lpc.policy_hash_key
inner join allianz_coe.bvault_silver_raw.link_product_home lpm
on lpm.product_hash_key=lpp.product_hash_key
inner join allianz_coe.bvault_silver_raw.hub_home home
on home.home_hash_key = lpm.home_hash_key
inner join allianz_coe.bvault_silver_raw.sat_home_sap shome
on shome.home_hash_key=home.home_hash_key
)sap
on (concat(crm.first_name,crm.last_name, crm.birth_date)=concat(sap.first_name,sap.last_name, sap.date_of_birth) 
    OR 
    concat(crm.company_name,to_date((crm.date_of_constitution),'y-M-d'))=concat(sap.organization,sap.org_establishment_date)
)

;