with source as (

    select * from {{ source('raw_synthea', 'claims') }}

),

renamed as (

    select
        id as claim_id,
        patientid as patient_id,
        providerid as provider_id,
        primarypatientinsuranceid as primary_patient_insurance_id,
        secondarypatientinsuranceid as secondary_patient_insurance_id,
        departmentid as department_id,
        patientdepartmentid as patient_department_id,
        diagnosis_1,
        diagnosis_2,
        diagnosis_3,
        diagnosis_4,
        diagnosis_5,
        diagnosis_6,
        diagnosis_7,
        diagnosis_8,
        --referringproviderid as referring_provider_id,
        appointmentid as appointment_id,
        cast(currentillnessdate as timestamp) as current_illness_date,
        cast(servicedate as timestamp) as service_date,
        supervisingproviderid as supervising_provider_id,
        status_1,
        status_2,
        statusp as status_p,
        cast(outstanding_1 as numeric) as outstanding_1,
        cast(outstanding_2 as numeric) as outstanding_2,
        cast(outstandingp as numeric) as outstanding_p,
        cast(lastbilleddate_1 as timestamp) as last_billed_date_1,
        cast(lastbilleddate_2 as timestamp) as last_billed_date_2,
        cast(lastbilleddatep as timestamp) as last_billed_date_p,
        cast(healthcareclaimtypeid_1 as integer) as healthcare_claim_type_id_1,
        cast(healthcareclaimtypeid_2 as integer) as healthcare_claim_type_id_2

    from source

)

select * from renamed