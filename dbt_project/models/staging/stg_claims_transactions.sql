with source as (

    select * from {{ source('raw_synthea', 'claims_transactions') }}

),

renamed as (

    select
        id as claim_transaction_id,
        claimid as claim_id,
        chargeid as charge_id,
        patientid as patient_id,
        type as transaction_type,
        cast(amount as numeric) as transaction_amount,
        method as transaction_method,
        cast(fromdate as timestamp) as from_date,
        cast(todate as timestamp) as to_date,
        placeofservice as place_of_service,
        procedurecode as procedure_code,
        --modifier1 as modifier_1,
        --modifier2 as modifier_2,
        diagnosisref_1 as diagnosis_ref_1,
        diagnosisref_2 as diagnosis_ref_2,
        diagnosisref_3 as diagnosis_ref_3,
        diagnosisref_4 as diagnosis_ref_4,
        cast(units as integer) as units,
        departmentid as department_id,
        notes as notes,
        cast(unitamount as numeric) as unit_amount,
        transferoutid as transfer_out_id,
        transfertype as transfer_type,
        cast(payments as numeric) as payments,
        cast(adjustments as numeric) as adjustments,
        cast(transfers as numeric) as transfers,
        cast(outstanding as numeric) as outstanding,
        appointmentid as appointment_id,
        --linenote as line_note,
        patientinsuranceid as patient_insurance_id,
        feescheduleid as fee_schedule_id,
        providerid as provider_id,
        supervisingproviderid as supervising_provider_id

    from source

)

select * from renamed