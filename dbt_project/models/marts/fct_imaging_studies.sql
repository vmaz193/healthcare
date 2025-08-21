{{ config(materialized='table') }}

with imaging_studies as (
    select
        i.imaging_study_id,
        p.patient_sk,
        e.encounter_id,
        cast(i.imaging_study_date as date) as study_date,
        i.series_uid,
        i.instance_uid,
        i.bodysite_code,
        i.bodysite_description,
        i.modality_code,
        i.modality_description,
        i.sop_code,
        i.sop_description,
        i.procedure_code
    from {{ ref('stg_imaging_studies') }} i
    left join {{ ref('dim_patients') }} p on i.patient_id = p.patient_id
    left join {{ ref('fct_encounters') }} e on i.encounter_id = e.encounter_id
    left join {{ ref('dim_imaging_studies') }} di on i.imaging_study_id = di.imaging_study_id
    where p.patient_sk is not null
)

select
    {{ dbt_utils.generate_surrogate_key(['imaging_study_id']) }} as imaging_study_event_sk,
    patient_sk,
    encounter_id,
    study_date,
    series_uid,
    instance_uid,
    bodysite_code,
    bodysite_description,
    modality_code,
    modality_description,
    sop_code,
    sop_description,
    procedure_code
from imaging_studies