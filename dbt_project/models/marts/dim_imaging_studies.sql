{{ config(materialized='table') }}

with imaging_studies as (
    select distinct
        {{ dbt_utils.generate_surrogate_key(['imaging_study_id']) }} as imaging_study_sk,
        imaging_study_id,
        series_uid,
        instance_uid,
        bodysite_code,
        bodysite_description,
        modality_code,
        modality_description,
        sop_code,
        sop_description,
        procedure_code
    from {{ ref('stg_imaging_studies') }}
    where imaging_study_id is not null
)

select
    imaging_study_sk,
    imaging_study_id,
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