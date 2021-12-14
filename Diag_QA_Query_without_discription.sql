/* params
${old_maps||delivered}$
${new_maps||mapset_20211112}$
${entity||BOC_MAP_DIAGNOSIS_ROLLUP}$
*/
with del as (
  select code_type, mapped_diagnosis_code,boc_cui,boc_name from ${old_maps}$..${entity}$ 
  minus
  select code_type, mapped_diagnosis_code,boc_cui,boc_name from ${new_maps}$..${entity}$
),
added as (
  select code_type, mapped_diagnosis_code,boc_cui,boc_name from ${new_maps}$..${entity}$
  minus
  select code_type, mapped_diagnosis_code,boc_cui,boc_name from ${old_maps}$..${entity}$
),
diags as 
(
    select 'added' status, * from added
    union
    select 'removed' status, * from del
),
code_types as 
(
    select p.*, r.title
    from diags p
    left join delivered.public.ref_boccui r
    on p.code_type = r.code
),
--select count(1) from code_types where status = 'removed'
added_codes as 
(
    select status, code_type, title code_type_name, mapped_diagnosis_code diag_new,  boc_cui boc_new, boc_name boc_name_new --standard_diagnosis_description desc_new,
    from code_types
    where status = 'added'
),
removed_codes as 
(
    select status, code_type, title code_type_name, mapped_diagnosis_code diag_old,  boc_cui boc_old, boc_name boc_name_old --standard_diagnosis_description desc_old,
    from code_types
    where status = 'removed'
),
qa_table as (
select distinct
diag_old,
diag_new,
    case 
        when diag_old is not null and diag_new is not null then 'updated'
        when diag_old is not null then 'removed'
        else 'added'
    end status,
    nvl(a.code_type, r.code_type) code_type, nvl(a.code_type_name, r.code_type_name) code_type_name, nvl(a.diag_new, r.diag_old) diag,
    --desc_old, desc_new, case when desc_old <> desc_new then 'updated' else null end desc_updated,
    boc_old, 
    boc_new, 
        case when boc_old <> boc_new then 'updated' 
         when boc_old = boc_new then 'not changed' 
         when boc_old is not null and boc_new is null then 'removed'
         when boc_old is null and boc_new is not null then 'added'

    else null end boc_status,
    
    boc_name_old,
    boc_name_new, 
    case when boc_name_old <> boc_name_new then 'updated' 
         when boc_name_old = boc_name_new then 'not changed' 
         when boc_name_old is not null and boc_name_new is null then 'removed'
         when boc_name_old is null and boc_name_new is not null then 'added'
   else null end boc_name_status,
    case when boc_status !='' and boc_name_status !='' then concat('BOC_STATUS   ',boc_status,',    BOC_NAME_STATUS    ',boc_name_status) else null 
    end as status_filter

from added_codes a 
full outer join removed_codes r
on 
    nvl(a.diag_new, 'x') = nvl(r.diag_old, 'x') and 
    nvl(a.code_type_name, 'x') = nvl(r.code_type_name, 'x')
    
    order by boc_old, boc_name_new
    )
    select 
    diag_old,
diag_new,
diag,
code_type,
status,
status_filter,
boc_old, 
boc_new,
boc_name_old,
boc_name_new 
from qa_table