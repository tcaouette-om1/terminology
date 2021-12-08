/* params
${old_maps||delivered}$
${new_maps||mapset_20211112}$
${entity||boc_map_medication}$
*/

with del as (
  select * from ${old_maps}$..${entity}$
  minus
  select * from ${new_maps}$..${entity}$
),
added as (
  select * from ${new_maps}$..${entity}$
  minus
  select * from ${old_maps}$..${entity}$
),
meds as 
(
    select 'added' status, * from added
    union
    select 'removed' status, * from del
)
,
code_types as 
(
    select p.*, r.title
    from meds p
    left join delivered.public.ref_boccui r
    on p.code_type = r.code
)
,
--select count(1) from code_types where status = 'removed'
added_codes as 
(
    select status, code_type, title code_type_name, mapped_medication_code med_new, medication_description desc_new, boc_cui boc_new, boc_name boc_name_new
    from code_types
    where status = 'added'
),
removed_codes as 
(
    select status, code_type, title code_type_name, mapped_medication_code med_old, medication_description desc_old, boc_cui boc_old, boc_name boc_name_old
    from code_types
    where status = 'removed'
)
select distinct
med_old,
med_new,
    case 
        when med_old is not null and med_new is not null then 'updated'
        when med_old is not null then 'removed'
        else 'added'
    end status,
    nvl(a.code_type, r.code_type) code_type, nvl(a.code_type_name, r.code_type_name) code_type_name, nvl(a.med_new, r.med_old) med,
    desc_old, desc_new, 
    case when desc_old <> desc_new then 'updated' 
         when desc_old = desc_new then 'not changed' 
         when desc_old is not null and desc_new is null then 'removed'
         when desc_old is null and desc_new is not null then 'added'
    else null end desc_updated, 
    boc_old, boc_new, 
    case when boc_old <> boc_new then 'updated' 
         when boc_old = boc_new then 'not changed' 
         when boc_old is not null and boc_new is null then 'removed'
         when boc_old is null and boc_new is not null then 'added'

    else null end boc_updated,
    boc_name_old, boc_name_new, 
    case when boc_name_old <> boc_name_new then 'updated' 
         when boc_name_old = boc_name_new then 'not changed' 
         when boc_name_old is not null and boc_name_new is null then 'removed'
         when boc_name_old is null and boc_name_new is not null then 'added'

    else null end boc_name_updated
from added_codes a 
full outer join removed_codes r
on 
    nvl(a.med_new, 'x') = nvl(r.med_old, 'x') and 
    nvl(a.code_type_name, 'x') = nvl(r.code_type_name, 'x')
    
    
order by 11, 9, 4