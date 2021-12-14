
   
/* params
${old_maps||delivered}$
${new_maps||mapset_20211112}$
${entity||BOC_MAP_PROCEDURE_ROLLUP}$
*/
with del as (
  select code_type, mapped_procedure_code,boc_cui,boc_name from ${old_maps}$..${entity}$
  minus
  select code_type, mapped_procedure_code,boc_cui,boc_name from ${new_maps}$..${entity}$
),
added as (
  select code_type, mapped_procedure_code,boc_cui,boc_name from ${new_maps}$..${entity}$
  minus
  select code_type, mapped_procedure_code,boc_cui,boc_name from ${old_maps}$..${entity}$
),
procs as 
(
    select 'added' status, * from added
    union
    select 'removed' status, * from del
),
code_types as 
(
    select p.*, r.title
    from procs p
    left join delivered.public.ref_boccui r
    on p.code_type = r.code
),
--select count(1) from code_types where status = 'removed'
added_codes as 
(
    select status, code_type, title code_type_name, mapped_procedure_code proc_new,  boc_cui boc_new, boc_name boc_name_new--standard_procedure_description desc_new,
    from code_types
    where status = 'added'
),
removed_codes as 
(
    select status, code_type, title code_type_name, mapped_procedure_code proc_old,  boc_cui boc_old, boc_name boc_name_old--standard_procedure_description desc_old,
    from code_types
    where status = 'removed'
),
qa_table as (
select distinct
proc_old,
proc_new,
    case 
        when proc_old is not null and proc_new is not null then 'updated'
        when proc_old is not null then 'removed'
        else 'added'
    end status,
    nvl(a.code_type, r.code_type) code_type, nvl(a.code_type_name, r.code_type_name) code_type_name, nvl(a.proc_new, r.proc_old) proc,
    --desc_old, desc_new, case when desc_old <> desc_new then 'updated' else null end desc_updated,
    boc_old, boc_new, 
   case when boc_old <> boc_new then 'updated' 
         when boc_old = boc_new then 'not changed' 
         when boc_old is not null and boc_new is null then 'removed'
         when boc_old is null and boc_new is not null then 'added'

    else null end boc_status,
    boc_name_old, boc_name_new, 
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
    nvl(a.proc_new, 'x') = nvl(r.proc_old, 'x') and 
    nvl(a.code_type_name, 'x') = nvl(r.code_type_name, 'x')
    )
    select 
proc_old,
proc_new,
proc,
code_type,
status,
status_filter,
boc_old, 
boc_new,
boc_name_old,
boc_name_new 
from qa_table