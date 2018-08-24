insert into crm_integration_anlt.t_fac_auto_task
with task_insert_rules as (select * from (
	select
		auto_task.*,
		row_number()
		over ( partition by auto_task.resource_type, auto_task.resource_id
			order by cod_auto_task desc ) rnumber
	from
		crm_integration_anlt.t_fac_auto_task auto_task,
		crm_integration_anlt.t_lkp_task task
	where 1 = 1
				and task.opr_task = auto_task.opr_task
				and task.valid_to = 20991231
				and task.cod_source_system = 14 --change this to right site
				and task.flg_completed = 1  --this sould be set to 1 = closed/completed
				and auto_task.cod_rule = 6 --change this to this task rule
				and auto_task.dat_task_rule >	to_char(sysdate - 30, 'YYYYMMDD') --change this to specific date, in case its monthly (30)
)
	where 1=1
	and rnumber = 1)
select  (select coalesce(max(cod_auto_task),0) max_cod_auto_task from crm_integration_anlt.t_fac_auto_task) + row_number() over () max_cod_auto_task,
  0 opr_task,
	14 cod_source_system,
	3 cod_status,
	cast(to_char(sysdate, 'YYYYMMDD') as integer) dat_task_rule,
	null dat_task_sync,
	6 cod_rule, --change this to this task rule
	'Konto Twojego klienta zostało zablokowano. Przypomnij o zapłaceniu faktury! (Auto_task_' || (select coalesce(max(cod_auto_task),0) max_cod_auto_task from crm_integration_anlt.t_fac_auto_task) + row_number() over () || ')' content,
	'contact' resource_type,
	to_char (sysdate + 365, 'YYYY-MM-DD HH24:MI:SS') due_date,
	sales_rep_id owner_id,
	task.base_id resource_id,
	False completed,
	to_char (sysdate + 1, 'YYYY-MM-DD') || ' 09:00:00' remind_at
from (
select
  base_user.dsc_base_user sales_rep,
  base_user.email sales_rep_email,
  base_user.opr_base_user sales_rep_id,
  base_contact.opr_contact base_id,
  atlas_user.opr_atlas_user,
  base_contact.email
from
  crm_integration_anlt.t_lkp_atlas_user atlas_user,
  crm_integration_anlt.t_lkp_contact base_contact,
  crm_integration_anlt.t_lkp_base_user base_user
where
  1=1
  and atlas_user.cod_source_system = 6
  and base_contact.cod_source_system = 14
  and atlas_user.valid_to = 20991231
  and base_contact.valid_to = 20991231
  and atlas_user.type = 'suspended'
  and atlas_user.suspend_reason = 'unpaid_invoice'
  and lower(base_contact.email) = lower(atlas_user.dsc_atlas_user)
  and base_contact.cod_base_user_owner = base_user.cod_base_user
  and base_user.valid_to = 20991231) task
  left outer join task_insert_rules on task.base_id = task_insert_rules.resource_id and task_insert_rules.resource_type = 'contact'
where 1=1
  and task_insert_rules.cod_auto_task is null;