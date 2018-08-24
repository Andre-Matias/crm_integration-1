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
				and auto_task.cod_rule = 4 --change this to this task rule
				and auto_task.dat_task_rule >	to_char(sysdate - 7, 'YYYYMMDD') --change this to specific date, in case its monthly (30)
)
	where 1=1
	and rnumber = 1)
select  (select coalesce(max(cod_auto_task),0) max_cod_auto_task from crm_integration_anlt.t_fac_auto_task) + row_number() over () max_cod_auto_task,
  0 opr_task,
	14 cod_source_system,
	3 cod_status,
	cast(to_char(sysdate, 'YYYYMMDD') as integer) dat_task_rule,
	null dat_task_sync,
	4 cod_rule, --change this to this task rule
	'Od 2 miesięcy nie kontaktowałeś się z tym klientem. Warto sprawdzić co u niego słychać! (Auto_task_' || (select coalesce(max(cod_auto_task),0) max_cod_auto_task from crm_integration_anlt.t_fac_auto_task) + row_number() over () || ')' content,
	'contact' resource_type,
	to_char (sysdate + 365, 'YYYY-MM-DD HH24:MI:SS') due_date,
	sales_rep_id owner_id,
	task.base_id resource_id,
	False completed,
	to_char (sysdate + 1, 'YYYY-MM-DD') || ' 11:00:00' remind_at
from (
select
  b.dsc_base_user sales_rep,
  b.email sales_rep_email,
  b.opr_base_user sales_rep_id,
  opr_contact base_id,
  (select opr_atlas_user from crm_integration_anlt.t_lkp_atlas_user atlas_user where atlas_user.valid_to = 20991231 and atlas_user.cod_source_system = 6 and atlas_user.cod_atlas_user = a.cod_atlas_user) as opr_atlas_user,
  a.email,
  a.created_at as recent_call,
  abs(nvl(extract(day from a.created_at -   sysdate) )) as days_since_last_call
from (
select
  row_number() over (partition by contact.phone order by call.created_at desc) rn,
  call.cod_call,
  call.opr_call,
  call.cod_contact,
  call.phone_number,
  call.created_at,
  contact.email,
  contact.opr_contact,
  contact.cod_atlas_user,
  contact.cod_base_user_owner
from
  crm_integration_anlt.t_fac_call call,
  crm_integration_anlt.t_lkp_contact contact
where 1=1
and contact.cod_contact = call.cod_contact
and contact.cod_source_system = call.cod_source_system
and contact.valid_to = 20991231
and contact.cod_source_system = 14
 -- and contact.cod_contact = 128437601
 -- and phone = '500302000'
) a,
  crm_integration_anlt.t_lkp_base_user b
where 1=1
  and rn = 1
  and a.email != ''
  and abs(nvl(extract(day from a.created_at -   sysdate) )) > 60
  and cod_atlas_user != -2
  and a.cod_base_user_owner = b.cod_base_user
  and b.valid_to = 20991231) task
  left outer join task_insert_rules on task.base_id = task_insert_rules.resource_id and task_insert_rules.resource_type = 'contact'
where 1=1
  and task_insert_rules.cod_auto_task is null;