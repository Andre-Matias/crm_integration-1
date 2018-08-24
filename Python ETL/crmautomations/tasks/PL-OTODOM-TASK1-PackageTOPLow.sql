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
				and auto_task.cod_rule = 1 --change this to this task rule
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
	1 cod_rule, --change this to this task rule
	'Twojemu klientowi kończą się TOPy. Skontaktuj się z nim! (Auto_task_' || (select coalesce(max(cod_auto_task),0) max_cod_auto_task from crm_integration_anlt.t_fac_auto_task) + row_number() over () || ')' content,
	'contact' resource_type,
	to_char (sysdate + 365, 'YYYY-MM-DD HH24:MI:SS') due_date,
	sales_rep_id owner_id,
	task.base_id resource_id,
	False completed,
	to_char (sysdate + 1, 'YYYY-MM-DD') || ' 10:00:00' remind_at
from (
select
  base_user.dsc_base_user sales_rep,
  base_user.email sales_rep_email,
  base_user.opr_base_user sales_rep_id,
  a.opr_contact base_id,
  opr_atlas_user atlas_id,
  a.email,
  total_bought,
  total_used,
  total_bought - total_used remaining_uses
from
  (
    select
      base_contact.cod_contact,
      base_contact.cod_base_user_owner,
      atlas_user.opr_atlas_user,
      base_contact.email,
      base_contact.opr_contact,
      base_contact.cod_atlas_user,
      sum(bought) total_bought,
      sum(used) total_used
    from
      db_atlas_verticals.paidpromo_packets packets,
      crm_integration_anlt.t_lkp_atlas_user atlas_user,
      crm_integration_anlt.t_lkp_contact base_contact
    where
      packets.livesync_dbname = 'otodompl'
      and lower(packets.index_type) like '%topads%'
      and base_contact.valid_to = 20991231
      and atlas_user.valid_to = 20991231
      and base_contact.cod_source_system = 14
      and atlas_user.cod_source_system = 6
      and packets.user_id = atlas_user.opr_atlas_user
      and lower(base_contact.email) = lower(atlas_user.dsc_atlas_user)
    group by
      base_contact.cod_contact,
      base_contact.cod_base_user_owner,
      atlas_user.opr_atlas_user,
      base_contact.email,
      base_contact.opr_contact,
      base_contact.cod_atlas_user) a,
  crm_integration_anlt.t_lkp_base_user base_user
where
  total_bought - total_used < 5
  and total_bought - total_used > 0
  and a.cod_base_user_owner = base_user.cod_base_user
  and base_user.valid_to = 20991231 ) task
  left outer join task_insert_rules on task.base_id = task_insert_rules.resource_id and task_insert_rules.resource_type = 'contact'
  left outer join (select ads.user_id, count(*) active_ads from db_atlas_verticals.ads ads
            where 1=1
            and ads.livesync_dbname = 'otodompl'
            and ads.status = 'active'
            group by ads.user_id) ads --is an active user by having at least one active ad
  on ads.user_id = task.atlas_id
where 1=1
  and task_insert_rules.cod_auto_task is null
  and ads.active_ads is not null;	

 