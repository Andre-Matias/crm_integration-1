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
				and ( (task.flg_completed = 1  --this sould be set to 1 = closed/completed
						and auto_task.dat_task_rule >	to_char(sysdate - 30, 'YYYYMMDD')) or task.flg_completed = 0 ) --change this to specific date, in case its monthly (30)
				and auto_task.cod_rule = 10 --change this to this task rule 
)
	where 1=1
	and rnumber = 1)
select  (select coalesce(max(cod_auto_task),0) max_cod_auto_task from crm_integration_anlt.t_fac_auto_task) + row_number() over () max_cod_auto_task,
  0 opr_task,
	14 cod_source_system,
	3 cod_status,
	cast(to_char(sysdate, 'YYYYMMDD') as integer) dat_task_rule,
	null dat_task_sync,
	10 cod_rule, --change this to this task rule
	'Twój klient kupuje podbicia pojedynczo. Czas temu zaradzić! (Auto_task_' || (select coalesce(max(cod_auto_task),0) max_cod_auto_task from crm_integration_anlt.t_fac_auto_task) + row_number() over () || ')' content,
	'contact' resource_type,
	to_char (sysdate + 1, 'YYYY-MM-DD HH24:MI:SS') due_date,
	sales_rep_id owner_id,
	task.base_id resource_id,
	False completed,
	to_char (sysdate + 1, 'YYYY-MM-DD') || ' 13:00:00' remind_at
from (
  select
  sales_rep,
  sales_rep_email,
  sales_rep_id,
  base_id,
  atlas_id,
  email,
  purchase_value
from (
select
  base_user.dsc_base_user sales_rep,
  base_user.email sales_rep_email,
  base_user.opr_base_user sales_rep_id,
    (select company.opr_contact from crm_integration_anlt.t_lkp_contact company where company.valid_to = 20991231 and company.cod_source_system = 14 and company.cod_contact = a.cod_contact_parent) base_id,
    opr_atlas_user atlas_id,
    a.email,
    a.date,
    purchase_value,
    row_number() over (partition by opr_atlas_user order by purchase_value desc) rn
  from (
		  select
		  base_contact.cod_contact,
		  base_contact.cod_contact_parent,
		  base_contact.cod_base_user_owner,
		  atlas_user.opr_atlas_user,
		  base_contact.email,
		  base_contact.opr_contact,
		  base_contact.cod_atlas_user,
		  to_char(a.date,'YYYYMM') date,
		  sum(a.price) as purchase_value
		  from
		  db_atlas_verticals.paidads_user_payments a,
		  db_atlas_verticals.paidads_indexes b,
		  crm_integration_anlt.t_lkp_atlas_user atlas_user,
		  crm_integration_anlt.t_lkp_contact base_contact
		  where
		  a.livesync_dbname = 'otodompl'
		  and a.livesync_dbname = b.livesync_dbname
		  and a.id_index = b.id
		  and lower(b.code) not like '%packet%'
		  and b.id not in (51,73,455,75,121,123)
		  and b.type = 'pushup'
		  and base_contact.valid_to = 20991231
		  and atlas_user.valid_to = 20991231
		  and base_contact.cod_source_system = 14
		  and atlas_user.cod_source_system = 6
		  and a.id_user = atlas_user.opr_atlas_user
		  and lower(base_contact.email) = lower(atlas_user.dsc_atlas_user)
		  and  to_char(a.date,'YYYYMM') in (to_char( add_months( sysdate, -1),'YYYYMM') ,  to_char( add_months( sysdate, -2),'YYYYMM') )
		  and a.is_removed_from_invoice = 0
		  and base_contact.cod_contact_parent is not null
		  group by base_contact.cod_contact, 
		  base_contact.cod_contact_parent, 
		  base_contact.cod_base_user_owner, 
		  atlas_user.opr_atlas_user,
		  base_contact.email, 
		  base_contact.opr_contact,
		  base_contact.cod_atlas_user, 
		  to_char(a.date,'YYYYMM') 
	  ) a,
      crm_integration_anlt.t_lkp_base_user base_user
  where 1=1
    and purchase_value < -150
    and a.cod_base_user_owner = base_user.cod_base_user
    and base_user.valid_to = 20991231) b
where rn = 1) task
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