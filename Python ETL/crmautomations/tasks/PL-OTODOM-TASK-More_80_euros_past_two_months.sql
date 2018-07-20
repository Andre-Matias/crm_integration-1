insert into crm_integration_anlt.t_fac_auto_task
  select
    max_cod_auto_task + row_number() over () max_cod_auto_task,
    0 cod_base_task,
	14 cod_source_system,
	3 cod_status,
	cast(to_char(sysdate, 'YYYYMMDD') as integer) dat_task_rule,
	null dat_task_sync,
	2 cod_rule,
	'Testing auto task creation script rule 1' content,
	'contact' resource_type,
	to_char (sysdate + 5000, 'YYYYMMDD HH24:MI:SS') due_date,
	1 owner_id,
	211216854 resource_id,
	False completed,
	to_char (sysdate + 300, 'YYYYMMDD HH24:MI:SS') remind_at
	
  from
    (select coalesce(max(cod_auto_task),0) max_cod_auto_task from crm_integration_anlt.t_fac_auto_task),
    crm_integration_anlt.t_rel_scai_country_integration rel_country_integr
  where
		1=1
limit 3;	