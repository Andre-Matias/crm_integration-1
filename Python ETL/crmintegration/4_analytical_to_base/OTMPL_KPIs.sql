-- #######################
-- ####    PASSO 3    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set dat_processing = source.dat_processing, execution_nbr = source.execution_nbr, cod_status = 2 -- Running
from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc, crm_integration_anlt.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_fac_base_integration_snap_plcars'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
  ) source
where crm_integration_anlt.t_rel_scai_integration_process.cod_process = source.cod_process
and crm_integration_anlt.t_rel_scai_integration_process.cod_country = source.cod_country
and crm_integration_anlt.t_rel_scai_integration_process.cod_integration = source.cod_integration;

--$$$

-- #######################
-- ####    PASSO 4    ####
-- #######################
insert into crm_integration_anlt.t_fac_scai_execution
  select
    max_cod_exec + 1 cod_execution,
	rel_integr_proc.cod_country,
    rel_integr_proc.cod_integration,
    rel_integr_proc.cod_process,
    rel_integr_proc.cod_status,
    1 cod_execution_type, -- Begin
    rel_integr_proc.dat_processing,
    rel_integr_proc.execution_nbr,
    sysdate
  from
    crm_integration_anlt.t_rel_scai_country_integration rel_country_integr,
    (select isnull(max(cod_execution),0) max_cod_exec from crm_integration_anlt.t_fac_scai_execution),
    crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
    crm_integration_anlt.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 50000 -- Chandra (Analytical) to Base
    and rel_country_integr.cod_country = 2 -- Poland
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_fac_base_integration_snap_plcars';

--$$$

--(--------REVENUE--------)
insert into crm_integration_anlt.t_fac_base_integration_snap (
select source.* from (
  select
    base_contact.cod_contact,
    4484 cod_custom_field,
    scai.dat_processing dat_snap,
    base_contact.cod_source_system,
    ' ' as custom_field_value
  from
    crm_integration_anlt.t_lkp_contact base_contact,
    crm_integration_anlt.t_rel_scai_country_integration scai
  where
    base_contact.cod_source_system = 12
    and base_contact.valid_to = 20991231
    and scai.cod_integration = 50000
    and scai.cod_country = 2
) source,
    crm_integration_anlt.t_fac_base_integration_snap fac_snap
where source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and fac_snap.cod_contact is null
);

--$$$

--(--------REPLIES--------)
insert into crm_integration_anlt.t_fac_base_integration_snap (
select source.* from (
  SELECT
    base_contact.cod_contact,
    4482 cod_custom_field,
    scai.dat_processing dat_snap,
    base_contact.cod_source_system,
    ' ' as custom_field_value
  FROM
    crm_integration_anlt.t_lkp_contact base_contact,
    crm_integration_anlt.t_rel_scai_country_integration scai
  WHERE
    base_contact.cod_source_system = 12
    AND base_contact.valid_to = 20991231
    AND scai.cod_integration = 50000
    and scai.cod_country = 2
) source,
    crm_integration_anlt.t_fac_base_integration_snap fac_snap
where source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and fac_snap.cod_contact is null
);

--$$$

--(--------PACKAGE--------)
insert into crm_integration_anlt.t_fac_base_integration_snap (
select source.* from (
  SELECT
    base_contact.cod_contact,
    4480 cod_custom_field,
    scai.dat_processing dat_snap,
    base_contact.cod_source_system,
    ' ' as custom_field_value
  FROM
    crm_integration_anlt.t_lkp_contact base_contact,
    crm_integration_anlt.t_rel_scai_country_integration scai
  WHERE
    base_contact.cod_source_system = 12
    AND base_contact.valid_to = 20991231
    AND scai.cod_integration = 50000
    and scai.cod_country = 2
) source,
    crm_integration_anlt.t_fac_base_integration_snap fac_snap
where source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and fac_snap.cod_contact is null
);

--$$$

--(--------ACTIVITY--------)
insert into crm_integration_anlt.t_fac_base_integration_snap (
select source.* from (
  SELECT
    base_contact.cod_contact,
    4483 cod_custom_field,
    scai.dat_processing dat_snap,
    base_contact.cod_source_system,
    ' ' as custom_field_value
  FROM
    crm_integration_anlt.t_lkp_contact base_contact,
    crm_integration_anlt.t_rel_scai_country_integration scai
  WHERE
    base_contact.cod_source_system = 12
    AND base_contact.valid_to = 20991231
    AND scai.cod_integration = 50000
    and scai.cod_country = 2
) source,
    crm_integration_anlt.t_fac_base_integration_snap fac_snap
where source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and fac_snap.cod_contact is null
);

--$$$

--(--------OTHER--------)
insert into crm_integration_anlt.t_fac_base_integration_snap (
select source.* from (
  SELECT
    base_contact.cod_contact,
    4481 cod_custom_field,
    scai.dat_processing dat_snap,
    base_contact.cod_source_system,
    ' ' as custom_field_value
  FROM
    crm_integration_anlt.t_lkp_contact base_contact,
    crm_integration_anlt.t_rel_scai_country_integration scai
  WHERE
    base_contact.cod_source_system = 12
    AND base_contact.valid_to = 20991231
    AND scai.cod_integration = 50000
    and scai.cod_country = 2
) source,
    crm_integration_anlt.t_fac_base_integration_snap fac_snap
where source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and fac_snap.cod_contact is null
);

--$$$

-- CREATE TMP - KPI OLX.BASE.084 (Last login)
create temp table tmp_pl_otomoto_calc_last_login_1 as
    select
		  a.cod_contact,
		  a.cod_contact_parent,
		  kpi_custom_field.cod_custom_field,
		  scai.dat_processing dat_snap,
		  isnull(a.cod_source_system,12) cod_source_system,
		  isnull(a.custom_field_value, '1900-01-01 00:00:00') custom_field_value
		from
		  (
			  select
					*
			  from
					(
						select
							base_contact.cod_contact_parent,
							base_contact.cod_contact,
							scai.dat_processing dat_snap,
							base_contact.cod_source_system,
							cast(atlas_user.last_login_at as varchar) custom_field_value,
							row_number() over (partition by cod_contact order by coalesce(atlas_user.last_login_at,'1900-01-01') desc) rn
						from
							crm_integration_anlt.t_lkp_atlas_user atlas_user,
							crm_integration_anlt.t_lkp_contact base_contact,
							crm_integration_anlt.t_rel_scai_country_integration scai
						where
							atlas_user.cod_source_system = 7
							and base_contact.cod_source_system = 12
							and lower(base_contact.email) = lower(atlas_user.dsc_atlas_user)
							and atlas_user.valid_to = 20991231
							and base_contact.valid_to = 20991231
							and scai.cod_integration = 50000
							and scai.cod_country = 2
					)
			  where
					rn = 1
		  ) a,
			crm_integration_anlt.t_rel_scai_country_integration scai,
			(
				select
				  rel.cod_custom_field,
				  rel.flg_active
				from
				  crm_integration_anlt.t_lkp_kpi kpi,
				  crm_integration_anlt.t_rel_kpi_custom_field rel
				where
				  kpi.cod_kpi = rel.cod_kpi
				  and lower(kpi.dsc_kpi) = 'last login'
				  and rel.cod_source_system = 12
			) kpi_custom_field
		where
		  1 = 1
		  and scai.cod_integration = 50000
		  and kpi_custom_field.flg_active = 1
		  and scai.cod_country = 2
	 ;


	 
--Calculate for employees	 
create temp table tmp_pl_otomoto_calc_last_login_2 as
select source.cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	source.custom_field_value
from tmp_pl_otomoto_calc_last_login_1 source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where 1 = 1
  and source.cod_contact_parent is not null
  and source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);
	 ;	 
	 

	 
--Calculate for companies and contacts not associated with companies	
create temp table tmp_pl_otomoto_calc_last_login_3 as
select nvl(source.cod_contact_parent, source.cod_contact) as cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	max(source.custom_field_value) custom_field_value
from tmp_pl_otomoto_calc_last_login_1 source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where 1 = 1
  and source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and nvl(source.cod_contact_parent, source.cod_contact) = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null)
  group by
  source.cod_custom_field,
  source.dat_snap,
  source.cod_source_system,  
  nvl(source.cod_contact_parent, source.cod_contact)
	 ;	 


-- HST INSERT - KPI OLX.BASE.085 (Last login)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in 
			(select cod_contact, cod_custom_field from tmp_pl_otomoto_calc_last_login_2
			union 
			select cod_contact, cod_custom_field from tmp_pl_otomoto_calc_last_login_3);


-- SNAP DELETE - KPI OLX.BASE.085 (Last login)
DELETE FROM crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in 
			(select cod_contact, cod_custom_field from tmp_pl_otomoto_calc_last_login_2
			union 
			select cod_contact, cod_custom_field from tmp_pl_otomoto_calc_last_login_3);



--KPI OLX.BASE.085 (Last login)
insert into crm_integration_anlt.t_fac_base_integration_snap
	select
		*
	from
		(select * from tmp_pl_otomoto_calc_last_login_2
		union 
		select * from tmp_pl_otomoto_calc_last_login_3);



--$$$

-- CREATE TMP - KPI OLX.BASE.031 (Created date)
create temp table tmp_pl_otomoto_calc_created_date_1 as
	select
		  a.cod_contact,
		  a.cod_contact_parent,
		  kpi_custom_field.cod_custom_field,
		  scai.dat_processing dat_snap,
		  isnull(a.cod_source_system,12) cod_source_system,
		  isnull(a.custom_field_value, ' ') custom_field_value
		from
		  (
				select
					*
				from
					(
						select
							base_contact.cod_contact_parent,
							base_contact.cod_contact,
							scai.dat_processing dat_snap,
							base_contact.cod_source_system,
							cast(atlas_user.created_at as varchar) custom_field_value,
							row_number() over (partition by cod_contact order by coalesce(atlas_user.created_at,'1900-01-01')) rn
						from
							crm_integration_anlt.t_lkp_atlas_user atlas_user,
							crm_integration_anlt.t_lkp_contact base_contact,
							crm_integration_anlt.t_rel_scai_country_integration scai
						where
							atlas_user.cod_source_system = 7
							and base_contact.cod_source_system = 12
							and lower(base_contact.email) = lower(atlas_user.dsc_atlas_user)
							and atlas_user.valid_to = 20991231
							and base_contact.valid_to = 20991231
							and scai.cod_integration = 50000
							and scai.cod_country = 2
							--and base_contact.cod_contact_parent = 306798
						) a
					where
						rn = 1
		  ) a,
			crm_integration_anlt.t_rel_scai_country_integration scai,
			(
				select
				  rel.cod_custom_field,
				  rel.flg_active
				from
				  crm_integration_anlt.t_lkp_kpi kpi,
				  crm_integration_anlt.t_rel_kpi_custom_field rel
				where
				  kpi.cod_kpi = rel.cod_kpi
				  and lower(kpi.dsc_kpi) = 'created date'
				  and rel.cod_source_system = 12
			) kpi_custom_field
		where
		  1 = 1
		  and scai.cod_integration = 50000
		  and kpi_custom_field.flg_active = 1
		  and scai.cod_country = 2
	;


	
--Calculate for employees	
create temp table tmp_pl_otomoto_calc_created_date_2 as
select source.cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	source.custom_field_value
from tmp_pl_otomoto_calc_created_date_1 source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where 1 = 1
  and source.cod_contact_parent is not null
  and source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);	
  

  
--Calculate for companies and contacts not associated with companies
create temp table tmp_pl_otomoto_calc_created_date_3 as
   select nvl(source.cod_contact_parent, source.cod_contact) as cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system
	,min(source.custom_field_value) custom_field_value
	from tmp_pl_otomoto_calc_created_date_1 source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where 1 = 1
  and source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and nvl(source.cod_contact_parent, source.cod_contact) = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null)
  group by
  source.cod_custom_field,
  source.dat_snap,
  source.cod_source_system,
  nvl(source.cod_contact_parent,source.cod_contact)
	 ;



-- HST INSERT - KPI OLX.BASE.031 (Created date)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in
			(select cod_contact, cod_custom_field from tmp_pl_otomoto_calc_created_date_2
			union
			select cod_contact, cod_custom_field from tmp_pl_otomoto_calc_created_date_3);



-- SNAP DELETE - KPI OLX.BASE.031 (Created date)
DELETE FROM crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in
			(select cod_contact, cod_custom_field from tmp_pl_otomoto_calc_created_date_2
			union
			select cod_contact, cod_custom_field from tmp_pl_otomoto_calc_created_date_3);



--KPI OLX.BASE.031 (Created date)
insert into crm_integration_anlt.t_fac_base_integration_snap
	select
		*
	from
		(select * from tmp_pl_otomoto_calc_created_date_2
		union
		select * from tmp_pl_otomoto_calc_created_date_3);


--$$$

-- CREATE TMP - KPI OLX.BASE.086 (# Logins last 30 days)
create temp table tmp_pl_otomoto_calc_logins_last_30_days_1 as
select
			a.cod_contact,
			a.cod_contact_parent,
			kpi_custom_field.cod_custom_field,
			scai.dat_processing dat_snap,
			isnull(a.cod_source_system,12) cod_source_system,
			isnull(a.custom_field_value, '0') custom_field_value
		from
			(
				select
					cod_contact,
					cod_contact_parent,
					dat_snap,
					cod_source_system,
					count(distinct server_date_day) custom_field_value
				from
					(
						select
							base_contact.cod_contact,
							base_contact.cod_contact_parent,
							inner_core.server_date_day,
							inner_core.dat_snap,
							base_contact.cod_source_system,
							inner_core.nbr_platform_interactions
						from
							(
								select
									base.cod_contact,
									base.cod_contact_parent,
									web.server_date_day,
									dat_snap,
									cod_source_system,
									count(*) nbr_platform_interactions
								from
									hydra_verticals.web web,
									(
										select
											base_contact.cod_contact_parent,
											base_contact.cod_contact,
											scai.dat_processing dat_snap,
											base_contact.cod_source_system,
											atlas_user.opr_atlas_user
										from
											crm_integration_anlt.t_lkp_atlas_user atlas_user,
											crm_integration_anlt.t_lkp_contact base_contact,
											crm_integration_anlt.t_rel_scai_country_integration scai
										where
											atlas_user.cod_source_system = 7
											and base_contact.cod_source_system = 12
											and base_contact.cod_atlas_user = atlas_user.cod_atlas_user
											and atlas_user.valid_to = 20991231
											and base_contact.valid_to = 20991231
											and scai.cod_integration = 50000
											and scai.cod_country = 2
											--and base_contact.cod_contact_parent = 306798
									) base
								where
									web.server_date_day >= dateadd(day,-30,sysdate)
									and web.country_code = 'PL'
									and web.stream = 'v-otomoto-web'
									and web.user_id = base.opr_atlas_user
								group by
										base.cod_contact,
									base.cod_contact_parent,
									dat_snap,
									cod_source_system,
									web.server_date_day

								union all

								select
										base.cod_contact,
									base.cod_contact_parent,
									ios.server_date_day,
									dat_snap,
									cod_source_system,
									count(*) nbr_platform_interactions
								from
									hydra_verticals.ios ios,
									(
										select
											base_contact.cod_contact_parent,
											base_contact.cod_contact,
											scai.dat_processing dat_snap,
											base_contact.cod_source_system,
											atlas_user.opr_atlas_user
										from
											crm_integration_anlt.t_lkp_atlas_user atlas_user,
											crm_integration_anlt.t_lkp_contact base_contact,
											crm_integration_anlt.t_rel_scai_country_integration scai
										where
											atlas_user.cod_source_system = 7
											and base_contact.cod_source_system = 12
											and base_contact.cod_atlas_user = atlas_user.cod_atlas_user
											and atlas_user.valid_to = 20991231
											and base_contact.valid_to = 20991231
											and scai.cod_integration = 50000
											and scai.cod_country = 2
											--and base_contact.cod_contact_parent = 306798
									) base
								where
									ios.server_date_day >= dateadd(day,-30,sysdate)
									and ios.country_code = 'PL'
									and ios.stream = 'v-otomoto-ios'
									and ios.user_id = base.opr_atlas_user
								group by
										base.cod_contact,
									base.cod_contact_parent,
									ios.server_date_day,
									dat_snap,
									cod_source_system

								union all

								select
										base.cod_contact,
									base.cod_contact_parent,
									android.server_date_day,
									dat_snap,
									cod_source_system,
									count(*) nbr_platform_interactions
								from
									hydra_verticals.android android,
									(
										select
											base_contact.cod_contact_parent,
											base_contact.cod_contact,
											scai.dat_processing dat_snap,
											base_contact.cod_source_system,
											atlas_user.opr_atlas_user
										from
											crm_integration_anlt.t_lkp_atlas_user atlas_user,
											crm_integration_anlt.t_lkp_contact base_contact,
											crm_integration_anlt.t_rel_scai_country_integration scai
										where
											atlas_user.cod_source_system = 7
											and base_contact.cod_source_system = 12
											and base_contact.cod_atlas_user = atlas_user.cod_atlas_user
											and atlas_user.valid_to = 20991231
											and base_contact.valid_to = 20991231
											and scai.cod_integration = 50000
											and scai.cod_country = 2
											--and base_contact.cod_contact_parent = 306798
									) base
								where
									android.server_date_day >= dateadd(day,-30,sysdate)
									and android.country_code = 'PL'
									and android.stream = 'v-otomoto-android'
									and android.user_id = base.opr_atlas_user
								group by
									base.cod_contact ,
									base.cod_contact_parent,
									android.server_date_day,
									dat_snap,
									cod_source_system
							) inner_core,
							crm_integration_anlt.t_lkp_contact base_contact
					where
						base_contact.cod_contact = inner_core.cod_contact (+)
						and base_contact.cod_source_system = 12
						and base_contact.valid_to = 20991231
					) core
				group by
					cod_contact,
					cod_contact_parent,
					dat_snap,
					cod_source_system
			) a,
			crm_integration_anlt.t_rel_scai_country_integration scai,
			(
				select
					rel.cod_custom_field,
					rel.flg_active
				from
					crm_integration_anlt.t_lkp_kpi kpi,
					crm_integration_anlt.t_rel_kpi_custom_field rel
				where
					kpi.cod_kpi = rel.cod_kpi
					and lower(kpi.dsc_kpi) = '# logins last 30 days'
					and rel.cod_source_system = 12
			) kpi_custom_field
		where
			1 = 1
			and scai.cod_integration = 50000
			and kpi_custom_field.flg_active = 1
			and scai.cod_country = 2;
			

		
--Calculate for employees
create temp table tmp_pl_otomoto_calc_logins_last_30_days_2 as
select source.cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	source.custom_field_value
from tmp_pl_otomoto_calc_logins_last_30_days_1 source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where 1 = 1
  and source.cod_contact_parent is not null
  and source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);	


--Calculate for companies and contacts not associated with companies
create temp table tmp_pl_otomoto_calc_logins_last_30_days_3 as
select nvl(source.cod_contact_parent, source.cod_contact) as cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system
	,sum(source.custom_field_value) custom_field_value
from tmp_pl_otomoto_calc_logins_last_30_days_1 source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where 1 = 1
  and source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and nvl(source.cod_contact_parent, source.cod_contact) = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null)
  group by
  source.cod_custom_field,
  source.dat_snap,
  source.cod_source_system,
	nvl(source.cod_contact_parent,source.cod_contact)
	 ;  


-- HST INSERT - KPI OLX.BASE.086 (# Logins last 30 days)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in
			(select cod_contact, cod_custom_field from tmp_pl_otomoto_calc_logins_last_30_days_2
			union
			select cod_contact, cod_custom_field from tmp_pl_otomoto_calc_logins_last_30_days_3);



-- SNAP DELETE - KPI OLX.BASE.086 (# Logins last 30 days)
DELETE FROM crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in
			(select cod_contact, cod_custom_field from tmp_pl_otomoto_calc_logins_last_30_days_2
			union
			select cod_contact, cod_custom_field from tmp_pl_otomoto_calc_logins_last_30_days_3);


--KPI OLX.BASE.086 (# Logins last 30 days)
insert into crm_integration_anlt.t_fac_base_integration_snap
	select
		*
	from
		(select * from tmp_pl_otomoto_calc_logins_last_30_days_2
		union
		select * from tmp_pl_otomoto_calc_logins_last_30_days_3);


--$$$

-- CREATE TMP - KPI OLX.BASE.012 (Last package purchased)
create temp table tmp_pl_otomoto_calc_last_package_purchased_1 as
select
  source.cod_contact,
  source.cod_contact_parent,
  source.cod_custom_field,
  source.dat_snap,
  source.cod_source_system,
  source.custom_field_value
from
  (
    select
      lkp_contact.cod_contact,
      lkp_contact.cod_contact_parent,
      kpi_custom_field.cod_custom_field,
      scai.dat_processing dat_snap,
      lkp_contact.cod_source_system,
      nvl(user_package.package_type,'-') custom_field_value
    from
      (
        select
          lkp_atlas_user.cod_atlas_user,
          case
            when billing_periods.package_id in (1,4) then 'Standard'
            when billing_periods.package_id in (2,5) then 'Premium'
            when billing_periods.package_id in (3,6) then 'Premium Plus'
          end package_type
        from
          db_atlas_verticals.billing_periods billing_periods,
          crm_integration_anlt.t_lkp_atlas_user lkp_atlas_user
        where
          lkp_atlas_user.opr_atlas_user = billing_periods.user_id
          and lkp_atlas_user.valid_to = 20991231
          and sysdate between billing_periods.starting_time and billing_periods.ending_time -- Current Billing Cycle
          and lkp_atlas_user.cod_source_system = 7
          and billing_periods.livesync_dbname = 'otomotopl'
      ) user_package,
      (
        select
          rel.cod_custom_field,
          rel.flg_active
        from
          crm_integration_anlt.t_lkp_kpi kpi,
          crm_integration_anlt.t_rel_kpi_custom_field rel
        where
          kpi.cod_kpi = rel.cod_kpi
          and lower(kpi.dsc_kpi) = 'last package purchased'
          and rel.cod_source_system = 12
      ) kpi_custom_field,
      crm_integration_anlt.t_lkp_contact lkp_contact,
      crm_integration_anlt.t_rel_scai_country_integration scai
    where
      lkp_contact.cod_atlas_user = user_package.cod_atlas_user (+)
      and lkp_contact.valid_to = 20991231
      and lkp_contact.cod_source_system = 12
      and kpi_custom_field.flg_active = 1
      and scai.cod_integration = 50000
      and scai.cod_country = 2) source,
    crm_integration_anlt.t_fac_base_integration_snap fac_snap
where source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);



--Calculate for employees
create temp table tmp_pl_otomoto_calc_last_package_purchased_2 as
   select source.cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	source.custom_field_value
from tmp_pl_otomoto_calc_last_package_purchased_1 source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where 1 = 1
  and source.cod_contact_parent is not null
  and source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);	



--Calculate for companies and contacts not associated with companies
create temp table tmp_pl_otomoto_calc_last_package_purchased_3 as
   select nvl(source.cod_contact_parent, source.cod_contact) as cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system
	,max(source.custom_field_value) custom_field_value
from tmp_pl_otomoto_calc_last_package_purchased_1 source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where 1 = 1
  and source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and nvl(source.cod_contact_parent, source.cod_contact) = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null)
  group by
  source.cod_custom_field,
  source.dat_snap,
  source.cod_source_system,
	nvl(source.cod_contact_parent,source.cod_contact)
	 ;  



-- HST INSERT - KPI OLX.BASE.012 (Last package purchased)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in
			(select cod_contact, cod_custom_field from tmp_pl_otomoto_calc_last_package_purchased_2
			union
			select cod_contact, cod_custom_field from tmp_pl_otomoto_calc_last_package_purchased_3);



-- SNAP DELETE - KPI OLX.BASE.012 (Last package purchased)
DELETE FROM crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in
			(select cod_contact, cod_custom_field from tmp_pl_otomoto_calc_last_package_purchased_2
			union
			select cod_contact, cod_custom_field from tmp_pl_otomoto_calc_last_package_purchased_3);



--KPI OLX.BASE.012 (Last package purchased) 
insert into crm_integration_anlt.t_fac_base_integration_snap
	SELECT
		*
	from
		(select * from tmp_pl_otomoto_calc_last_package_purchased_2
		union
		select * from tmp_pl_otomoto_calc_last_package_purchased_3);


--$$$

-- CREATE TMP - KPI OLX.BASE.091 (Wallet)
create temp table tmp_pl_otomoto_calc_wallet_1 as
select
  cod_contact,
  cod_contact_parent,
  inner_core.cod_atlas_user,
  cod_custom_field,
  cast(round(nvl(val_current_credits,0),0) as varchar) custom_field_value,
  cod_source_system,
  dat_processing dat_snap
from
  (
	select
	  h.cod_atlas_user,
	  h.dsc_atlas_user,
	  d.dsc_source_system,
	  i.val_current_credits
	from
	  crm_integration_anlt.t_lkp_source_system d,
	  crm_integration_anlt.t_lkp_atlas_user h,
	  (
		SELECT
		  *
		FROM
		  (
			SELECT
			  fac.id_user opr_atlas_user,
			  fac.id_transaction opr_payment_session,
			  7 cod_source_system,
			  fac.current_credits val_current_credits,
			  row_number() OVER ( PARTITION BY fac.id_user ORDER BY fac.date DESC, fac.id DESC ) rn
			FROM
			  db_atlas_verticals.paidads_user_payments fac
			WHERE
			  livesync_dbname = 'otomotopl'
		)
	  WHERE rn = 1
	  ) i
	where
	  i.cod_source_system = d.cod_source_system
	  and i.opr_atlas_user = h.opr_atlas_user
	  and i.cod_source_system = h.cod_source_system
	  and d.cod_source_system = 7
	  and h.valid_to = 20991231
  ) inner_core,
  crm_integration_anlt.t_lkp_contact base_contact,
  crm_integration_anlt.t_rel_scai_country_integration scai,
  (
	select
	  rel.cod_custom_field,
	  rel.flg_active
	from
	  crm_integration_anlt.t_lkp_kpi kpi,
	  crm_integration_anlt.t_rel_kpi_custom_field rel
	where
	  kpi.cod_kpi = rel.cod_kpi
	  and lower(kpi.dsc_kpi) = 'wallet'
	  and rel.cod_source_system = 12
  ) kpi_custom_field
where
  lower(inner_core.dsc_atlas_user(+)) = lower(base_contact.email)
  and base_contact.valid_to = 20991231
  and base_contact.cod_source_system = 12
  and scai.cod_integration = 50000
  and scai.cod_country = 2;


	
--Calculate for employees
create temp table tmp_pl_otomoto_calc_wallet_2 as
   select source.cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	source.custom_field_value
	from tmp_pl_otomoto_calc_wallet_1 source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where 1 = 1
  and source.cod_contact_parent is not null
  and source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);



--Calculate for companies and contacts not associated with companies
create temp table tmp_pl_otomoto_calc_wallet_3 as
   select nvl(source.cod_contact_parent, source.cod_contact) as cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system
	,cast(sum(source.custom_field_value) as varchar) custom_field_value
	from tmp_pl_otomoto_calc_wallet_1 source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where 1 = 1
  and source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and nvl(source.cod_contact_parent, source.cod_contact) = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null)
  group by
  source.cod_custom_field,
  source.dat_snap,
  source.cod_source_system,
	nvl(source.cod_contact_parent,source.cod_contact)
	 ;			



-- HST INSERT - KPI OLX.BASE.091 (Wallet)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in
			(select cod_contact, cod_custom_field from tmp_pl_otomoto_calc_wallet_2
			union
			select cod_contact, cod_custom_field from tmp_pl_otomoto_calc_wallet_3);



-- SNAP DELETE - KPI OLX.BASE.091 (Wallet)
delete from crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in
			(select cod_contact, cod_custom_field from tmp_pl_otomoto_calc_wallet_2
			union
			select cod_contact, cod_custom_field from tmp_pl_otomoto_calc_wallet_3);



-- KPI OLX.BASE.091 (Wallet)
insert into crm_integration_anlt.t_fac_base_integration_snap
  select
    *
  from
    (select * from tmp_pl_otomoto_calc_wallet_2
		union
		select * from tmp_pl_otomoto_calc_wallet_3);



--$$$

-- CREATE TMP - KPI OLX.BASE.023 (# Replies)
create temp table tmp_pl_otomoto_calc_replies_1 as
select
  a.cod_contact_parent,
  a.cod_contact,
  kpi_custom_field.cod_custom_field,
  scai.dat_processing dat_snap,
  coalesce(a.cod_source_system,12) cod_source_system,
  coalesce(a.custom_field_value, '0') custom_field_value
from
  (
		select
      base_contact.cod_contact,
      base_contact.cod_contact_parent,
      inner_core.dat_snap,
      base_contact.cod_source_system,
      inner_core.custom_field_value
		from
			(
				select
					source.cod_contact_parent,
					source.cod_contact,
					source.dat_processing dat_snap,
					source.cod_source_system,
					cast(sum(nr_replies) as varchar) custom_field_value --nr_replies,
				from
					(
					select
						lkp_contact.cod_contact_parent,
						lkp_contact.cod_contact,
						scai.dat_processing,
						lkp_contact.cod_source_system,
						ads.id,
						count(*) nr_replies
					from
						db_atlas_verticals.answers fac,
						crm_integration_anlt.t_lkp_source_system lkp_source_system,
						db_atlas_verticals.ads ads,
						crm_integration_anlt.t_lkp_atlas_user lkp_user,
						crm_integration_anlt.t_lkp_contact lkp_contact,
						crm_integration_anlt.t_rel_scai_country_integration scai
					where
						lkp_user.cod_source_system = 7
						and lkp_contact.cod_source_system = 12
						and lkp_user.cod_source_system = lkp_source_system.cod_source_system
						and fac.ad_id = ads.id
						and fac.livesync_dbname = lkp_source_system.opr_source_system
						and ads.user_id = lkp_user.opr_atlas_user
						and lkp_user.valid_to = 20991231
						and lower(lkp_contact.email) = lower(lkp_user.dsc_atlas_user)
						and lkp_contact.valid_to = 20991231
						and scai.cod_integration = 50000
						and trunc(fac.posted) between trunc(sysdate) - 30 and trunc(sysdate)
						and scai.cod_country = 2
						and ads.livesync_dbname = 'otomotopl'
					group by
						lkp_contact.cod_contact_parent,
						lkp_contact.cod_contact,
						scai.dat_processing,
						lkp_contact.cod_source_system,
						ads.id
					) source
				group by
					source.cod_contact_parent,
					source.cod_contact,
					source.dat_processing,
					source.cod_source_system
				) inner_core,
				crm_integration_anlt.t_lkp_contact base_contact
    where
      base_contact.cod_contact = inner_core.cod_contact (+)
      and base_contact.cod_source_system = 12
      and base_contact.valid_to = 20991231
	) a,
	crm_integration_anlt.t_rel_scai_country_integration scai,
	(
		select
			rel.cod_custom_field,
			rel.flg_active
		from
			crm_integration_anlt.t_lkp_kpi kpi,
			crm_integration_anlt.t_rel_kpi_custom_field rel
		where
			kpi.cod_kpi = rel.cod_kpi
			and lower(kpi.dsc_kpi) = '# replies'
			and rel.cod_source_system = 12
	) kpi_custom_field
	where
		scai.cod_integration = 50000
		and kpi_custom_field.flg_active = 1
			and scai.cod_country = 2;


  
 --Calculate for employees
create temp table tmp_pl_otomoto_calc_replies_2 as
select source.cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	cast(coalesce(source.custom_field_value, '0') as varchar) custom_field_value
from tmp_pl_otomoto_calc_replies_1 source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where 1 = 1
  and source.cod_contact_parent is not null
  and source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);



--Calculate for companies and contacts not associated with companies
create temp table tmp_pl_otomoto_calc_replies_3 as
select nvl(source.cod_contact_parent, source.cod_contact) as cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	coalesce(cast(sum(source.custom_field_value) as varchar), '0') custom_field_value
from tmp_pl_otomoto_calc_replies_1 source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where 1 = 1
  and source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and nvl(source.cod_contact_parent, source.cod_contact) = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null)
  group by
  source.cod_custom_field,
  source.dat_snap,
  source.cod_source_system,
	nvl(source.cod_contact_parent,source.cod_contact)
	 ;



-- HST INSERT - KPI OLX.BASE.023 (# Replies)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in
			(select cod_contact, cod_custom_field from tmp_pl_otomoto_calc_replies_2
			union
			select cod_contact, cod_custom_field from tmp_pl_otomoto_calc_replies_3);



-- SNAP DELETE - KPI OLX.BASE.023 (# Replies)
delete from crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in
			(select cod_contact, cod_custom_field from tmp_pl_otomoto_calc_replies_2
			union
			select cod_contact, cod_custom_field from tmp_pl_otomoto_calc_replies_3);



-- OLX.BASE.023 (# Replies)
insert into crm_integration_anlt.t_fac_base_integration_snap
  select
    *
  from
    (select * from tmp_pl_otomoto_calc_replies_2
		union
		select * from tmp_pl_otomoto_calc_replies_3);



--$$$

-- CREATE TMP - KPI OLX.BASE.081 (# Replies per Ad)
create temp table tmp_pl_otomoto_calc_replies_per_ad_1 as
select
  a.cod_contact,
  a.cod_contact_parent,
  kpi_custom_field.cod_custom_field,
  scai.dat_processing dat_snap,
  coalesce(a.cod_source_system,12) cod_source_system,
  coalesce(a.custom_field_value, '0') custom_field_value
from
	(
		select
      base_contact.cod_contact,
      base_contact.cod_contact_parent,
      inner_core.dat_snap,
      base_contact.cod_source_system,
      inner_core.custom_field_value
		from
  		(
				select
					source.cod_contact_parent,
					source.cod_contact,
					source.dat_processing dat_snap,
					source.cod_source_system,
					cast(sum(nr_replies) / count(distinct source.id) as varchar) custom_field_value --nr_replies_per_ad,
				from
					(
					select
						lkp_contact.cod_contact_parent,
						lkp_contact.cod_contact,
						scai.dat_processing,
						lkp_contact.cod_source_system,
						ads.id,
						count(*) nr_replies
					from
						db_atlas_verticals.answers fac,
						crm_integration_anlt.t_lkp_source_system lkp_source_system,
						db_atlas_verticals.ads ads,
						crm_integration_anlt.t_lkp_atlas_user lkp_user,
						crm_integration_anlt.t_lkp_contact lkp_contact,
						crm_integration_anlt.t_rel_scai_country_integration scai
					where
						lkp_user.cod_source_system = 7
						and lkp_contact.cod_source_system = 12
						and lkp_user.cod_source_system = lkp_source_system.cod_source_system
						and fac.ad_id = ads.id
						and ads.status = 'active'
						and fac.livesync_dbname = lkp_source_system.opr_source_system
						and ads.user_id = lkp_user.opr_atlas_user
						and lkp_user.valid_to = 20991231
						and lower(lkp_contact.email) = lower(lkp_user.dsc_atlas_user)
						and lkp_contact.valid_to = 20991231
						and scai.cod_integration = 50000
						and trunc(fac.posted) between trunc(sysdate) - 30 and trunc(sysdate)
						and scai.cod_country = 2
						and ads.livesync_dbname = 'otomotopl'
				group by
					lkp_contact.cod_contact_parent,
						lkp_contact.cod_contact,
						scai.dat_processing,
						lkp_contact.cod_source_system,
						ads.id
					) source
				group by
					source.cod_contact_parent,
					source.cod_contact,
					source.dat_processing,
					source.cod_source_system
				) inner_core,
				crm_integration_anlt.t_lkp_contact base_contact
			where
				base_contact.cod_contact = inner_core.cod_contact (+)
				and base_contact.cod_source_system = 12
				and base_contact.valid_to = 20991231
			) a,
		crm_integration_anlt.t_rel_scai_country_integration scai,
		(
			select
				rel.cod_custom_field,
				rel.flg_active
			from
				crm_integration_anlt.t_lkp_kpi kpi,
				crm_integration_anlt.t_rel_kpi_custom_field rel
			where
				kpi.cod_kpi = rel.cod_kpi
				and lower(kpi.dsc_kpi) = '# replies per ad'
				and rel.cod_source_system = 12
		) kpi_custom_field
		where
			scai.cod_integration = 50000
			and kpi_custom_field.flg_active = 1
			and scai.cod_country = 2;



--Calculate for employees
create temp table tmp_pl_otomoto_calc_replies_per_ad_2 as
select source.cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	cast(coalesce(source.custom_field_value, '0') as varchar) custom_field_value
from tmp_pl_otomoto_calc_replies_per_ad_1 source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where 1 = 1
  and source.cod_contact_parent is not null
  and source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);
 


--Calculate for companies and contacts not associated with companies
create temp table tmp_pl_otomoto_calc_replies_per_ad_3 as
select nvl(source.cod_contact_parent, source.cod_contact) as cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	coalesce(cast(sum(source.custom_field_value) as varchar), '0') custom_field_value
from tmp_pl_otomoto_calc_replies_per_ad_1 source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where 1 = 1
  and source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and nvl(source.cod_contact_parent, source.cod_contact) = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null)
  group by
  source.cod_custom_field,
  source.dat_snap,
  source.cod_source_system,
	nvl(source.cod_contact_parent,source.cod_contact)
	 ;


-- HST INSERT - KPI OLX.BASE.081 (# Replies per Ad)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in
			(select cod_contact, cod_custom_field from tmp_pl_otomoto_calc_replies_per_ad_2
			union
			select cod_contact, cod_custom_field from tmp_pl_otomoto_calc_replies_per_ad_3);



-- SNAP DELETE - KPI OLX.BASE.081 (# Replies per Ad)
DELETE FROM crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in
			(select cod_contact, cod_custom_field from tmp_pl_otomoto_calc_replies_per_ad_2
			union
			select cod_contact, cod_custom_field from tmp_pl_otomoto_calc_replies_per_ad_3);



-- OLX.BASE.081 (# Replies per Ad)
insert into crm_integration_anlt.t_fac_base_integration_snap
  select
    *
  from
    (select * from tmp_pl_otomoto_calc_replies_per_ad_2
	union
	select * from tmp_pl_otomoto_calc_replies_per_ad_3);



--$$$

-- CREATE TMP - KPI OLX.BASE.082 (# Ads with replies)
create temp table tmp_pl_otomoto_calc_ads_with_replies_1 as
select
  a.cod_contact,
  a.cod_contact_parent,
  kpi_custom_field.cod_custom_field,
  scai.dat_processing dat_snap,
  coalesce(a.cod_source_system,12) cod_source_system,
  coalesce(a.custom_field_value, '0') custom_field_value
from
  (
	select
	  source.cod_contact,
	  source.cod_contact_parent,
	  source.dat_processing dat_snap,
	  source.cod_source_system,
	  cast(count(distinct source.id) as varchar) custom_field_value --nr_replies_per_ad,
	from
	  (
		select
			base_contact.cod_contact,
			base_contact.cod_contact_parent,
			inner_core.dat_processing,
			base_contact.cod_source_system,
			inner_core.id,
			inner_core.nr_replies
		from
			(
			select
				lkp_contact.cod_contact_parent,
				lkp_contact.cod_contact,
				scai.dat_processing,
				lkp_contact.cod_source_system,
				ads.id,
				count(*) nr_replies
			from
				db_atlas_verticals.answers fac,
				crm_integration_anlt.t_lkp_source_system lkp_source_system,
				db_atlas_verticals.ads ads,
				crm_integration_anlt.t_lkp_atlas_user lkp_user,
				crm_integration_anlt.t_lkp_contact lkp_contact,
				crm_integration_anlt.t_rel_scai_country_integration scai
			where
				lkp_user.cod_source_system = 7
				and lkp_contact.cod_source_system = 12
				and lkp_user.cod_source_system = lkp_source_system.cod_source_system
				and fac.ad_id = ads.id
				and ads.status = 'active'
				and fac.livesync_dbname = lkp_source_system.opr_source_system
				and ads.user_id = lkp_user.opr_atlas_user
				and lkp_user.valid_to = 20991231
				and lower(lkp_contact.email) = lower(lkp_user.dsc_atlas_user)
				and lkp_contact.valid_to = 20991231
				and scai.cod_integration = 50000
				and trunc(fac.posted) between trunc(sysdate) - 30 and trunc(sysdate)
				and scai.cod_country = 2
				and ads.livesync_dbname = 'otomotopl'
			group by
				lkp_contact.cod_contact_parent,
				lkp_contact.cod_contact,
				scai.dat_processing,
				lkp_contact.cod_source_system,
				ads.id
			) inner_core,
			crm_integration_anlt.t_lkp_contact base_contact
		where
		  base_contact.cod_contact = inner_core.cod_contact (+)
		  and base_contact.cod_source_system = 12
   	 	  and base_contact.valid_to = 20991231
		) source
	group by
	  source.cod_contact,
	  source.cod_contact_parent,
	  source.dat_processing,
	  source.cod_source_system
	) a,
	crm_integration_anlt.t_rel_scai_country_integration scai,
	(
		select
			rel.cod_custom_field,
			rel.flg_active
		from
			crm_integration_anlt.t_lkp_kpi kpi,
			crm_integration_anlt.t_rel_kpi_custom_field rel
		where
			kpi.cod_kpi = rel.cod_kpi
			and lower(kpi.dsc_kpi) = '# ads with replies'
			and rel.cod_source_system = 12
	) kpi_custom_field
	where
		scai.cod_integration = 50000
		and kpi_custom_field.flg_active = 1
		and scai.cod_country = 2;



--Calculate for employees
create temp table tmp_pl_otomoto_calc_ads_with_replies_2 as
select source.cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	cast(coalesce(source.custom_field_value, '0') as varchar) custom_field_value
from tmp_pl_otomoto_calc_ads_with_replies_1 source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where 1 = 1
  and source.cod_contact_parent is not null
  and source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);



--Calculate for companies and contacts not associated with companies
create temp table tmp_pl_otomoto_calc_ads_with_replies_3 as
   select nvl(source.cod_contact_parent, source.cod_contact) as cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	coalesce(cast(sum(source.custom_field_value) as varchar), '0') custom_field_value
	from tmp_pl_otomoto_calc_ads_with_replies_1 source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where 1 = 1
  and source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and nvl(source.cod_contact_parent, source.cod_contact) = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null)
  group by
  source.cod_custom_field,
  source.dat_snap,
  source.cod_source_system,
	nvl(source.cod_contact_parent,source.cod_contact)
	 ;   



-- HST INSERT - KPI OLX.BASE.082 (# Ads with replies)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in
			(select cod_contact, cod_custom_field from tmp_pl_otomoto_calc_ads_with_replies_2
			union
			select cod_contact, cod_custom_field from tmp_pl_otomoto_calc_ads_with_replies_3);


-- SNAP DELETE - KPI OLX.BASE.082 (# Ads with replies)
delete from crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in
			(select cod_contact, cod_custom_field from tmp_pl_otomoto_calc_ads_with_replies_2
			union
			select cod_contact, cod_custom_field from tmp_pl_otomoto_calc_ads_with_replies_3);



 -- OLX.BASE.082 (# Ads with replies)
insert into crm_integration_anlt.t_fac_base_integration_snap
  select
      *
  from
    (select * from tmp_pl_otomoto_calc_ads_with_replies_2
	union
	select * from tmp_pl_otomoto_calc_ads_with_replies_3);



--$$$

-- CREATE TMP - KPI OLX.BASE.084 (# Views)
create temp table tmp_pl_otomoto_calc_views_1 as
select
      a.cod_contact,
	  a.cod_contact_parent,
      kpi_custom_field.cod_custom_field,
      scai.dat_processing dat_snap,
      isnull(a.cod_source_system,12) cod_source_system,
      a.custom_field_value custom_field_value
    from
      (
				select
					cod_contact,
					cod_contact_parent,
					dat_snap,
					cod_source_system,
					cast(sum(nbr_views) as varchar) custom_field_value
				from
					(
						select
							base_contact.cod_contact,
							base_contact.cod_contact_parent,
							inner_core.server_date_day,
							inner_core.dat_snap,
							base_contact.cod_source_system,
							inner_core.nbr_views
						from
							(
								select
									base.cod_contact,
									base.cod_contact_parent,
									web.server_date_day,
									dat_snap,
									cod_source_system,
									count(*) nbr_views
								from
									hydra_verticals.web web,
									(
										select
											base_contact.cod_contact_parent,
											base_contact.cod_contact,
											scai.dat_processing dat_snap,
											base_contact.cod_source_system,
											atlas_user.opr_atlas_user
										from
											crm_integration_anlt.t_lkp_atlas_user atlas_user,
											crm_integration_anlt.t_lkp_contact base_contact,
											crm_integration_anlt.t_rel_scai_country_integration scai
										where
											atlas_user.cod_source_system = 7
											and base_contact.cod_source_system = 12
											and base_contact.cod_atlas_user = atlas_user.cod_atlas_user
											and atlas_user.valid_to = 20991231
											and base_contact.valid_to = 20991231
											and scai.cod_integration = 50000
											and scai.cod_country = 2
											--and base_contact.cod_contact_parent = 306798
									) base,
									db_atlas_verticals.ads ads
								where
									web.server_date_day >= dateadd(day,-30,sysdate)
									and web.country_code = 'PL'
									and web.stream = 'v-otomoto-web'
									and trackname = 'ad_page'
									and web.ad_id = ads.id
									and web.seller_id = base.opr_atlas_user
									and ads.livesync_dbname = 'otomotopl'
								group by
									base.cod_contact,
									base.cod_contact_parent,
									dat_snap,
									cod_source_system,
									web.server_date_day

								union all

								select
										base.cod_contact,
									base.cod_contact_parent,
									ios.server_date_day,
									dat_snap,
									cod_source_system,
									count(*) nbr_platform_interactions
								from
									hydra_verticals.ios ios,
									(
										select
											base_contact.cod_contact_parent,
											base_contact.cod_contact,
											scai.dat_processing dat_snap,
											base_contact.cod_source_system,
											atlas_user.opr_atlas_user
										from
											crm_integration_anlt.t_lkp_atlas_user atlas_user,
											crm_integration_anlt.t_lkp_contact base_contact,
											crm_integration_anlt.t_rel_scai_country_integration scai
										where
											atlas_user.cod_source_system = 7
											and base_contact.cod_source_system = 12
											and base_contact.cod_atlas_user = atlas_user.cod_atlas_user
											and atlas_user.valid_to = 20991231
											and base_contact.valid_to = 20991231
											and scai.cod_integration = 50000
											and scai.cod_country = 2
											--and base_contact.cod_contact_parent = 306798
									) base,
									db_atlas_verticals.ads ads
								where
									ios.server_date_day >= dateadd(day,-30,sysdate)
									and ios.country_code = 'PL'
									and trackname = 'ad_page'
									and ios.stream = 'v-otomoto-ios'
									and ios.ad_id = ads.id
									and ios.seller_id = base.opr_atlas_user
									and ads.livesync_dbname = 'otomotopl'
								group by
										base.cod_contact,
									base.cod_contact_parent,
									ios.server_date_day,
									dat_snap,
									cod_source_system

								union all

								select
									base.cod_contact,
									base.cod_contact_parent,
									android.server_date_day,
									dat_snap,
									cod_source_system,
									count(*) nbr_platform_interactions
								from
									hydra_verticals.android android,
									(
										select
											base_contact.cod_contact_parent,
											base_contact.cod_contact,
											scai.dat_processing dat_snap,
											base_contact.cod_source_system,
											atlas_user.opr_atlas_user
										from
											crm_integration_anlt.t_lkp_atlas_user atlas_user,
											crm_integration_anlt.t_lkp_contact base_contact,
											crm_integration_anlt.t_rel_scai_country_integration scai
										where
											atlas_user.cod_source_system = 7
											and base_contact.cod_source_system = 12
											and base_contact.cod_atlas_user = atlas_user.cod_atlas_user
											and atlas_user.valid_to = 20991231
											and base_contact.valid_to = 20991231
											and scai.cod_integration = 50000
											and scai.cod_country = 2
											--and base_contact.cod_contact_parent = 306798
									) base,
									db_atlas_verticals.ads ads
								where
									android.server_date_day >= dateadd(day,-30,sysdate)
									and android.country_code = 'PL'
									and android.stream = 'v-otomoto-android'
									and trackname = 'ad_page'
									and android.ad_id = ads.id
									and android.seller_id = base.opr_atlas_user
									and ads.livesync_dbname = 'otomotopl'
								group by
									base.cod_contact,
									base.cod_contact_parent,
									android.server_date_day,
									dat_snap,
									cod_source_system
							) inner_core,
							crm_integration_anlt.t_lkp_contact base_contact
					where
						base_contact.cod_contact = inner_core.cod_contact (+)
						and base_contact.cod_source_system = 12
						and base_contact.valid_to = 20991231
					) core
				group by
				    cod_contact,
					cod_contact_parent,
					dat_snap,
					cod_source_system
      ) a,
      crm_integration_anlt.t_rel_scai_country_integration scai,
			(
				select
					rel.cod_custom_field,
					rel.flg_active
				from
					crm_integration_anlt.t_lkp_kpi kpi,
					crm_integration_anlt.t_rel_kpi_custom_field rel
				where
					kpi.cod_kpi = rel.cod_kpi
					and lower(kpi.dsc_kpi) = '# views'
					and rel.cod_source_system = 12
			) kpi_custom_field
    where
      1=1
      and scai.cod_integration = 50000
	  and kpi_custom_field.flg_active = 1
	  and scai.cod_country = 2;



--Calculate for employees
create temp table tmp_pl_otomoto_calc_views_2 as
select source.cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	cast(coalesce(source.custom_field_value, '0') as varchar) custom_field_value
from tmp_pl_otomoto_calc_views_1 source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where 1 = 1
  and source.cod_contact_parent is not null
  and source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);


--Calculate for companies and contacts not associated with companies
create temp table tmp_pl_otomoto_calc_views_3 as
select nvl(source.cod_contact_parent, source.cod_contact) as cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	coalesce(cast(max(source.custom_field_value) as varchar), '0') custom_field_value
from tmp_pl_otomoto_calc_views_1 source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where 1 = 1
  and source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and nvl(source.cod_contact_parent, source.cod_contact) = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null)
  group by
  source.cod_custom_field,
  source.dat_snap,
  source.cod_source_system,
	nvl(source.cod_contact_parent,source.cod_contact);


	
-- HST INSERT - KPI OLX.BASE.084 (# Views)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in
			(select cod_contact, cod_custom_field from tmp_pl_otomoto_calc_views_2
			union
			select cod_contact, cod_custom_field from tmp_pl_otomoto_calc_views_3)
			;


-- SNAP DELETE - KPI OLX.BASE.084 (# Views)
delete from crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in
			(select cod_contact, cod_custom_field from tmp_pl_otomoto_calc_views_2
			union
			select cod_contact, cod_custom_field from tmp_pl_otomoto_calc_views_3)
			;



-- OLX.BASE.084 (# Views)
insert into crm_integration_anlt.t_fac_base_integration_snap
  select
    *
  from
    (select * from tmp_pl_otomoto_calc_views_2
		union
		select * from tmp_pl_otomoto_calc_views_3);



--$$$

-- CREATE TEMPORARY TABLE - KPI OLX.BASE.088 (Active package expiry date)
create temp table tmp_pl_otomoto_calc_active_package_expiry_date_1 as
select
  source.cod_contact,
  source.cod_contact_parent,
  source.cod_custom_field,
  source.dat_snap,
  source.cod_source_system,
  source.custom_field_value
from
  (
    select
      lkp_contact.cod_contact,
      lkp_contact.cod_contact_parent,
      kpi_custom_field.cod_custom_field,
      scai.dat_processing dat_snap,
      lkp_contact.cod_source_system,
      nvl(billing.ending_time,'1900-01-01 00:00:00') custom_field_value
    from
      (
        select
          lkp_atlas_user.cod_atlas_user,
          billing_periods.ending_time
        from
          db_atlas_verticals.billing_periods billing_periods,
          crm_integration_anlt.t_lkp_atlas_user lkp_atlas_user
        where
          lkp_atlas_user.opr_atlas_user = billing_periods.user_id
          and lkp_atlas_user.valid_to = 20991231
          and sysdate between billing_periods.starting_time and billing_periods.ending_time
          and lkp_atlas_user.cod_source_system = 7
          and billing_periods.livesync_dbname = 'otomotopl'
      ) billing,
      (
        select
          rel.cod_custom_field,
          rel.flg_active
        from
          crm_integration_anlt.t_lkp_kpi kpi,
          crm_integration_anlt.t_rel_kpi_custom_field rel
        where
          kpi.cod_kpi = rel.cod_kpi
          and lower(kpi.dsc_kpi) = 'active package expiry date'
          and rel.cod_source_system = 12
      ) kpi_custom_field,
      crm_integration_anlt.t_lkp_contact lkp_contact,
      crm_integration_anlt.t_rel_scai_country_integration scai
    where
      lkp_contact.cod_atlas_user = billing.cod_atlas_user (+)
      and lkp_contact.valid_to = 20991231
      and lkp_contact.cod_source_system = 12
      and kpi_custom_field.flg_active = 1
      and scai.cod_integration = 50000
      and scai.cod_country = 2) source,
    crm_integration_anlt.t_fac_base_integration_snap fac_snap
where source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);



--Calculate for employees
create temp table tmp_pl_otomoto_calc_active_package_expiry_date_2 as
select source.cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	cast(coalesce(source.custom_field_value, '1900-01-01 00:00:00') as varchar) custom_field_value
from tmp_pl_otomoto_calc_active_package_expiry_date_1 source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where 1 = 1
  and source.cod_contact_parent is not null
  and source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);



--Calculate for companies and contacts not associated with companies
create temp table tmp_pl_otomoto_calc_active_package_expiry_date_3 as
select nvl(source.cod_contact_parent, source.cod_contact) as cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	coalesce(cast(max(source.custom_field_value) as varchar), '1900-01-01 00:00:00') custom_field_value
from tmp_pl_otomoto_calc_active_package_expiry_date_1 source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where 1 = 1
  and source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and nvl(source.cod_contact_parent, source.cod_contact) = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null)
  group by
  source.cod_custom_field,
  source.dat_snap,
  source.cod_source_system,
	nvl(source.cod_contact_parent,source.cod_contact);

	
-- HST INSERT - KPI OLX.BASE.088 ((Active package expiry date)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in
			(select cod_contact, cod_custom_field from tmp_pl_otomoto_calc_active_package_expiry_date_2
			union
			select cod_contact, cod_custom_field from tmp_pl_otomoto_calc_active_package_expiry_date_3)
			;



-- SNAP DELETE - KPI OLX.BASE.088 (Active package expiry date)
delete from crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in
			(select cod_contact, cod_custom_field from tmp_pl_otomoto_calc_active_package_expiry_date_2
			union
			select cod_contact, cod_custom_field from tmp_pl_otomoto_calc_active_package_expiry_date_3)
			;


-- OLX.BASE.088 (Active package expiry date)
insert into crm_integration_anlt.t_fac_base_integration_snap
  select
    *
  from
    (select * from tmp_pl_otomoto_calc_active_package_expiry_date_2
		union
		select * from tmp_pl_otomoto_calc_active_package_expiry_date_3);


--$$$

-- CREATE TMP - KPI OLX.BASE.014 (Max days since last call)
create temp table tmp_pl_otomoto_calc_max_days_since_last_call_1 as
select
  a.cod_contact,
  a.cod_contact_parent,
  kpi_custom_field.cod_custom_field,
  scai.dat_processing dat_snap,
  isnull(a.cod_source_system,12) cod_source_system,
  isnull(a.custom_field_value, '0') custom_field_value
from
  (
			select
				cod_contact,
				cod_contact_parent,
				dat_snap,
				cod_source_system,
				cast(min(custom_field_value) as varchar) custom_field_value
			from
				(
					select
						lkp_contact.cod_contact_parent,
						lkp_contact.cod_contact,
						scai.dat_processing dat_snap,
						lkp_contact.cod_source_system,
						min(datediff(days, trunc(fac.updated_at), trunc(sysdate))) custom_field_value
					from
						crm_integration_anlt.t_fac_call fac,
						crm_integration_anlt.t_lkp_call_outcome call_outcome,
						crm_integration_anlt.t_lkp_contact lkp_contact,
						crm_integration_anlt.t_rel_scai_country_integration scai
					where
						lkp_contact.cod_source_system = 12
						and fac.cod_source_system = 12
						and call_outcome.cod_source_system = 12
						and call_outcome.cod_call_outcome = fac.cod_call_outcome
						and call_outcome.cod_call_outcome not in (73) --no answer
						and lkp_contact.cod_contact = fac.cod_contact
						and lkp_contact.valid_to = 20991231
						and scai.cod_integration = 50000
						and fac.flg_missed = 0
						and scai.cod_country = 2
						and lkp_contact.cod_contact_parent is not null
					group by
						lkp_contact.cod_contact_parent,
						lkp_contact.cod_source_system,
						lkp_contact.cod_contact_parent,
						lkp_contact.cod_contact,
						scai.dat_processing
				) core
			group by
				cod_contact,
				cod_contact_parent,
				dat_snap,
				cod_source_system
  ) a,
  crm_integration_anlt.t_rel_scai_country_integration scai,
	(
		select
		  rel.cod_custom_field,
		  rel.flg_active
		from
		  crm_integration_anlt.t_lkp_kpi kpi,
		  crm_integration_anlt.t_rel_kpi_custom_field rel
		where
		  kpi.cod_kpi = rel.cod_kpi
		  and lower(kpi.dsc_kpi) = 'max days since last call'
		  and rel.cod_source_system = 12
	) kpi_custom_field
where 1=1
  and scai.cod_integration = 50000
  and scai.cod_country = 2
  and kpi_custom_field.flg_active = 1;



--Calculate for employees
create temp table tmp_pl_otomoto_calc_max_days_since_last_call_2 as
   select source.cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	cast(coalesce(source.custom_field_value, '-') as varchar) custom_field_value
from tmp_pl_otomoto_calc_max_days_since_last_call_1 source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where 1 = 1
  and source.cod_contact_parent is not null
  and source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);



--Calculate for companies and contacts not associated with companies
create temp table tmp_pl_otomoto_calc_max_days_since_last_call_3 as
   select nvl(source.cod_contact_parent, source.cod_contact) as cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	coalesce(cast(min(cast(source.custom_field_value as int)) as varchar), '-') custom_field_value
from tmp_pl_otomoto_calc_max_days_since_last_call_1 source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where 1 = 1
  and source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and nvl(source.cod_contact_parent, source.cod_contact) = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null)
  group by
  source.cod_custom_field,
  source.dat_snap,
  source.cod_source_system,
	nvl(source.cod_contact_parent,source.cod_contact)
	 ;	  



-- HST INSERT - KPI OLX.BASE.014 (Max days since last call)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field)  in
			(select cod_contact, cod_custom_field from tmp_pl_otomoto_calc_max_days_since_last_call_2
			union
			select cod_contact, cod_custom_field from tmp_pl_otomoto_calc_max_days_since_last_call_3);



-- SNAP DELETE - KPI OLX.BASE.014 (Max days since last call)
delete from crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field)  in
			(select cod_contact, cod_custom_field from tmp_pl_otomoto_calc_max_days_since_last_call_2
			union
			select cod_contact, cod_custom_field from tmp_pl_otomoto_calc_max_days_since_last_call_3);



--KPI OLX.BASE.014 (Max days since last call)
insert into crm_integration_anlt.t_fac_base_integration_snap
  select
    *
  from
    (select * from tmp_pl_otomoto_calc_max_days_since_last_call_2
		union
		select * from tmp_pl_otomoto_calc_max_days_since_last_call_3);



--$$$
-- CREATE TMP - KPI OLX.BASE.XXX (Revenue (0) - Total / VAS / Listings)
drop table if exists crm_integration_anlt.otomoto_ads_categories;

create table crm_integration_anlt.otomoto_ads_categories
(
id bigint encode zstd,
created_at_first timestamp encode zstd,
category_id int encode zstd,
new_used varchar(256) encode zstd,
source varchar(256) encode zstd,
sup_category_id int encode zstd
);



insert into crm_integration_anlt.otomoto_ads_categories
select a.id, a.created_at_first, a.category_id, a.new_used,
case when a.external_partner_code is null then 'otomoto' else a.external_partner_code end as source,
coalesce(case when a.category_id in (29,73,65) then a.category_id else b.parent_id end,29) as sup_category_id
from db_atlas_verticals.ads as a
left join db_atlas_verticals.categories as b
on a.category_id=b.id
and a.livesync_dbname = b.livesync_dbname
where a.livesync_dbname = 'otomotopl'
and a.private_business='business' --and a.created_at_first>(select max(created_at_first) from forbi.ads_categories) /and a.created_at_first<current_date()/
group by 1,2,3,4,5,6;




create temp table tmp_pl_otomoto_calc_revenue_listings as
select c.id, c.id_index, pi.cod_index_type, c.date, c.category, c.category_no, c.user_id, c.invoice, c.package, c.period, c.rank,
 (case
  when category = 'P' and package = 1 and rank >2500 then 0.13
  when category = 'P' and package = 2 and rank >2500 then 0.33
  when category = 'P' and package = 3 and rank >2500 then 0.39
  when category = 'C' and package = 1 and id_index = 51 and rank >150 then 10.00
  when category = 'C' and package = 2 and id_index = 55 and rank >150 then 19.00
  when category = 'C' and package = 3 and id_index = 55 and rank >150 then 23.00
  when category = 'C' and package = 1 and id_index = 115 and rank >150 then 12.00
  when category = 'C' and package = 2 and id_index = 117 and rank >150 then 21.00
  when category = 'C' and package = 3 and id_index = 117 and rank >150 then 25.00
  when category = 'M' and package = 1 and id_index = 115 and rank >150 then 8.00
  when category = 'M' and package = 2 and id_index = 117 and rank >150 then 11.00
  when category = 'M' and package = 3 and id_index = 117 and rank >150 then 17.00
  when category = 'N' then 0.00
  else cast(p.subs as numeric(15,2)) end) as subs_value
from (
	select
	id, id_index, date, category, category_no, user_id, invoice, package, period,
	  rank() over (partition by user_id, period, category order by id) rank
	from (
		  select
		  w.id, w.id_index, w.date, w.category, w.category_no, w.user_id, w.invoice, w.package, w.diff as period
		from (
			select
			pup.id as id, pup.date, pup.id_index,
			case
			when (ad.sup_category_id = 65 and pup.id_index in (115, 117)) then 'M'
			when ad.sup_category_id = 161 then 'P'
			when ad.sup_category_id is null then 'C'
			else 'C'
			end as category,
			ad.sup_category_id as category_no,
			pup.id_user as user_id,
			add_months(ub.next_invoice_date, -ceil(months_between(ub.next_invoice_date, pup.date))::INTEGER) as invoice,
			case when bp.package_id > 3 then bp.package_id - 3 else coalesce(bp.package_id,'1') end as package,
			-ceil(months_between(ub.next_invoice_date, pup.date)) as diff
			from db_atlas_verticals.paidads_user_payments pup
			join db_atlas_verticals.users_business ub on pup.id_user=ub.id and pup.livesync_dbname = ub.livesync_dbname
			left join crm_integration_anlt.otomoto_ads_categories ad on pup.id_ad = ad.id
			left join db_atlas_verticals.billing_periods as bp on pup.id_user = bp.user_id and pup.livesync_dbname = bp.livesync_dbname
			and (
			add_months(ub.next_invoice_date,(datediff(month,ub.next_invoice_date, pup.date))) = bp.ending_time
			or
			add_months(dateadd(day, -1, ub.next_invoice_date), (-ceil(months_between(ub.next_invoice_date, pup.date))::INTEGER)) = bp.starting_time or
			add_months(dateadd(day, -2, ub.next_invoice_date), (-ceil(months_between(ub.next_invoice_date, pup.date))::INTEGER)) = bp.starting_time or
			add_months(dateadd(day, -3, ub.next_invoice_date), (-ceil(months_between(ub.next_invoice_date, pup.date))::INTEGER)) = bp.starting_time
			)
			where
			pup.livesync_dbname = 'otomotopl'
			and payment_provider='postpay'
			and pup.date >= add_months(ub.next_invoice_date, -5)
			and pup.date < dateadd(day, -1, ub.next_invoice_date)
			and is_removed_from_invoice = 0
			and pup.is_invalid_item = 0
			and pup.id_index in (51,55,115,117)
			--and user_id = 68764
		  ) w
		order by
			w.user_id asc,
			w.diff asc,
			w.category,
			w.id asc
		) z
	where 1=1
	order by user_id, category, period, id
	) c
left join crm_integration_anlt.otomotopl_pricing p on  c.id_index = p.index_id
                              and c.category = p.sup_category
                              and c.package = p.pack
                            and c.rank = p.dense_rank
left join crm_integration_anlt.v_lkp_paidad_index pi on pi.opr_paidad_index = c.id_index and pi.cod_source_system = 7;

	

create temp table tmp_pl_otomoto_calc_revenue_vas as
SELECT 
	pup.id_user as user_id, 
	pup.id_transaction, 
	pup.date, 
	pup.id_ad, 
	pup.name, 
	pup.payment_provider,
	cast( -1 * case when pup.payment_provider='postpay' then round(pup.price/1.23, 2) else round(wm.amount/1.23, 2) end as numeric(15,2)) as sales_vas_value,
	1 as cod_index_type,
	-ceil(months_between(ub.next_invoice_date, pup.date)) as period
FROM db_atlas_verticals.paidads_user_payments as pup
	join db_atlas_verticals.users_business ub on pup.id_user=ub.id and pup.livesync_dbname = ub.livesync_dbname
	LEFT OUTER JOIN db_atlas_verticals.wallet_movements as wm
		on pup.id_user=wm.user_id and pup.id_ad=wm.ad_id and pup.id_transaction=wm.session_id and pup.id_index = wm.index_id
WHERE 1=1
AND pup.payment_provider in ('postpay', 'account')
AND (pup.is_removed_from_invoice=0 or pup.is_removed_from_invoice is null)
AND (pup.is_invalid_item=0 or pup.is_invalid_item is null)
and (pup.livesync_dbname = 'otomotopl'
	or wm.livesync_dbname = 'otomotopl')
AND pup.DATE>= '2018-01-01' 
and  (pup.id_index not in (0,51,49,55,61,105,113,115,117,119) 
	or (pup.id_index=49 and pup.price<0))
;



-- CREATE TMP - KPI OLX.BASE.099 (Revenue (0) - Total)
create temp table tmp_pl_otomoto_calc_revenue_listings_0_total_1 as
select
	core.cod_contact,
	core.cod_contact_parent,
	core.cod_custom_field,
	core.dat_snap,
	core.cod_source_system,
	core.custom_field_value
from
	(
		select
			cod_contact,
			cod_contact_parent,
			cod_custom_field,
			dat_snap,
			cod_source_system,
			cast(round(nvl(revenue_value,0),2) as varchar) custom_field_value
		from
			(
				select
					rev_cars.cod_contact,
					rev_cars.cod_contact_parent,
					kpi_custom_field.cod_custom_field,
					rev_cars.dat_snap,
					rev_cars.cod_source_system,
					revenue_value
				from
					(
						select
						  lkp_contact.cod_contact,
						  lkp_contact.cod_contact_parent,
						  lkp_contact.cod_source_system,
						  scai.dat_processing dat_snap,
						  sum(tmp_revenue.value) revenue_value
						from
						  (select user_id, subs_value as value  from tmp_pl_otomoto_calc_revenue_listings where cod_index_type in (2) and period = -1
							union
							select user_id, sales_vas_value as value from tmp_pl_otomoto_calc_revenue_vas where cod_index_type in (1) and period = -1) tmp_revenue, 
						  crm_integration_anlt.t_lkp_atlas_user lkp_atlas_user,
						  crm_integration_anlt.t_lkp_contact lkp_contact,
						  crm_integration_anlt.t_rel_scai_country_integration scai
						where
						  lkp_contact.cod_atlas_user = lkp_atlas_user.cod_atlas_user (+)
						  and lkp_atlas_user.opr_atlas_user = tmp_revenue.user_id (+)
						  and lkp_contact.valid_to = 20991231
						  and lkp_contact.cod_source_system = 12
		                  and lkp_atlas_user.valid_to (+) = 20991231
						  and lkp_atlas_user.cod_source_system (+) = 7
						  and scai.cod_integration = 50000
						  and scai.cod_country = 2
						group by
						  lkp_contact.cod_contact,
						  lkp_contact.cod_contact_parent,
						  lkp_contact.cod_source_system,
						  scai.dat_processing
					) rev_cars,
					(
						select
							rel.cod_custom_field,
							rel.flg_active
						from
							crm_integration_anlt.t_lkp_kpi kpi,
							crm_integration_anlt.t_rel_kpi_custom_field rel
						where
							kpi.cod_kpi = rel.cod_kpi
							and lower(kpi.dsc_kpi) = 'revenue (0) - total'
							and rel.cod_source_system = 12
					) kpi_custom_field
				where
					kpi_custom_field.flg_active = 1
			) core
	) core,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where
	core.cod_source_system = fac_snap.cod_source_system (+)
	and core.cod_custom_field = fac_snap.cod_custom_field (+)
	and core.cod_contact = fac_snap.cod_contact (+)
	and (core.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);



--Calculate for employees
create temp table tmp_pl_otomoto_calc_revenue_listings_0_total_2 as
select source.cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	source.custom_field_value
from tmp_pl_otomoto_calc_revenue_listings_0_total_1 source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where 1 = 1
  and source.cod_contact_parent is not null
  and source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);



--Calculate for companies and contacts not associated with companies
create temp table tmp_pl_otomoto_calc_revenue_listings_0_total_3 as
/*select nvl(source.cod_contact_parent, source.cod_contact) as cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system
	,cast(sum(CAST(source.custom_field_value AS NUMERIC(15,2))) as varchar) custom_field_value
from tmp_pl_otomoto_calc_revenue_listings_0_total_1 source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where 1 = 1
  and source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and nvl(source.cod_contact_parent, source.cod_contact) = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null)
  group by
  source.cod_custom_field,
  source.dat_snap,
  source.cod_source_system,
	nvl(source.cod_contact_parent,source.cod_contact)
	 ;*/
select
 coalesce(core.cod_contact_parent,core.cod_contact) cod_contact,
 core.cod_custom_field,
 core.dat_snap,
 core.cod_source_system,
 cast(round(sum(core.revenue),2) as varchar) custom_field_value
from
 (
   select
     *,
     sum(cast(custom_field_value as numeric(15,2))) over (partition by cod_atlas_user) / nbr_atlas_users revenue,
     row_number() over (partition by coalesce(cod_contact_parent,cod_contact), cod_atlas_user order by cod_contact) rn
   from
     (
       select
         a.*,
         b.cod_atlas_user,
         count(*) over (partition by cod_atlas_user) nbr_atlas_users
       from
         tmp_pl_otomoto_calc_revenue_listings_0_total_1 a,
         crm_integration_anlt.t_lkp_contact b
       where 1
         --a.cod_contact in (320977,322379,327830,289792,327855)
         and a.cod_contact = b.cod_contact
         and a.cod_source_system = b.cod_source_system
         and b.valid_to = 20991231
     ) inner_core
 ) core,
 crm_integration_anlt.t_fac_base_integration_snap fac_snap
where
  rn = 1
  and core.cod_source_system = fac_snap.cod_source_system (+)
  and core.cod_custom_field = fac_snap.cod_custom_field (+)
  and nvl(core.cod_contact_parent, core.cod_contact) = fac_snap.cod_contact (+)
  and (core.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null)
group by
 coalesce(core.cod_contact_parent,core.cod_contact),
 core.cod_custom_field,
 core.dat_snap,
 core.cod_source_system;



-- HST INSERT - KPI OLX.BASE.099 (Revenue (0) - Total)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in
			(select cod_contact, cod_custom_field from tmp_pl_otomoto_calc_revenue_listings_0_total_2
			union
			select cod_contact, cod_custom_field from tmp_pl_otomoto_calc_revenue_listings_0_total_3);



-- SNAP DELETE - KPI OLX.BASE.099 (Revenue (0) - Total)
delete from crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in
			(select cod_contact, cod_custom_field from tmp_pl_otomoto_calc_revenue_listings_0_total_2
			union
			select cod_contact, cod_custom_field from tmp_pl_otomoto_calc_revenue_listings_0_total_3);



--KPI OLX.BASE.099 (Revenue (0) - Total)
insert into crm_integration_anlt.t_fac_base_integration_snap
	select
		*
	from
		(select * from tmp_pl_otomoto_calc_revenue_listings_0_total_2
		union
		select * from tmp_pl_otomoto_calc_revenue_listings_0_total_3);




-- CREATE TMP - KPI OLX.BASE.099 (Revenue (-1) - Total)
create temp table tmp_pl_otomoto_calc_revenue_listings_1_total_1 as
select
	core.cod_contact,
	core.cod_contact_parent,
	core.cod_custom_field,
	core.dat_snap,
	core.cod_source_system,
	core.custom_field_value
from
	(
		select
			cod_contact,
			cod_contact_parent,
			cod_custom_field,
			dat_snap,
			cod_source_system,
			cast(round(nvl(revenue_value,0),2) as varchar) custom_field_value
		from
			(
				select
					rev_cars.cod_contact,
					rev_cars.cod_contact_parent,
					kpi_custom_field.cod_custom_field,
					rev_cars.dat_snap,
					rev_cars.cod_source_system,
					revenue_value
				from
					(
						select
						  lkp_contact.cod_contact,
						  lkp_contact.cod_contact_parent,
						  lkp_contact.cod_source_system,
						  scai.dat_processing dat_snap,
						  sum(tmp_revenue.value) revenue_value
						from
						  (select user_id, subs_value as value  from tmp_pl_otomoto_calc_revenue_listings where cod_index_type in (2) and period = -2
							union
							select user_id, sales_vas_value as value from tmp_pl_otomoto_calc_revenue_vas where cod_index_type in (1) and period = -2) tmp_revenue, 
						  crm_integration_anlt.t_lkp_atlas_user lkp_atlas_user,
						  crm_integration_anlt.t_lkp_contact lkp_contact,
						  crm_integration_anlt.t_rel_scai_country_integration scai
						where
						  lkp_contact.cod_atlas_user = lkp_atlas_user.cod_atlas_user (+)
						  and lkp_atlas_user.opr_atlas_user = tmp_revenue.user_id (+)
						  and lkp_contact.valid_to = 20991231
						  and lkp_contact.cod_source_system = 12
						  and lkp_atlas_user.valid_to (+) = 20991231
						  and lkp_atlas_user.cod_source_system (+) = 7
						  and scai.cod_integration = 50000
						  and scai.cod_country = 2
						group by
						  lkp_contact.cod_contact,
						  lkp_contact.cod_contact_parent,
						  lkp_contact.cod_source_system,
						  scai.dat_processing
					) rev_cars,
					(
						select
							rel.cod_custom_field,
							rel.flg_active
						from
							crm_integration_anlt.t_lkp_kpi kpi,
							crm_integration_anlt.t_rel_kpi_custom_field rel
						where
							kpi.cod_kpi = rel.cod_kpi
							and lower(kpi.dsc_kpi) = 'revenue (-1) - total'
							and rel.cod_source_system = 12
					) kpi_custom_field
				where
					kpi_custom_field.flg_active = 1
			) core
	) core,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where
	core.cod_source_system = fac_snap.cod_source_system (+)
	and core.cod_custom_field = fac_snap.cod_custom_field (+)
	and core.cod_contact = fac_snap.cod_contact (+)
	and (core.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);



--Calculate for employees
create temp table tmp_pl_otomoto_calc_revenue_listings_1_total_2 as
select source.cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	source.custom_field_value
from tmp_pl_otomoto_calc_revenue_listings_1_total_1 source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where 1 = 1
  and source.cod_contact_parent is not null
  and source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);


--Calculate for companies and contacts not associated with companies
create temp table tmp_pl_otomoto_calc_revenue_listings_1_total_3 as
/*select nvl(source.cod_contact_parent, source.cod_contact) as cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system
	,cast(sum(CAST(source.custom_field_value AS NUMERIC(15,2))) as varchar) custom_field_value
from tmp_pl_otomoto_calc_revenue_listings_1_total_1 source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where 1 = 1
  and source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and nvl(source.cod_contact_parent, source.cod_contact) = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null)
  group by
  source.cod_custom_field,
  source.dat_snap,
  source.cod_source_system,
	nvl(source.cod_contact_parent,source.cod_contact)
	 ;*/
select
 coalesce(core.cod_contact_parent,core.cod_contact) cod_contact,
 core.cod_custom_field,
 core.dat_snap,
 core.cod_source_system,
 cast(round(sum(core.revenue),2) as varchar) custom_field_value
from
 (
   select
     *,
     sum(cast(custom_field_value as numeric(15,2))) over (partition by cod_atlas_user) / nbr_atlas_users revenue,
     row_number() over (partition by coalesce(cod_contact_parent,cod_contact), cod_atlas_user order by cod_contact) rn
   from
     (
       select
         a.*,
         b.cod_atlas_user,
         count(*) over (partition by cod_atlas_user) nbr_atlas_users
       from
         tmp_pl_otomoto_calc_revenue_listings_1_total_1 a,
         crm_integration_anlt.t_lkp_contact b
       where 1
         and a.cod_contact = b.cod_contact
         and a.cod_source_system = b.cod_source_system
         and b.valid_to = 20991231
     ) inner_core
 ) core,
 crm_integration_anlt.t_fac_base_integration_snap fac_snap
where
  rn = 1
  and core.cod_source_system = fac_snap.cod_source_system (+)
  and core.cod_custom_field = fac_snap.cod_custom_field (+)
  and nvl(core.cod_contact_parent, core.cod_contact) = fac_snap.cod_contact (+)
  and (core.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null)
group by
 coalesce(core.cod_contact_parent,core.cod_contact),
 core.cod_custom_field,
 core.dat_snap,
 core.cod_source_system;



-- HST INSERT - KPI OLX.BASE.099 (Revenue (-1) - Total)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in
			(select cod_contact, cod_custom_field from tmp_pl_otomoto_calc_revenue_listings_1_total_2
			union
			select cod_contact, cod_custom_field from tmp_pl_otomoto_calc_revenue_listings_1_total_3);


-- SNAP DELETE - KPI OLX.BASE.099 (Revenue (-1) - Total)
delete from crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in
			(select cod_contact, cod_custom_field from tmp_pl_otomoto_calc_revenue_listings_1_total_2
			union
			select cod_contact, cod_custom_field from tmp_pl_otomoto_calc_revenue_listings_1_total_3);



--KPI OLX.BASE.099 (Revenue (-1) - Total)
insert into crm_integration_anlt.t_fac_base_integration_snap
	select
		*
	from
		(select * from tmp_pl_otomoto_calc_revenue_listings_1_total_2
		union
		select * from tmp_pl_otomoto_calc_revenue_listings_1_total_3);





-- CREATE TMP - KPI OLX.BASE.101 (Revenue (0) - VAS)
create temp table tmp_pl_otomoto_calc_revenue_listings_0_vas_1 as
select
	core.cod_contact,
	core.cod_contact_parent,
	core.cod_custom_field,
	core.dat_snap,
	core.cod_source_system,
	core.custom_field_value
from
	(
		select
			cod_contact,
			cod_contact_parent,
			cod_custom_field,
			dat_snap,
			cod_source_system,
			cast(round(nvl(revenue_value,0),2) as varchar) custom_field_value
		from
			(
				select
					rev_cars.cod_contact,
					rev_cars.cod_contact_parent,
					kpi_custom_field.cod_custom_field,
					rev_cars.dat_snap,
					rev_cars.cod_source_system,
					revenue_value
				from
					(
						select
						  lkp_contact.cod_contact,
						  lkp_contact.cod_contact_parent,
						  lkp_contact.cod_source_system,
						  scai.dat_processing dat_snap,
						  sum(tmp_revenue.sales_vas_value) revenue_value
						from
						  (select * from tmp_pl_otomoto_calc_revenue_vas where cod_index_type = 1 and period = -1) tmp_revenue, -- VAS, Month 0
						  crm_integration_anlt.t_lkp_atlas_user lkp_atlas_user,
						  crm_integration_anlt.t_lkp_contact lkp_contact,
						  crm_integration_anlt.t_rel_scai_country_integration scai
						where
						  lkp_contact.cod_atlas_user = lkp_atlas_user.cod_atlas_user (+)
						  and lkp_atlas_user.opr_atlas_user = tmp_revenue.user_id (+)
						  and lkp_contact.valid_to = 20991231
						  and lkp_contact.cod_source_system = 12
		                  and lkp_atlas_user.valid_to (+) = 20991231
						  and lkp_atlas_user.cod_source_system (+) = 7
						  and scai.cod_integration = 50000
						  and scai.cod_country = 2
						group by
						  lkp_contact.cod_contact,
						  lkp_contact.cod_contact_parent,
						  lkp_contact.cod_source_system,
						  scai.dat_processing
					) rev_cars,
					(
						select
							rel.cod_custom_field,
							rel.flg_active
						from
							crm_integration_anlt.t_lkp_kpi kpi,
							crm_integration_anlt.t_rel_kpi_custom_field rel
						where
							kpi.cod_kpi = rel.cod_kpi
							and lower(kpi.dsc_kpi) = 'revenue (0) - vas'
							and rel.cod_source_system = 12
					) kpi_custom_field
				where
					kpi_custom_field.flg_active = 1
			) core
	) core,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where
	core.cod_source_system = fac_snap.cod_source_system (+)
	and core.cod_custom_field = fac_snap.cod_custom_field (+)
	and core.cod_contact = fac_snap.cod_contact (+)
	and (core.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);



--Calculate for employees
create temp table tmp_pl_otomoto_calc_revenue_listings_0_vas_2 as
select source.cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	source.custom_field_value
from tmp_pl_otomoto_calc_revenue_listings_0_vas_1 source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where 1 = 1
  and source.cod_contact_parent is not null
  and source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);



--Calculate for companies and contacts not associated with companies
create temp table tmp_pl_otomoto_calc_revenue_listings_0_vas_3 as
/*select nvl(source.cod_contact_parent, source.cod_contact) as cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system
	,cast(sum(CAST(source.custom_field_value AS NUMERIC(15,2))) as varchar) custom_field_value
from tmp_pl_otomoto_calc_revenue_listings_0_vas_1 source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where 1 = 1
  and source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and nvl(source.cod_contact_parent, source.cod_contact) = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null)
  group by
  source.cod_custom_field,
  source.dat_snap,
  source.cod_source_system,
	nvl(source.cod_contact_parent,source.cod_contact)
	 ;*/
select
 coalesce(core.cod_contact_parent,core.cod_contact) cod_contact,
 core.cod_custom_field,
 core.dat_snap,
 core.cod_source_system,
 cast(round(sum(core.revenue),2) as varchar) custom_field_value
from
 (
   select
     *,
     sum(cast(custom_field_value as numeric(15,2))) over (partition by cod_atlas_user) / nbr_atlas_users revenue,
     row_number() over (partition by coalesce(cod_contact_parent,cod_contact), cod_atlas_user order by cod_contact) rn
   from
     (
       select
         a.*,
         b.cod_atlas_user,
         count(*) over (partition by cod_atlas_user) nbr_atlas_users
       from
         tmp_pl_otomoto_calc_revenue_listings_0_vas_1 a,
         crm_integration_anlt.t_lkp_contact b
       where 1
         and a.cod_contact = b.cod_contact
         and a.cod_source_system = b.cod_source_system
         and b.valid_to = 20991231
     ) inner_core
 ) core,
 crm_integration_anlt.t_fac_base_integration_snap fac_snap
where
  rn = 1
  and core.cod_source_system = fac_snap.cod_source_system (+)
  and core.cod_custom_field = fac_snap.cod_custom_field (+)
  and nvl(core.cod_contact_parent, core.cod_contact) = fac_snap.cod_contact (+)
  and (core.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null)
group by
 coalesce(core.cod_contact_parent,core.cod_contact),
 core.cod_custom_field,
 core.dat_snap,
 core.cod_source_system;



-- HST INSERT - KPI OLX.BASE.099 (Revenue (0) - VAS)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in
			(select cod_contact, cod_custom_field from tmp_pl_otomoto_calc_revenue_listings_0_vas_2
			union
			select cod_contact, cod_custom_field from tmp_pl_otomoto_calc_revenue_listings_0_vas_3);



-- SNAP DELETE - KPI OLX.BASE.099 (Revenue (0) - VAS)
delete from crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in
			(select cod_contact, cod_custom_field from tmp_pl_otomoto_calc_revenue_listings_0_vas_2
			union
			select cod_contact, cod_custom_field from tmp_pl_otomoto_calc_revenue_listings_0_vas_3);



--KPI OLX.BASE.099 (Revenue (0) - VAS)
insert into crm_integration_anlt.t_fac_base_integration_snap
	select
		*
	from
		(select * from tmp_pl_otomoto_calc_revenue_listings_0_vas_2
		union
		select * from tmp_pl_otomoto_calc_revenue_listings_0_vas_3);




-- CREATE TMP - KPI OLX.BASE.104 (Revenue (-1) - VAS)
create temp table tmp_pl_otomoto_calc_revenue_listings_1_vas_1 as
select
	core.cod_contact,
	core.cod_contact_parent,
	core.cod_custom_field,
	core.dat_snap,
	core.cod_source_system,
	core.custom_field_value
from
	(
		select
			cod_contact,
			cod_contact_parent,
			cod_custom_field,
			dat_snap,
			cod_source_system,
			cast(round(nvl(revenue_value,0),2) as varchar) custom_field_value
		from
			(
				select
					rev_cars.cod_contact,
					rev_cars.cod_contact_parent,
					kpi_custom_field.cod_custom_field,
					rev_cars.dat_snap,
					rev_cars.cod_source_system,
					revenue_value
				from
					(
						select
						  lkp_contact.cod_contact,
						  lkp_contact.cod_contact_parent,
						  lkp_contact.cod_source_system,
						  scai.dat_processing dat_snap,
						  sum(tmp_revenue.sales_vas_value) revenue_value
						from
						  (select * from tmp_pl_otomoto_calc_revenue_vas where cod_index_type = 1 and period = -2) tmp_revenue, -- VAS, Month -1
						  crm_integration_anlt.t_lkp_atlas_user lkp_atlas_user,
						  crm_integration_anlt.t_lkp_contact lkp_contact,
						  crm_integration_anlt.t_rel_scai_country_integration scai
						where
						  lkp_contact.cod_atlas_user = lkp_atlas_user.cod_atlas_user (+)
						  and lkp_atlas_user.opr_atlas_user = tmp_revenue.user_id (+)
						  and lkp_contact.valid_to = 20991231
						  and lkp_contact.cod_source_system = 12
		                  and lkp_atlas_user.valid_to (+) = 20991231
						  and lkp_atlas_user.cod_source_system (+) = 7
						  and scai.cod_integration = 50000
						  and scai.cod_country = 2
						group by
						  lkp_contact.cod_contact,
						  lkp_contact.cod_contact_parent,
						  lkp_contact.cod_source_system,
						  scai.dat_processing
					) rev_cars,
					(
						select
							rel.cod_custom_field,
							rel.flg_active
						from
							crm_integration_anlt.t_lkp_kpi kpi,
							crm_integration_anlt.t_rel_kpi_custom_field rel
						where
							kpi.cod_kpi = rel.cod_kpi
							and lower(kpi.dsc_kpi) = 'revenue (-1) - vas'
							and rel.cod_source_system = 12
					) kpi_custom_field
				where
					kpi_custom_field.flg_active = 1
			) core
	) core,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where
	core.cod_source_system = fac_snap.cod_source_system (+)
	and core.cod_custom_field = fac_snap.cod_custom_field (+)
	and core.cod_contact = fac_snap.cod_contact (+)
	and (core.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);



--Calculate for employees
create temp table tmp_pl_otomoto_calc_revenue_listings_1_vas_2 as
select source.cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	source.custom_field_value
from tmp_pl_otomoto_calc_revenue_listings_1_vas_1 source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where 1 = 1
  and source.cod_contact_parent is not null
  and source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);



--Calculate for companies and contacts not associated with companies
create temp table tmp_pl_otomoto_calc_revenue_listings_1_vas_3 as
/*select nvl(source.cod_contact_parent, source.cod_contact) as cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system
	,cast(sum(CAST(source.custom_field_value AS NUMERIC(15,2))) as varchar) custom_field_value
from tmp_pl_otomoto_calc_revenue_listings_1_vas_1 source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where 1 = 1
  and source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and nvl(source.cod_contact_parent, source.cod_contact) = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null)
  group by
  source.cod_custom_field,
  source.dat_snap,
  source.cod_source_system,
	nvl(source.cod_contact_parent,source.cod_contact)
	 ;*/
select
 coalesce(core.cod_contact_parent,core.cod_contact) cod_contact,
 core.cod_custom_field,
 core.dat_snap,
 core.cod_source_system,
 cast(round(sum(core.revenue),2) as varchar) custom_field_value
from
 (
   select
     *,
     sum(cast(custom_field_value as numeric(15,2))) over (partition by cod_atlas_user) / nbr_atlas_users revenue,
     row_number() over (partition by coalesce(cod_contact_parent,cod_contact), cod_atlas_user order by cod_contact) rn
   from
     (
       select
         a.*,
         b.cod_atlas_user,
         count(*) over (partition by cod_atlas_user) nbr_atlas_users
       from
         tmp_pl_otomoto_calc_revenue_listings_1_vas_1 a,
         crm_integration_anlt.t_lkp_contact b
       where 1
         and a.cod_contact = b.cod_contact
         and a.cod_source_system = b.cod_source_system
         and b.valid_to = 20991231
     ) inner_core
 ) core,
 crm_integration_anlt.t_fac_base_integration_snap fac_snap
where
  rn = 1
  and core.cod_source_system = fac_snap.cod_source_system (+)
  and core.cod_custom_field = fac_snap.cod_custom_field (+)
  and nvl(core.cod_contact_parent, core.cod_contact) = fac_snap.cod_contact (+)
  and (core.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null)
group by
 coalesce(core.cod_contact_parent,core.cod_contact),
 core.cod_custom_field,
 core.dat_snap,
 core.cod_source_system;



-- HST INSERT - KPI OLX.BASE.099 (Revenue (-1) - VAS)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in
			(select cod_contact, cod_custom_field from tmp_pl_otomoto_calc_revenue_listings_1_vas_2
			union
			select cod_contact, cod_custom_field from tmp_pl_otomoto_calc_revenue_listings_1_vas_3);



-- SNAP DELETE - KPI OLX.BASE.099 (Revenue (-1) - VAS)
delete from crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in
			(select cod_contact, cod_custom_field from tmp_pl_otomoto_calc_revenue_listings_1_vas_2
			union
			select cod_contact, cod_custom_field from tmp_pl_otomoto_calc_revenue_listings_1_vas_3);



--KPI OLX.BASE.099 (Revenue (-1) - VAS)
insert into crm_integration_anlt.t_fac_base_integration_snap
	select
		*
	from
		(select * from tmp_pl_otomoto_calc_revenue_listings_1_vas_2
		union
		select * from tmp_pl_otomoto_calc_revenue_listings_1_vas_3);





-- CREATE TMP - KPI OLX.BASE.100 (Revenue (0) - Listings)
create temp table tmp_pl_otomoto_calc_revenue_listings_0_listings_1 as
select
	core.cod_contact,
	core.cod_contact_parent,
	core.cod_custom_field,
	core.dat_snap,
	core.cod_source_system,
	core.custom_field_value
from
	(
		select
			cod_contact,
			cod_contact_parent,
			cod_custom_field,
			dat_snap,
			cod_source_system,
			cast(round(nvl(revenue_value,0),2) as varchar) custom_field_value
		from
			(
				select
					rev_cars.cod_contact,
					rev_cars.cod_contact_parent,
					kpi_custom_field.cod_custom_field,
					rev_cars.dat_snap,
					rev_cars.cod_source_system,
					revenue_value
				from
					(
						select
						  lkp_contact.cod_contact,
						  lkp_contact.cod_contact_parent,
						  lkp_contact.cod_source_system,
						  scai.dat_processing dat_snap,
						  sum(tmp_revenue.subs_value) revenue_value
						from
						  (select * from tmp_pl_otomoto_calc_revenue_listings where cod_index_type = 2 and period = -1) tmp_revenue,  
						  crm_integration_anlt.t_lkp_atlas_user lkp_atlas_user,
						  crm_integration_anlt.t_lkp_contact lkp_contact,
						  crm_integration_anlt.t_rel_scai_country_integration scai
						where
						  lkp_contact.cod_atlas_user = lkp_atlas_user.cod_atlas_user (+)
						  and lkp_atlas_user.opr_atlas_user = tmp_revenue.user_id (+)
						  and lkp_contact.valid_to = 20991231
						  and lkp_contact.cod_source_system = 12
		                  and lkp_atlas_user.valid_to (+) = 20991231
						  and lkp_atlas_user.cod_source_system (+) = 7
						  and scai.cod_integration = 50000
						  and scai.cod_country = 2
						group by
						  lkp_contact.cod_contact,
						  lkp_contact.cod_contact_parent,
						  lkp_contact.cod_source_system,
						  scai.dat_processing
					) rev_cars,
					(
						select
							rel.cod_custom_field,
							rel.flg_active
						from
							crm_integration_anlt.t_lkp_kpi kpi,
							crm_integration_anlt.t_rel_kpi_custom_field rel
						where
							kpi.cod_kpi = rel.cod_kpi
							and lower(kpi.dsc_kpi) = 'revenue (0) - listings'
							and rel.cod_source_system = 12
					) kpi_custom_field
				where
					kpi_custom_field.flg_active = 1
			) core
	) core,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where
	core.cod_source_system = fac_snap.cod_source_system (+)
	and core.cod_custom_field = fac_snap.cod_custom_field (+)
	and core.cod_contact = fac_snap.cod_contact (+)
	and (core.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);



--Calculate for employees
create temp table tmp_pl_otomoto_calc_revenue_listings_0_listings_2 as
select source.cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	source.custom_field_value
from tmp_pl_otomoto_calc_revenue_listings_0_listings_1 source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where 1 = 1
  and source.cod_contact_parent is not null
  and source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);



--Calculate for companies and contacts not associated with companies
create temp table tmp_pl_otomoto_calc_revenue_listings_0_listings_3 as
/*select nvl(source.cod_contact_parent, source.cod_contact) as cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system
	,cast(sum(CAST(source.custom_field_value AS NUMERIC(15,2))) as varchar) custom_field_value
from tmp_pl_otomoto_calc_revenue_listings_0_listings_1 source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where 1 = 1
  and source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and nvl(source.cod_contact_parent, source.cod_contact) = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null)
  group by
  source.cod_custom_field,
  source.dat_snap,
  source.cod_source_system,
	nvl(source.cod_contact_parent,source.cod_contact)
	 ;*/
select
 coalesce(core.cod_contact_parent,core.cod_contact) cod_contact,
 core.cod_custom_field,
 core.dat_snap,
 core.cod_source_system,
 cast(round(sum(core.revenue),2) as varchar) custom_field_value
from
 (
   select
     *,
     sum(cast(custom_field_value as numeric(15,2))) over (partition by cod_atlas_user) / nbr_atlas_users revenue,
     row_number() over (partition by coalesce(cod_contact_parent,cod_contact), cod_atlas_user order by cod_contact) rn
   from
     (
       select
         a.*,
         b.cod_atlas_user,
         count(*) over (partition by cod_atlas_user) nbr_atlas_users
       from
         tmp_pl_otomoto_calc_revenue_listings_0_listings_1 a,
         crm_integration_anlt.t_lkp_contact b
       where 1
         and a.cod_contact = b.cod_contact
         and a.cod_source_system = b.cod_source_system
         and b.valid_to = 20991231
     ) inner_core
 ) core,
 crm_integration_anlt.t_fac_base_integration_snap fac_snap
where
  rn = 1
  and core.cod_source_system = fac_snap.cod_source_system (+)
  and core.cod_custom_field = fac_snap.cod_custom_field (+)
  and nvl(core.cod_contact_parent, core.cod_contact) = fac_snap.cod_contact (+)
  and (core.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null)
group by
 coalesce(core.cod_contact_parent,core.cod_contact),
 core.cod_custom_field,
 core.dat_snap,
 core.cod_source_system;



-- HST INSERT - KPI OLX.BASE.099 (Revenue (0) - Listings)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in
			(select cod_contact, cod_custom_field from tmp_pl_otomoto_calc_revenue_listings_0_listings_2
			union
			select cod_contact, cod_custom_field from tmp_pl_otomoto_calc_revenue_listings_0_listings_3);


-- SNAP DELETE - KPI OLX.BASE.099 (Revenue (0) - Listings)
delete from crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in
			(select cod_contact, cod_custom_field from tmp_pl_otomoto_calc_revenue_listings_0_listings_2
			union
			select cod_contact, cod_custom_field from tmp_pl_otomoto_calc_revenue_listings_0_listings_3);


--KPI OLX.BASE.099 (Revenue (0) - Listings)
insert into crm_integration_anlt.t_fac_base_integration_snap
	select
		*
	from
		(select * from tmp_pl_otomoto_calc_revenue_listings_0_listings_2
		union
		select * from tmp_pl_otomoto_calc_revenue_listings_0_listings_3);





-- CREATE TMP - KPI OLX.BASE.103 (Revenue (-1) - Listings)
create temp table tmp_pl_otomoto_calc_revenue_listings_1_listings_1 as
select
	core.cod_contact,
	core.cod_contact_parent,
	core.cod_custom_field,
	core.dat_snap,
	core.cod_source_system,
	core.custom_field_value
from
	(
		select
			cod_contact,
			cod_contact_parent,
			cod_custom_field,
			dat_snap,
			cod_source_system,
			cast(round(nvl(revenue_value,0),2) as varchar) custom_field_value
		from
			(
				select
					rev_cars.cod_contact,
					rev_cars.cod_contact_parent,
					kpi_custom_field.cod_custom_field,
					rev_cars.dat_snap,
					rev_cars.cod_source_system,
					revenue_value
				from
					(
						select
						  lkp_contact.cod_contact,
						  lkp_contact.cod_contact_parent,
						  lkp_contact.cod_source_system,
						  scai.dat_processing dat_snap,
						  sum(tmp_revenue.subs_value) revenue_value
						from
						  (select * from tmp_pl_otomoto_calc_revenue_listings where cod_index_type = 2 and period = -2) tmp_revenue, -- Listings, Month -1
						  crm_integration_anlt.t_lkp_atlas_user lkp_atlas_user,
						  crm_integration_anlt.t_lkp_contact lkp_contact,
						  crm_integration_anlt.t_rel_scai_country_integration scai
						where
						  lkp_contact.cod_atlas_user = lkp_atlas_user.cod_atlas_user (+)
						  and lkp_atlas_user.opr_atlas_user = tmp_revenue.user_id (+)
						  and lkp_contact.valid_to = 20991231
						  and lkp_contact.cod_source_system = 12
		                  and lkp_atlas_user.valid_to (+) = 20991231
						  and lkp_atlas_user.cod_source_system (+) = 7
						  and scai.cod_integration = 50000
						  and scai.cod_country = 2
						group by
						  lkp_contact.cod_contact,
						  lkp_contact.cod_contact_parent,
						  lkp_contact.cod_source_system,
						  scai.dat_processing
					) rev_cars,
					(
						select
							rel.cod_custom_field,
							rel.flg_active
						from
							crm_integration_anlt.t_lkp_kpi kpi,
							crm_integration_anlt.t_rel_kpi_custom_field rel
						where
							kpi.cod_kpi = rel.cod_kpi
							and lower(kpi.dsc_kpi) = 'revenue (-1) - listings'
							and rel.cod_source_system = 12
					) kpi_custom_field
				where
					kpi_custom_field.flg_active = 1
			) core
	) core,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where
	core.cod_source_system = fac_snap.cod_source_system (+)
	and core.cod_custom_field = fac_snap.cod_custom_field (+)
	and core.cod_contact = fac_snap.cod_contact (+)
	and (core.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);



--Calculate for employees
create temp table tmp_pl_otomoto_calc_revenue_listings_1_listings_2 as
select source.cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	source.custom_field_value
from tmp_pl_otomoto_calc_revenue_listings_1_listings_1 source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where 1 = 1
  and source.cod_contact_parent is not null
  and source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);



--Calculate for companies and contacts not associated with companies
create temp table tmp_pl_otomoto_calc_revenue_listings_1_listings_3 as
/*select nvl(source.cod_contact_parent, source.cod_contact) as cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system
	,cast(sum(CAST(source.custom_field_value AS NUMERIC(15,2))) as varchar) custom_field_value
from tmp_pl_otomoto_calc_revenue_listings_1_listings_1 source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where 1 = 1
  and source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and nvl(source.cod_contact_parent, source.cod_contact) = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null)
  group by
  source.cod_custom_field,
  source.dat_snap,
  source.cod_source_system,
	nvl(source.cod_contact_parent,source.cod_contact)
	 ;*/
select
 coalesce(core.cod_contact_parent,core.cod_contact) cod_contact,
 core.cod_custom_field,
 core.dat_snap,
 core.cod_source_system,
 cast(round(sum(core.revenue),2) as varchar) custom_field_value
from
 (
   select
     *,
     sum(cast(custom_field_value as numeric(15,2))) over (partition by cod_atlas_user) / nbr_atlas_users revenue,
     row_number() over (partition by coalesce(cod_contact_parent,cod_contact), cod_atlas_user order by cod_contact) rn
   from
     (
       select
         a.*,
         b.cod_atlas_user,
         count(*) over (partition by cod_atlas_user) nbr_atlas_users
       from
         tmp_pl_otomoto_calc_revenue_listings_1_listings_1 a,
         crm_integration_anlt.t_lkp_contact b
       where 1
         and a.cod_contact = b.cod_contact
         and a.cod_source_system = b.cod_source_system
         and b.valid_to = 20991231
     ) inner_core
 ) core,
 crm_integration_anlt.t_fac_base_integration_snap fac_snap
where
  rn = 1
  and core.cod_source_system = fac_snap.cod_source_system (+)
  and core.cod_custom_field = fac_snap.cod_custom_field (+)
  and nvl(core.cod_contact_parent, core.cod_contact) = fac_snap.cod_contact (+)
  and (core.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null)
group by
 coalesce(core.cod_contact_parent,core.cod_contact),
 core.cod_custom_field,
 core.dat_snap,
 core.cod_source_system;



-- HST INSERT - KPI OLX.BASE.099 (Revenue (-1) - Listings)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in
			(select cod_contact, cod_custom_field from tmp_pl_otomoto_calc_revenue_listings_1_listings_2
			union
			select cod_contact, cod_custom_field from tmp_pl_otomoto_calc_revenue_listings_1_listings_3);



-- SNAP DELETE - KPI OLX.BASE.099 (Revenue (-1) - Listings)
delete from crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in
			(select cod_contact, cod_custom_field from tmp_pl_otomoto_calc_revenue_listings_1_listings_2
			union
			select cod_contact, cod_custom_field from tmp_pl_otomoto_calc_revenue_listings_1_listings_3);



--KPI OLX.BASE.099 (Revenue (-1) - Listings)
insert into crm_integration_anlt.t_fac_base_integration_snap
	select
		*
	from
		(select * from tmp_pl_otomoto_calc_revenue_listings_1_listings_2
		union
		select * from tmp_pl_otomoto_calc_revenue_listings_1_listings_3);

 
 
 
 
 

-- CREATE TMP - KPI OLX.BASE.XYZ (Max Value Package)
create temp table tmp_pl_otomoto_calc_max_value_package_1 as
select
	core.cod_contact,
	core.cod_contact_parent,
	core.cod_custom_field,
	core.dat_snap,
	core.cod_source_system,
	core.custom_field_value
from
	(
		select
			cod_contact,
			cod_contact_parent,
			cod_custom_field,
			dat_snap,
			cod_source_system,
			cast(round(nvl(max_invoice_value,0),2) as varchar) custom_field_value
		from
			(
				select
					rev_cars.cod_contact,
					rev_cars.cod_contact_parent,
					kpi_custom_field.cod_custom_field,
					rev_cars.dat_snap,
					rev_cars.cod_source_system,
					max_invoice_value
				from
					(
            select
              cod_contact,
              cod_contact_parent,
              cod_source_system,
              dat_snap,
              max(invoice_value) max_invoice_value
            from
              (
                select
                  lkp_contact.cod_contact,
                  lkp_contact.cod_contact_parent,
                  lkp_contact.cod_source_system,
                  scai.dat_processing dat_snap,
                  tmp_revenue.period,
                  sum(tmp_revenue.subs_value) invoice_value
                from
                  (select * from tmp_pl_otomoto_calc_revenue_listings where cod_index_type in (1,2) and period in (-2,-3,-4)) tmp_revenue, -- VAS and Listings, last 3 invoices
                  crm_integration_anlt.t_lkp_atlas_user lkp_atlas_user,
                  crm_integration_anlt.t_lkp_contact lkp_contact,
                  crm_integration_anlt.t_rel_scai_country_integration scai
                where
                  lkp_contact.cod_atlas_user = lkp_atlas_user.cod_atlas_user (+)
                  and lkp_atlas_user.opr_atlas_user = tmp_revenue.user_id (+)
                  and lkp_contact.valid_to = 20991231
                  and lkp_contact.cod_source_system = 12
                  and lkp_atlas_user.valid_to (+) = 20991231
                  and lkp_atlas_user.cod_source_system (+) = 7
                  and scai.cod_integration = 50000
                  and scai.cod_country = 2
                group by
                  lkp_contact.cod_contact,
                  lkp_contact.cod_contact_parent,
                  lkp_contact.cod_source_system,
                  scai.dat_processing,
                  tmp_revenue.period
              ) inner_core
            group by
              cod_contact,
              cod_contact_parent,
              cod_source_system,
              dat_snap
					) rev_cars,
					(
						select
							rel.cod_custom_field,
							rel.flg_active
						from
							crm_integration_anlt.t_lkp_kpi kpi,
							crm_integration_anlt.t_rel_kpi_custom_field rel
						where
							kpi.cod_kpi = rel.cod_kpi
							and lower(kpi.dsc_kpi) = 'max package value last 3 months'
							and rel.cod_source_system = 12
					) kpi_custom_field
				where
					kpi_custom_field.flg_active = 1
			) core
	) core,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where
	core.cod_source_system = fac_snap.cod_source_system (+)
	and core.cod_custom_field = fac_snap.cod_custom_field (+)
	and core.cod_contact = fac_snap.cod_contact (+)
	and (core.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);


		  
--Calculate for employees
create temp table tmp_pl_otomoto_calc_max_value_package_2 as
select source.cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	cast(coalesce(cast(source.custom_field_value as varchar), '0') as varchar) custom_field_value
from tmp_pl_otomoto_calc_max_value_package_1 source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where 1 = 1
  and source.cod_contact_parent is not null
  and source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);



--Calculate for companies and contacts not associated with companies
create temp table tmp_pl_otomoto_calc_max_value_package_3 as
select nvl(source.cod_contact_parent, source.cod_contact) as cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	coalesce(cast(max(cast(source.custom_field_value as numeric(15,2) )) as varchar), '0') custom_field_value
from tmp_pl_otomoto_calc_max_value_package_1 source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where 1 = 1
  and source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and nvl(source.cod_contact_parent, source.cod_contact) = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null)
  group by
  source.cod_custom_field,
  source.dat_snap,
  source.cod_source_system,
	nvl(source.cod_contact_parent,source.cod_contact)
	 ;		  


	 
-- HST INSERT - KPI OLX.BASE.XYZ (Max Value Package)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in
			(select cod_contact, cod_custom_field from tmp_pl_otomoto_calc_max_value_package_2
			union
			select cod_contact, cod_custom_field from tmp_pl_otomoto_calc_max_value_package_3)
			;



-- SNAP DELETE - KPI OLX.BASE.XYZ (Max Value Package)
delete from crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in
			(select cod_contact, cod_custom_field from tmp_pl_otomoto_calc_max_value_package_2
			union
			select cod_contact, cod_custom_field from tmp_pl_otomoto_calc_max_value_package_3)
			;



--KPI OLX.BASE.XYZ (Max Value Package)
insert into crm_integration_anlt.t_fac_base_integration_snap
  select
    *
  from
    (select * from tmp_pl_otomoto_calc_max_value_package_2
	union
	select * from tmp_pl_otomoto_calc_max_value_package_3);


	
	
	

-- CREATE TMP - KPI OLX.BASE.121 (Maximum package purchased)
create temp table tmp_pl_otomoto_calc_max_package_1 as
select
	core.cod_contact,
	core.cod_contact_parent,
	core.cod_custom_field,
	core.dat_snap,
	core.cod_source_system,
	core.custom_field_value
from
	(
		select
			cod_contact,
			cod_contact_parent,
			cod_custom_field,
			dat_snap,
			cod_source_system,
			package_name || ', ' || package_value as custom_field_value
		from
			(
				select
					inner_core.cod_contact,
					inner_core.cod_contact_parent,
					kpi_custom_field.cod_custom_field,
					inner_core.dat_snap,
					inner_core.cod_source_system,
					package_name,
					cast(round(nvl(package_value,0),2) as varchar) package_value
				from
              (
                select
                  lkp_contact.cod_contact,
                  lkp_contact.cod_contact_parent,
                  lkp_contact.cod_source_system,
                  scai.dat_processing dat_snap,
                  idx.name_pl as package_name,
                  max(tmp_revenue.subs_value) package_value
                from
                  (select * from tmp_pl_otomoto_calc_revenue_listings where cod_index_type in (1,2) and period in (-2,-3,-4)) tmp_revenue, -- VAS and Listings, last 3 invoices
                  crm_integration_anlt.t_lkp_atlas_user lkp_atlas_user,
                  crm_integration_anlt.t_lkp_contact lkp_contact,
                  crm_integration_anlt.t_rel_scai_country_integration scai,
				  db_atlas_verticals.paidads_indexes idx
                where
                  lkp_contact.cod_atlas_user = lkp_atlas_user.cod_atlas_user (+)
                  and lkp_atlas_user.opr_atlas_user = tmp_revenue.user_id (+)
                  and lkp_contact.valid_to = 20991231
                  and lkp_contact.cod_source_system = 12
                  and lkp_atlas_user.valid_to (+) = 20991231
                  and lkp_atlas_user.cod_source_system (+) = 7
                  and scai.cod_integration = 50000
                  and scai.cod_country = 2
				  and idx.livesync_dbname = 'otomotopl'
				  and tmp_revenue.id_index = idx.id
                group by
                  lkp_contact.cod_contact,
                  lkp_contact.cod_contact_parent,
                  lkp_contact.cod_source_system,
                  scai.dat_processing,
                  idx.name_pl
              ) inner_core ,
					(
						select
							rel.cod_custom_field,
							rel.flg_active
						from
							crm_integration_anlt.t_lkp_kpi kpi,
							crm_integration_anlt.t_rel_kpi_custom_field rel
						where
							kpi.cod_kpi = rel.cod_kpi
							and lower(kpi.dsc_kpi) = 'maximum package purchased'
							and rel.cod_source_system = 12
					) kpi_custom_field
				where
					kpi_custom_field.flg_active = 1
			) core
	) core,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where
	core.cod_source_system = fac_snap.cod_source_system (+)
	and core.cod_custom_field = fac_snap.cod_custom_field (+)
	and core.cod_contact = fac_snap.cod_contact (+)
	and (core.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);


		  
--Calculate for employees
create temp table tmp_pl_otomoto_calc_max_package_2 as
select source.cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	cast(coalesce(cast(source.custom_field_value as varchar), '0') as varchar) custom_field_value
from tmp_pl_otomoto_calc_max_package_1 source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where 1 = 1
  and source.cod_contact_parent is not null
  and source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);



--Calculate for companies and contacts not associated with companies
create temp table tmp_pl_otomoto_calc_max_package_3 as
select nvl(source.cod_contact_parent, source.cod_contact) as cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	coalesce(cast(max(cast(source.custom_field_value as numeric(15,2) )) as varchar), '0') custom_field_value
from tmp_pl_otomoto_calc_max_package_1 source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where 1 = 1
  and source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and nvl(source.cod_contact_parent, source.cod_contact) = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null)
  group by
  source.cod_custom_field,
  source.dat_snap,
  source.cod_source_system,
	nvl(source.cod_contact_parent,source.cod_contact)
	 ;		  


	 
-- HST INSERT - KPI OLX.BASE.121 (Maximum package purchased)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in
			(select cod_contact, cod_custom_field from tmp_pl_otomoto_calc_max_package_2
			union
			select cod_contact, cod_custom_field from tmp_pl_otomoto_calc_max_package_3)
			;



-- SNAP DELETE - KPI OLX.BASE.121 (Maximum package purchased)
delete from crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in
			(select cod_contact, cod_custom_field from tmp_pl_otomoto_calc_max_package_2
			union
			select cod_contact, cod_custom_field from tmp_pl_otomoto_calc_max_package_3)
			;



--KPI KPI OLX.BASE.121 (Maximum package purchased)
insert into crm_integration_anlt.t_fac_base_integration_snap
  select
    *
  from
    (select * from tmp_pl_otomoto_calc_max_package_2
	union
	select * from tmp_pl_otomoto_calc_max_package_3);
	


--$$$

-- CREATE TMP - KPI OLX.BASE.105 (User_ID)
create temp table tmp_pl_otomoto_calc_user_id as
select
  source.cod_contact,
  source.cod_custom_field,
  source.dat_snap,
  source.cod_source_system,
  source.custom_field_value
from
	(
	SELECT
	  b.cod_contact,
	  kpi_custom_field.cod_custom_field,
	  scai.dat_processing dat_snap,
	  isnull(a.cod_source_system,12) cod_source_system,
	  isnull(a.custom_field_value, ' ') custom_field_value
	FROM
	  (
		SELECT
		base_contact.cod_contact,
		scai.dat_processing dat_snap,
		base_contact.cod_source_system,
		cast(atlas_user.opr_atlas_user as varchar) custom_field_value
	  FROM
		crm_integration_anlt.t_lkp_atlas_user atlas_user,
		crm_integration_anlt.t_lkp_contact base_contact,
		crm_integration_anlt.t_rel_scai_country_integration scai
	  WHERE
		atlas_user.cod_source_system = 7
		AND base_contact.cod_source_system = 12
		AND lower(base_contact.email) = lower(atlas_user.dsc_atlas_user)
		AND atlas_user.valid_to = 20991231
		AND base_contact.valid_to = 20991231
		AND scai.cod_integration = 50000
		and scai.cod_country = 2
	  ) A,
		crm_integration_anlt.t_lkp_contact B,
		crm_integration_anlt.t_rel_scai_country_integration scai,
		(
			select
			  rel.cod_custom_field,
			  rel.flg_active
			from
			  crm_integration_anlt.t_lkp_kpi kpi,
			  crm_integration_anlt.t_rel_kpi_custom_field rel
			where
			  kpi.cod_kpi = rel.cod_kpi
			  and lower(kpi.dsc_kpi) = 'user_id'
			  and rel.cod_source_system = 12
		) kpi_custom_field
	WHERE
	  B.cod_contact = A.cod_contact (+)
	  and b.valid_to = 20991231
	  and b.cod_source_system = 12
	  and scai.cod_integration = 50000
	  and kpi_custom_field.flg_active = 1
		and scai.cod_country = 2
	) source,
		crm_integration_anlt.t_fac_base_integration_snap fac_snap
where source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);



-- HST INSERT - KPI OLX.BASE.105 (User_ID)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from tmp_pl_otomoto_calc_user_id);



-- SNAP DELETE - KPI OLX.BASE.105 (User_ID)
DELETE FROM crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from tmp_pl_otomoto_calc_user_id);



--KPI OLX.BASE.105 (User_ID)
insert into crm_integration_anlt.t_fac_base_integration_snap
	select
		*
	from
		tmp_pl_otomoto_calc_user_id;



--$$$

--(# Ads consumed) (NOT IN OTOMOTO)
insert into crm_integration_anlt.t_fac_base_integration_snap (
select source.* from (
  SELECT
    base_contact.cod_contact,
    kpi_custom_field.cod_custom_field,
    scai.dat_processing dat_snap,
    base_contact.cod_source_system,
    ' ' as custom_field_value
  FROM
    crm_integration_anlt.t_lkp_contact base_contact,
    crm_integration_anlt.t_rel_scai_country_integration scai,
	(
		select
		  rel.cod_custom_field,
		  rel.flg_active
		from
		  crm_integration_anlt.t_lkp_kpi kpi,
		  crm_integration_anlt.t_rel_kpi_custom_field rel
		where
		  kpi.cod_kpi = rel.cod_kpi
		  and lower(kpi.dsc_kpi) = '# ads consumed'
		  and rel.cod_source_system = 12
	) kpi_custom_field
  WHERE
    base_contact.cod_source_system = 12
    AND base_contact.valid_to = 20991231
    AND scai.cod_integration = 50000
		and scai.cod_country = 2
	and kpi_custom_field.flg_active = 1
    --and atlas_user.valid_from = scai.dat_processing
) source,
    crm_integration_anlt.t_fac_base_integration_snap fac_snap
where source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and source.custom_field_value = fac_snap.custom_field_value (+)
  and fac_snap.cod_contact is null
);

--$$$

-- CREATE TMP - KPI OLX.BASE.087 ( Chat response % )
create temp table tmp_otomotopl_calc_chat_response as
select
  source.cod_contact,
  source.cod_custom_field,
  source.dat_snap,
  source.cod_source_system,
  source.custom_field_value
from
  (
    select
      b.cod_contact,
      kpi_custom_field.cod_custom_field,
      scai.dat_processing dat_snap,
      isnull(a.cod_source_system,12) cod_source_system,
      isnull(a.custom_field_value, '0') custom_field_value
    from
      (
        select
          source.cod_contact,
          source.dat_processing dat_snap,
          source.cod_source_system,
          cast(case when repliestotal>0 then replies48*100/repliestotal else 0 end as varchar) custom_field_value
        from
          (
            select
              lkp_contact.cod_contact,
              scai.dat_processing,
              lkp_contact.cod_source_system,
              sum(reply24) + sum(reply48) replies48,
							sum(reply24) + sum(reply48) + sum(replylonger48) repliestotal
            from
              crm_integration_anlt.v_agg_verticals_answer answer,
              crm_integration_anlt.t_lkp_atlas_user lkp_user,
              crm_integration_anlt.t_lkp_contact lkp_contact,
              crm_integration_anlt.t_rel_scai_country_integration scai
            where
              lkp_user.cod_source_system = 7
              and lkp_contact.cod_source_system = 12
			  and answer.livesync_dbname = 'otomoto'
              and answer.seller_id = lkp_user.opr_atlas_user
              and lkp_user.valid_to = 20991231
              and lower(lkp_contact.email) = lower(lkp_user.dsc_atlas_user)
              and lkp_contact.valid_to = 20991231
              and scai.cod_integration = 50000
			  and scai.cod_country = 2
            group by
              lkp_contact.cod_contact,
              scai.dat_processing,
              lkp_contact.cod_source_system
          ) source
      ) a,
        crm_integration_anlt.t_lkp_contact b,
        crm_integration_anlt.t_rel_scai_country_integration scai,
		(
			select
			  rel.cod_custom_field,
			  rel.flg_active
			from
			  crm_integration_anlt.t_lkp_kpi kpi,
			  crm_integration_anlt.t_rel_kpi_custom_field rel
			where
			  kpi.cod_kpi = rel.cod_kpi
			  and lower(kpi.dsc_kpi) = 'chat response %'
			  and rel.cod_source_system = 12
		) kpi_custom_field
    where
      b.cod_contact = a.cod_contact (+)
      and b.valid_to = 20991231
      and b.cod_source_system = 12
      and scai.cod_integration = 50000
	  and kpi_custom_field.flg_active = 1
	  and scai.cod_country = 2
  ) source,
  crm_integration_anlt.t_fac_base_integration_snap fac_snap
where
  source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);



-- HST INSERT - KPI OLX.BASE.087 ( Chat response % )
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from tmp_otomotopl_calc_chat_response);



-- SNAP DELETE - KPI OLX.BASE.087 ( Chat response % )
DELETE FROM crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from tmp_otomotopl_calc_chat_response);



--KPI OLX.BASE.087 ( Chat response % )
insert into crm_integration_anlt.t_fac_base_integration_snap
	select
		*
	from
		tmp_otomotopl_calc_chat_response;

--$$$

--(Scheduled package) (NOT IN T_REL_KPI_CUSTOM_FIELD YET)
insert into crm_integration_anlt.t_fac_base_integration_snap (
select source.* from (
  SELECT
    base_contact.cod_contact,
    kpi_custom_field.cod_custom_field,
    scai.dat_processing dat_snap,
    base_contact.cod_source_system,
    ' ' as custom_field_value
  FROM
    crm_integration_anlt.t_lkp_contact base_contact,
    crm_integration_anlt.t_rel_scai_country_integration scai,
	(
		select
		  rel.cod_custom_field,
		  rel.flg_active
		from
		  crm_integration_anlt.t_lkp_kpi kpi,
		  crm_integration_anlt.t_rel_kpi_custom_field rel
		where
		  kpi.cod_kpi = rel.cod_kpi
		  and lower(kpi.dsc_kpi) = 'scheduled package'
		  and rel.cod_source_system = 12
	) kpi_custom_field
  WHERE
    base_contact.cod_source_system = 12
    AND base_contact.valid_to = 20991231
    AND scai.cod_integration = 50000
		and kpi_custom_field.flg_active = 1
		and scai.cod_country = 2
) source,
    crm_integration_anlt.t_fac_base_integration_snap fac_snap
where source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and source.custom_field_value = fac_snap.custom_field_value (+)
  and fac_snap.cod_contact is null
);

--$$$

--(AMI License) (NOT IN OTOMOTO)
insert into crm_integration_anlt.t_fac_base_integration_snap (
select source.* from (
  SELECT
    base_contact.cod_contact,
    kpi_custom_field.cod_custom_field,
    scai.dat_processing dat_snap,
    base_contact.cod_source_system,
    ' ' as custom_field_value
  FROM
    crm_integration_anlt.t_lkp_contact base_contact,
    crm_integration_anlt.t_rel_scai_country_integration scai,
	(
		select
		  rel.cod_custom_field,
		  rel.flg_active
		from
		  crm_integration_anlt.t_lkp_kpi kpi,
		  crm_integration_anlt.t_rel_kpi_custom_field rel
		where
		  kpi.cod_kpi = rel.cod_kpi
		  and lower(kpi.dsc_kpi) = 'ami license'
		  and rel.cod_source_system = 12
	) kpi_custom_field
  WHERE
    base_contact.cod_source_system = 12
    AND base_contact.valid_to = 20991231
    AND scai.cod_integration = 50000
		and kpi_custom_field.flg_active = 1
		and scai.cod_country = 2
) source,
    crm_integration_anlt.t_fac_base_integration_snap fac_snap
where source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and source.custom_field_value = fac_snap.custom_field_value (+)
  and fac_snap.cod_contact is null
);

--$$$

--(NIF) (NOT IN T_REL_KPI_CUSTOM_FIELD YET)
insert into crm_integration_anlt.t_fac_base_integration_snap (
select source.* from (
  SELECT
    base_contact.cod_contact,
    kpi_custom_field.cod_custom_field,
    scai.dat_processing dat_snap,
    base_contact.cod_source_system,
    ' ' as custom_field_value
  FROM
    crm_integration_anlt.t_lkp_contact base_contact,
    crm_integration_anlt.t_rel_scai_country_integration scai,
	(
		select
		  rel.cod_custom_field,
		  rel.flg_active
		from
		  crm_integration_anlt.t_lkp_kpi kpi,
		  crm_integration_anlt.t_rel_kpi_custom_field rel
		where
		  kpi.cod_kpi = rel.cod_kpi
		  and lower(kpi.dsc_kpi) = 'nif'
		  and rel.cod_source_system = 12
	) kpi_custom_field
  WHERE
    base_contact.cod_source_system = 12
    AND base_contact.valid_to = 20991231
    AND scai.cod_integration = 50000
	and kpi_custom_field.flg_active = 1
    and scai.cod_country = 2
) source,
    crm_integration_anlt.t_rel_contact_custom_field rel
where source.cod_source_system = rel.cod_source_system (+)
  and source.cod_custom_field = rel.cod_custom_field (+)
  and source.cod_contact = rel.cod_contact (+)
  and rel.cod_contact is null);

--$$$

-- #######################
-- ####    PASSO 5    ####
-- #######################
insert into crm_integration_anlt.t_fac_scai_execution
  select
    max_cod_exec + 1 cod_execution,
	rel_integr_proc.cod_country,
    rel_integr_proc.cod_integration,
    rel_integr_proc.cod_process,
    1 cod_status,
    2 cod_execution_type, -- End
    rel_integr_proc.dat_processing,
    rel_integr_proc.execution_nbr,
    sysdate
  from
    crm_integration_anlt.t_rel_scai_country_integration rel_country_integr,
    (select isnull(max(cod_execution),0) max_cod_exec from crm_integration_anlt.t_fac_scai_execution),
    crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
    crm_integration_anlt.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 50000 -- Chandra (Analytical) to Base
    and rel_country_integr.cod_country = 2 -- Poland
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_fac_base_integration_snap_plcars';

--$$$
	
-- #######################
-- ####    PASSO 6    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set cod_status = 1, -- Ok
last_processing_datetime = sysdate
from crm_integration_anlt.t_lkp_scai_process proc
where t_rel_scai_integration_process.cod_process = proc.cod_process
and t_rel_scai_integration_process.cod_status = 2
and t_rel_scai_integration_process.cod_country = 2
and proc.dsc_process_short = 't_fac_base_integration_snap_plcars'
and t_rel_scai_integration_process.ind_active = 1;

--$$$

delete from crm_integration_anlt.t_fac_base_integration_snap
where cod_source_system = 12
and cod_contact not in (127199,
143475,
197881,
252603,
262396,
263153,
273867,
288971,
291421,
304078,
304401,
304750,
305106,
305580,
313990,
314986,
316241,
317901,
320142,
322154,
326059,
922418,
1057558,
1159836,
1170573,
1187167,
1187250,
1196461,
1196795,
1425157,
114581,
127199,
131795,
137309,
138340,
141769,
142882,
143475,
158830,
197881,
199108,
199778,
216662,
231301,
231570,
232460,
235122,
246484,
248750,
250399,
250802,
251381,
252603,
254977,
259007,
262203,
262331,
262396,
263153,
264462,
264526,
268443,
269884,
271537,
272571,
273867,
282152,
282377,
282576,
286367,
288323,
288971,
290048,
290173,
290183,
291421,
292746,
292804,
293246,
293645,
303955,
304078,
304401,
304501,
304562,
304614,
304750,
304931,
304933,
304970,
305250,
305580,
306302,
306489,
307069,
307736,
308745,
309379,
309537,
309838
);