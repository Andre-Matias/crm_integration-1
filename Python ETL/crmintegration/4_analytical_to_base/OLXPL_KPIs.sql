-- #######################
-- ####    PASSO 3    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set dat_processing = source.dat_processing, execution_nbr = source.execution_nbr, cod_status = 2 -- Running
from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc, crm_integration_anlt.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_fac_base_integration_snap_plhorizontal'
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
	and proc.dsc_process_short = 't_fac_base_integration_snap_plhorizontal';

--$$$

--(--------REVENUE--------)
insert into crm_integration_anlt.t_fac_base_integration_snap (
select source.* from (
  select
    base_contact.cod_contact,
    7094 cod_custom_field,
    scai.dat_processing dat_snap,
    base_contact.cod_source_system,
    ' ' as custom_field_value
  from
    crm_integration_anlt.t_lkp_contact base_contact,
    crm_integration_anlt.t_rel_scai_country_integration scai
  where
    base_contact.cod_source_system = 13
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
    7089 cod_custom_field,
    scai.dat_processing dat_snap,
    base_contact.cod_source_system,
    ' ' as custom_field_value
  FROM
    crm_integration_anlt.t_lkp_contact base_contact,
    crm_integration_anlt.t_rel_scai_country_integration scai
  WHERE
    base_contact.cod_source_system = 13
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
    7090 cod_custom_field,
    scai.dat_processing dat_snap,
    base_contact.cod_source_system,
    ' ' as custom_field_value
  FROM
    crm_integration_anlt.t_lkp_contact base_contact,
    crm_integration_anlt.t_rel_scai_country_integration scai
  WHERE
    base_contact.cod_source_system = 13
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
    7093 cod_custom_field,
    scai.dat_processing dat_snap,
    base_contact.cod_source_system,
    ' ' as custom_field_value
  FROM
    crm_integration_anlt.t_lkp_contact base_contact,
    crm_integration_anlt.t_rel_scai_country_integration scai
  WHERE
    base_contact.cod_source_system = 13
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
    7091 cod_custom_field,
    scai.dat_processing dat_snap,
    base_contact.cod_source_system,
    ' ' as custom_field_value
  FROM
    crm_integration_anlt.t_lkp_contact base_contact,
    crm_integration_anlt.t_rel_scai_country_integration scai
  WHERE
    base_contact.cod_source_system = 13
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

--(------NEW FIELDS-------)
insert into crm_integration_anlt.t_fac_base_integration_snap (
select source.* from (
  SELECT
    base_contact.cod_contact,
    7095 cod_custom_field,
    scai.dat_processing dat_snap,
    base_contact.cod_source_system,
    ' ' as custom_field_value
  FROM
    crm_integration_anlt.t_lkp_contact base_contact,
    crm_integration_anlt.t_rel_scai_country_integration scai
  WHERE
    base_contact.cod_source_system = 13
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

--(-----OLD FIELDS-----)
insert into crm_integration_anlt.t_fac_base_integration_snap (
select source.* from (
  SELECT
    base_contact.cod_contact,
    7092 cod_custom_field,
    scai.dat_processing dat_snap,
    base_contact.cod_source_system,
    ' ' as custom_field_value
  FROM
    crm_integration_anlt.t_lkp_contact base_contact,
    crm_integration_anlt.t_rel_scai_country_integration scai
  WHERE
    base_contact.cod_source_system = 13
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
create table crm_integration_anlt.tmp_pl_olx_calc_last_login_1 as
    select
		  a.cod_contact,
		  a.cod_contact_parent,
		  kpi_custom_field.cod_custom_field,
		  scai.dat_processing dat_snap,
		  isnull(a.cod_source_system,13) cod_source_system,
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
							atlas_user.cod_source_system = 9
							and base_contact.cod_source_system = 13
							and lower(base_contact.email) = lower(atlas_user.dsc_atlas_user)
							and atlas_user.valid_to = 20991231
							and base_contact.valid_to = 20991231
							and scai.cod_integration = 50000
							and scai.cod_country = 2
							--and base_contact.cod_contact_parent = 306798
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
				  and rel.cod_source_system = 13
			) kpi_custom_field
		where
		  1 = 1
		  and scai.cod_integration = 50000
		  and kpi_custom_field.flg_active = 1
		  and scai.cod_country = 2
	 ;

--$$$
	 
--Calculate for employees	 
create table crm_integration_anlt.tmp_pl_olx_calc_last_login_2 as
   select source.cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	source.custom_field_value
	from crm_integration_anlt.tmp_pl_olx_calc_last_login_1 source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where 1 = 1
  and source.cod_contact_parent is not null
  and source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);
	 ;	 
	 
--$$$
	 
--Calculate for companies and contacts not associated with companies	
create table crm_integration_anlt.tmp_pl_olx_calc_last_login_3 as
   select nvl(source.cod_contact_parent, source.cod_contact) as cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	max(source.custom_field_value) custom_field_value
	from crm_integration_anlt.tmp_pl_olx_calc_last_login_1 source,
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

--$$$

-- HST INSERT - KPI OLX.BASE.085 (Last login)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in 
			(select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_last_login_2
			union 
			select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_last_login_3);

--$$$

-- SNAP DELETE - KPI OLX.BASE.085 (Last login)
DELETE FROM crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in 
			(select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_last_login_2
			union 
			select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_last_login_3);

--$$$

--KPI OLX.BASE.085 (Last login)
insert into crm_integration_anlt.t_fac_base_integration_snap
	select
		*
	from
		(select * from crm_integration_anlt.tmp_pl_olx_calc_last_login_2
		union 
		select * from crm_integration_anlt.tmp_pl_olx_calc_last_login_3);

--$$$

drop table if exists crm_integration_anlt.tmp_pl_olx_calc_last_login_1;
drop table if exists crm_integration_anlt.tmp_pl_olx_calc_last_login_2;
drop table if exists crm_integration_anlt.tmp_pl_olx_calc_last_login_3;

--$$$

-- CREATE TMP - KPI OLX.BASE.031 (Created date)
create table crm_integration_anlt.tmp_pl_olx_calc_created_date_1 as
	select
		  a.cod_contact,
		  a.cod_contact_parent,
		  kpi_custom_field.cod_custom_field,
		  scai.dat_processing dat_snap,
		  isnull(a.cod_source_system,13) cod_source_system,
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
							atlas_user.cod_source_system = 9
							and base_contact.cod_source_system = 13
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
				  and rel.cod_source_system = 13
			) kpi_custom_field
		where
		  1 = 1
		  and scai.cod_integration = 50000
		  and kpi_custom_field.flg_active = 1
		  and scai.cod_country = 2
	;

--$$$
	
--Calculate for employees	
create table crm_integration_anlt.tmp_pl_olx_calc_created_date_2 as
   select source.cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	source.custom_field_value
	from crm_integration_anlt.tmp_pl_olx_calc_created_date_1 source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where 1 = 1
  and source.cod_contact_parent is not null
  and source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);	
  
--$$$
  
--Calculate for companies and contacts not associated with companies
create table crm_integration_anlt.tmp_pl_olx_calc_created_date_3 as
   select nvl(source.cod_contact_parent, source.cod_contact) as cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system
	,min(source.custom_field_value) custom_field_value
	from crm_integration_anlt.tmp_pl_olx_calc_created_date_1 source,
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

--$$$

-- HST INSERT - KPI OLX.BASE.031 (Created date)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in
			(select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_created_date_2
			union
			select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_created_date_3);

--$$$

-- SNAP DELETE - KPI OLX.BASE.031 (Created date)
DELETE FROM crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in
			(select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_created_date_2
			union
			select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_created_date_3);

--$$$

--KPI OLX.BASE.031 (Created date)
insert into crm_integration_anlt.t_fac_base_integration_snap
	select
		*
	from
		(select * from crm_integration_anlt.tmp_pl_olx_calc_created_date_2
		union
		select * from crm_integration_anlt.tmp_pl_olx_calc_created_date_3);

--$$$

drop table if exists crm_integration_anlt.tmp_pl_olx_calc_created_date_1;
drop table if exists crm_integration_anlt.tmp_pl_olx_calc_created_date_2;
drop table if exists crm_integration_anlt.tmp_pl_olx_calc_created_date_3;

--$$$

-- CREATE TMP - KPI OLX.BASE.112 (Account Status)
create table crm_integration_anlt.tmp_pl_olx_calc_account_status_1 as
	select
		  a.cod_contact,
		  a.cod_contact_parent,
		  kpi_custom_field.cod_custom_field,
		  scai.dat_processing dat_snap,
		  isnull(a.cod_source_system,13) cod_source_system,
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
							atlas_user.type custom_field_value,
							row_number() over (partition by cod_contact order by coalesce(atlas_user.created_at,'1900-01-01')) rn
						from
							crm_integration_anlt.t_lkp_atlas_user atlas_user,
							crm_integration_anlt.t_lkp_contact base_contact,
							crm_integration_anlt.t_rel_scai_country_integration scai
						where
							atlas_user.cod_source_system = 9
							and base_contact.cod_source_system = 13
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
				  and lower(kpi.dsc_kpi) = 'account status'
				  and rel.cod_source_system = 13
			) kpi_custom_field
		where
		  1 = 1
		  and scai.cod_integration = 50000
		  and kpi_custom_field.flg_active = 1
		  and scai.cod_country = 2
		  ;
	
--$$$
	
--Calculate for employees, companies and contacts not associated with companies
create table crm_integration_anlt.tmp_pl_olx_calc_account_status_2 as
   select source.cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	source.custom_field_value
	from crm_integration_anlt.tmp_pl_olx_calc_account_status_1 source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where 1 = 1
  and source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);		
 
--$$$	 

-- HST INSERT - KPI OLX.BASE.112 (Account Status)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_account_status_2);

--$$$

-- SNAP DELETE - KPI OLX.BASE.112 (Account Status)
DELETE FROM crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_account_status_2);

--$$$

--KPI OLX.BASE.112 (Account Status)
insert into crm_integration_anlt.t_fac_base_integration_snap
	select
		*
	from
		crm_integration_anlt.tmp_pl_olx_calc_account_status_2;

--$$$

drop table if exists crm_integration_anlt.tmp_pl_olx_calc_account_status_1;
drop table if exists crm_integration_anlt.tmp_pl_olx_calc_account_status_2;

--$$$

-- CREATE TMP - KPI OLX.BASE.086 (# Logins last 30 days)
create table crm_integration_anlt.tmp_pl_olx_calc_logins_last_30_days_1 as
	select
			a.cod_contact,
			a.cod_contact_parent,
			kpi_custom_field.cod_custom_field,
			scai.dat_processing dat_snap,
			isnull(a.cod_source_system,13) cod_source_system,
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
							base.cod_contact,
							base.cod_contact_parent,
							web.server_date_day,
							dat_snap,
							cod_source_system,
							count(*) nbr_platform_interactions
						from
							hydra.web web,
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
									atlas_user.cod_source_system = 9
									and base_contact.cod_source_system = 13
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
							and web.host like '%olx.pl%'
							and web.user_id = base.opr_atlas_user
							--and trackname like 'login%'
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
							hydra.ios ios,
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
									atlas_user.cod_source_system = 9
									and base_contact.cod_source_system = 13
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
							and ios.user_id = base.opr_atlas_user
							--and trackname like 'login%'
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
							hydra.android android,
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
									atlas_user.cod_source_system = 9
									and base_contact.cod_source_system = 13
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
							and android.user_id = base.opr_atlas_user
							--and trackname like 'login%'
						group by
							base.cod_contact ,
							base.cod_contact_parent,
							android.server_date_day,
							dat_snap,
							cod_source_system
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
					and rel.cod_source_system = 13
			) kpi_custom_field
		where
			1 = 1
			and scai.cod_integration = 50000
			and kpi_custom_field.flg_active = 1
			and scai.cod_country = 2
			;
			
--$$$
		
--Calculate for employees
create table crm_integration_anlt.tmp_pl_olx_calc_logins_last_30_days_2 as
   select source.cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	source.custom_field_value
	from crm_integration_anlt.tmp_pl_olx_calc_logins_last_30_days_1 source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where 1 = 1
  and source.cod_contact_parent is not null
  and source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);	

--$$$

--Calculate for companies and contacts not associated with companies
create table crm_integration_anlt.tmp_pl_olx_calc_logins_last_30_days_3 as
   select nvl(source.cod_contact_parent, source.cod_contact) as cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system
	,sum(source.custom_field_value) custom_field_value
	from crm_integration_anlt.tmp_pl_olx_calc_logins_last_30_days_1 source,
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

--$$$

-- HST INSERT - KPI OLX.BASE.086 (# Logins last 30 days)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in
			(select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_logins_last_30_days_2
			union
			select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_logins_last_30_days_3);

--$$$

-- SNAP DELETE - KPI OLX.BASE.086 (# Logins last 30 days)
DELETE FROM crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in
			(select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_logins_last_30_days_2
			union
			select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_logins_last_30_days_3);

--$$$

--KPI OLX.BASE.086 (# Logins last 30 days)
insert into crm_integration_anlt.t_fac_base_integration_snap
	select
		*
	from
		(select * from crm_integration_anlt.tmp_pl_olx_calc_logins_last_30_days_2
		union
		select * from crm_integration_anlt.tmp_pl_olx_calc_logins_last_30_days_3);

--$$$

drop table if exists crm_integration_anlt.tmp_pl_olx_calc_logins_last_30_days_1;
drop table if exists crm_integration_anlt.tmp_pl_olx_calc_logins_last_30_days_2;
drop table if exists crm_integration_anlt.tmp_pl_olx_calc_logins_last_30_days_3;

--$$$

-- CREATE TMP - KPI OLX.BASE.012 (Last package purchased) 
create table crm_integration_anlt.tmp_pl_olx_calc_last_package_purchased_1 as
select
	source.cod_contact,
	cod_contact_parent,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	to_char(source.custom_field_value, 'YYYYMMDD HH24:MI:SS') as custom_field_value
from
	(
select
	cod_contact,
	cod_contact_parent,
	cod_custom_field,
	dat_snap,
	cod_source_system,
	custom_field_value
from
	(
	select
		cod_contact,
		cod_contact_parent,
		dat_snap,
		cod_source_system,
		max(custom_field_value) as custom_field_value
	from(
			select
				base_contact.cod_contact,
				base_contact.cod_contact_parent,
				scai.dat_processing dat_snap,
				base_contact.cod_source_system,
				package.bought as custom_field_value,
				row_number()
				over (
					partition by cod_contact
					order by coalesce(atlas_user.created_at, '1900-01-01') ) rn
			from (select
							a1.*, packets.name packet_name, variants.name variant_name
						from
							db_atlas.olxpl_nnl_userpackets a1
							join db_atlas.olxpl_nnl_variants a2 on a2.variant_id=a1.variant_id
							join db_atlas.olxpl_nnl_usage_log a3 on a1.userpacket_id=a3.userpacket_id
							join db_atlas.olxpl_ads a4 on a4.id=a3.ad_id
							left outer join db_atlas.olxpl_nnl_packets packets on packets.packet_id = a1.packet_id
							left outer join db_atlas.olxpl_nnl_variants variants on variants.variant_id = a1.variant_id
							left outer join crm_integration_anlt.v_lkp_paidad_index_new index on index.cod_source_system = 9 
																								and (index.dsc_paidad_index != 'Ogłoszenie na 30 dni' or index.dsc_paidad_index != 'Dodanie ogłoszenia na 30 dni')
																								and index.paidad_index_code = variants.variant_id
							where 1=1) package,
				crm_integration_anlt.t_lkp_atlas_user atlas_user,
				crm_integration_anlt.t_lkp_contact base_contact,
				crm_integration_anlt.t_rel_scai_country_integration scai
			where 1 = 1
				and package.user_id = atlas_user.opr_atlas_user
				and lower(base_contact.email) = lower(atlas_user.dsc_atlas_user)
				and atlas_user.cod_source_system = 9
				and base_contact.cod_source_system = 13
				and atlas_user.valid_to = 20991231
				and base_contact.valid_to = 20991231
				and scai.cod_integration = 50000
				and scai.cod_country = 2
		)
		where 1=1
		and rn = 1
		group by 	cod_contact,
							cod_contact_parent,
							dat_snap ,
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
			  and lower(kpi.dsc_kpi) = 'last package purchased'
			  and rel.cod_source_system = 13
		) kpi_custom_field
    where 1=1
	and scai.cod_integration = 50000
    and scai.cod_country = 2
	and kpi_custom_field.flg_active = 1 )  source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null)
  ;
  
--$$$
		
--Calculate for employees
create table crm_integration_anlt.tmp_pl_olx_calc_last_package_purchased_2 as
   select source.cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	source.custom_field_value
	from crm_integration_anlt.tmp_pl_olx_calc_last_package_purchased_1 source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where 1 = 1
  and source.cod_contact_parent is not null
  and source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);	

--$$$

--Calculate for companies and contacts not associated with companies
create table crm_integration_anlt.tmp_pl_olx_calc_last_package_purchased_3 as
   select nvl(source.cod_contact_parent, source.cod_contact) as cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system
	,max(source.custom_field_value) custom_field_value
	from crm_integration_anlt.tmp_pl_olx_calc_last_package_purchased_1 source,
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

--$$$

-- HST INSERT - KPI OLX.BASE.012 (Last package purchased)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in
			(select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_last_package_purchased_2
			union
			select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_last_package_purchased_3);

--$$$

-- SNAP DELETE - KPI OLX.BASE.012 (Last package purchased)
DELETE FROM crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in
			(select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_last_package_purchased_2
			union
			select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_last_package_purchased_3);

--$$$

--KPI OLX.BASE.012 (Last package purchased) 
insert into crm_integration_anlt.t_fac_base_integration_snap
	SELECT
		*
	from
		(select * from crm_integration_anlt.tmp_pl_olx_calc_last_package_purchased_2
		union
		select * from crm_integration_anlt.tmp_pl_olx_calc_last_package_purchased_3);

--$$$

drop table if exists crm_integration_anlt.tmp_pl_olx_calc_last_package_purchased_1;
drop table if exists crm_integration_anlt.tmp_pl_olx_calc_last_package_purchased_2;
drop table if exists crm_integration_anlt.tmp_pl_olx_calc_last_package_purchased_3;

--$$$

-- CREATE TMP - KPI OLX.BASE.023 (# Replies)
create table crm_integration_anlt.tmp_pl_olx_calc_replies_1 as
    select
      a.cod_contact,
	  a.cod_contact_parent,
      kpi_custom_field.cod_custom_field,
      scai.dat_processing dat_snap,
      coalesce(a.cod_source_system,13) cod_source_system,
      a.custom_field_value 
    from
      (
        select
		 source.cod_contact,
          source.cod_contact_parent,
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
              db_atlas.olxpl_answers fac,
              crm_integration_anlt.t_lkp_source_system lkp_source_system,
              db_atlas.olxpl_ads ads,
              crm_integration_anlt.t_lkp_atlas_user lkp_user,
              crm_integration_anlt.t_lkp_contact lkp_contact,
              crm_integration_anlt.t_rel_scai_country_integration scai
            where
              lkp_user.cod_source_system = 9
              and lkp_contact.cod_source_system = 13
              and lkp_user.cod_source_system = lkp_source_system.cod_source_system
              and fac.ad_id = ads.id
              and ads.user_id = lkp_user.opr_atlas_user
              and lkp_user.valid_to = 20991231
			  			and lower(lkp_contact.email) = lower(lkp_user.dsc_atlas_user)
              and lkp_contact.valid_to = 20991231
              and scai.cod_integration = 50000
              and trunc(fac.posted) between trunc(sysdate) - 30 and trunc(sysdate)
							and scai.cod_country = 2
						group by
							lkp_contact.cod_contact_parent,
              lkp_contact.cod_contact,
              scai.dat_processing,
              lkp_contact.cod_source_system,
              ads.id
          ) source
        group by
		  source.cod_contact,
          source.cod_source_system,
          source.cod_contact_parent,
          source.dat_processing
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
					and rel.cod_source_system = 13
			) kpi_custom_field
			where
			1=1
			and scai.cod_integration = 50000
			and kpi_custom_field.flg_active = 1
			and scai.cod_country = 2
  ;

--$$$
  
 --Calculate for employees
create table crm_integration_anlt.tmp_pl_olx_calc_replies_2 as
   select source.cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	cast(coalesce(source.custom_field_value, '-') as varchar) custom_field_value
	from crm_integration_anlt.tmp_pl_olx_calc_replies_1 source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where 1 = 1
  and source.cod_contact_parent is not null
  and source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);

--$$$

--Calculate for companies and contacts not associated with companies
create table crm_integration_anlt.tmp_pl_olx_calc_replies_3 as
   select nvl(source.cod_contact_parent, source.cod_contact) as cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	coalesce(cast(sum(source.custom_field_value) as varchar), '-') custom_field_value
	from crm_integration_anlt.tmp_pl_olx_calc_replies_1 source,
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

--$$$

-- HST INSERT - KPI OLX.BASE.023 (# Replies)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in
			(select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_replies_2
			union
			select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_replies_3);

--$$$

-- SNAP DELETE - KPI OLX.BASE.023 (# Replies)
delete from crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in
			(select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_replies_2
			union
			select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_replies_3);

--$$$

-- OLX.BASE.023 (# Replies)
insert into crm_integration_anlt.t_fac_base_integration_snap
  select
    *
  from
    (select * from crm_integration_anlt.tmp_pl_olx_calc_replies_2
		union
		select * from crm_integration_anlt.tmp_pl_olx_calc_replies_3);

--$$$

drop table if exists crm_integration_anlt.tmp_pl_olx_calc_replies_1;
drop table if exists crm_integration_anlt.tmp_pl_olx_calc_replies_2;
drop table if exists crm_integration_anlt.tmp_pl_olx_calc_replies_3;

--$$$

-- CREATE TMP - KPI OLX.BASE.081 (# Replies per Ad)
create table crm_integration_anlt.tmp_pl_olx_calc_replies_per_ad_1 as
    select
      a.cod_contact,
	  a.cod_contact_parent,
      kpi_custom_field.cod_custom_field,
      scai.dat_processing dat_snap,
      coalesce(a.cod_source_system,13) cod_source_system,
       a.custom_field_value 
    from
      (
        select
		  source.cod_contact,
          source.cod_contact_parent,
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
              db_atlas.olxpl_answers fac,
              crm_integration_anlt.t_lkp_source_system lkp_source_system,
              db_atlas.olxpl_ads ads,
              crm_integration_anlt.t_lkp_atlas_user lkp_user,
              crm_integration_anlt.t_lkp_contact lkp_contact,
              crm_integration_anlt.t_rel_scai_country_integration scai
            where
              lkp_user.cod_source_system = 9
              and lkp_contact.cod_source_system = 13
              and lkp_user.cod_source_system = lkp_source_system.cod_source_system
              and fac.ad_id = ads.id
							and ads.status = 'active'
              and ads.user_id = lkp_user.opr_atlas_user
              and lkp_user.valid_to = 20991231
			  			and lower(lkp_contact.email) = lower(lkp_user.dsc_atlas_user)
              and lkp_contact.valid_to = 20991231
              and scai.cod_integration = 50000
              and trunc(fac.posted) between trunc(sysdate) - 30 and trunc(sysdate)
							and scai.cod_country = 2
						group by
							lkp_contact.cod_contact_parent,
              lkp_contact.cod_contact,
              scai.dat_processing,
              lkp_contact.cod_source_system,
              ads.id
          ) source
        group by
		  source.cod_contact,
          source.cod_source_system,
          source.cod_contact_parent,
          source.dat_processing
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
					and rel.cod_source_system = 13
			) kpi_custom_field
			where
			1=1
			and scai.cod_integration = 50000
			and kpi_custom_field.flg_active = 1
			and scai.cod_country = 2
 ;

--$$$
 
--Calculate for employees
create table crm_integration_anlt.tmp_pl_olx_calc_replies_per_ad_2 as
   select source.cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	cast(coalesce(source.custom_field_value, '-') as varchar) custom_field_value
	from crm_integration_anlt.tmp_pl_olx_calc_replies_per_ad_1 source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where 1 = 1
  and source.cod_contact_parent is not null
  and source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);
 
--$$$

--Calculate for companies and contacts not associated with companies
create table crm_integration_anlt.tmp_pl_olx_calc_replies_per_ad_3 as
   select nvl(source.cod_contact_parent, source.cod_contact) as cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	coalesce(cast(sum(source.custom_field_value) as varchar), '-') custom_field_value
	from crm_integration_anlt.tmp_pl_olx_calc_replies_per_ad_1 source,
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

--$$$

-- HST INSERT - KPI OLX.BASE.081 (# Replies per Ad)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in
			(select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_replies_per_ad_2
			union
			select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_replies_per_ad_3);

--$$$

-- SNAP DELETE - KPI OLX.BASE.081 (# Replies per Ad)
DELETE FROM crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in
			(select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_replies_per_ad_2
			union
			select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_replies_per_ad_3);

--$$$

-- OLX.BASE.081 (# Replies per Ad)
insert into crm_integration_anlt.t_fac_base_integration_snap
  select
    *
  from
    (select * from crm_integration_anlt.tmp_pl_olx_calc_replies_per_ad_2
	union
	select * from crm_integration_anlt.tmp_pl_olx_calc_replies_per_ad_3);

--$$$

drop table if exists crm_integration_anlt.tmp_pl_olx_calc_replies_per_ad_1;
drop table if exists crm_integration_anlt.tmp_pl_olx_calc_replies_per_ad_2;
drop table if exists crm_integration_anlt.tmp_pl_olx_calc_replies_per_ad_3;

--$$$

-- CREATE TMP - KPI OLX.BASE.082 (# Ads with replies)
create table crm_integration_anlt.tmp_pl_olx_calc_ads_with_replies_1 as
    select
      a.cod_contact,
	  a.cod_contact_parent,
      kpi_custom_field.cod_custom_field,
      scai.dat_processing dat_snap,
      coalesce(a.cod_source_system,13) cod_source_system,
      a.custom_field_value  custom_field_value
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
							lkp_contact.cod_contact_parent,
              lkp_contact.cod_contact,
              scai.dat_processing,
              lkp_contact.cod_source_system,
              ads.id,
              count(*) nr_replies
            from
              db_atlas.olxpl_answers fac,
              crm_integration_anlt.t_lkp_source_system lkp_source_system,
              db_atlas.olxpl_ads ads,
              crm_integration_anlt.t_lkp_atlas_user lkp_user,
              crm_integration_anlt.t_lkp_contact lkp_contact,
              crm_integration_anlt.t_rel_scai_country_integration scai
            where
              lkp_user.cod_source_system = 9
              and lkp_contact.cod_source_system = 13
              and lkp_user.cod_source_system = lkp_source_system.cod_source_system
              and fac.ad_id = ads.id
							and ads.status = 'active'
              and ads.user_id = lkp_user.opr_atlas_user
              and lkp_user.valid_to = 20991231
			  			and lower(lkp_contact.email) = lower(lkp_user.dsc_atlas_user)
              and lkp_contact.valid_to = 20991231
              and scai.cod_integration = 50000
              and trunc(fac.posted) between trunc(sysdate) - 30 and trunc(sysdate)
							and scai.cod_country = 2
						group by
							lkp_contact.cod_contact_parent,
              lkp_contact.cod_contact,
              scai.dat_processing,
              lkp_contact.cod_source_system,
              ads.id
          ) source
        group by
		  source.cod_contact,	
          source.cod_source_system,
          source.cod_contact_parent,
          source.dat_processing
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
					and rel.cod_source_system = 13
			) kpi_custom_field
			where
			1=1
			and scai.cod_integration = 50000
			and kpi_custom_field.flg_active = 1
			and scai.cod_country = 2
   ;

--$$$
   
--Calculate for employees
create table crm_integration_anlt.tmp_pl_olx_calc_ads_with_replies_2 as
   select source.cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	cast(coalesce(source.custom_field_value, '-') as varchar) custom_field_value
	from crm_integration_anlt.tmp_pl_olx_calc_ads_with_replies_1 source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where 1 = 1
  and source.cod_contact_parent is not null
  and source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);

--$$$

--Calculate for companies and contacts not associated with companies
create table crm_integration_anlt.tmp_pl_olx_calc_ads_with_replies_3 as
   select nvl(source.cod_contact_parent, source.cod_contact) as cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	coalesce(cast(sum(source.custom_field_value) as varchar), '-') custom_field_value
	from crm_integration_anlt.tmp_pl_olx_calc_ads_with_replies_1 source,
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

--$$$

-- HST INSERT - KPI OLX.BASE.082 (# Ads with replies)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in
			(select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_ads_with_replies_2
			union
			select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_ads_with_replies_3);
--$$$

-- SNAP DELETE - KPI OLX.BASE.082 (# Ads with replies)
delete from crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in
			(select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_ads_with_replies_2
			union
			select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_ads_with_replies_3);

--$$$

 -- OLX.BASE.082 (# Ads with replies)
insert into crm_integration_anlt.t_fac_base_integration_snap
  select
      *
  from
    (select * from crm_integration_anlt.tmp_pl_olx_calc_ads_with_replies_2
	union
	select * from crm_integration_anlt.tmp_pl_olx_calc_ads_with_replies_3);

--$$$

drop table if exists crm_integration_anlt.tmp_pl_olx_calc_ads_with_replies_1;
drop table if exists crm_integration_anlt.tmp_pl_olx_calc_ads_with_replies_2;
drop table if exists crm_integration_anlt.tmp_pl_olx_calc_ads_with_replies_3;

--$$$

-- CREATE TMP - KPI OLX.BASE.084 (# Views)
create table crm_integration_anlt.tmp_pl_olx_calc_views_1 as
    select
      a.cod_contact,
	    a.cod_contact_parent,
      kpi_custom_field.cod_custom_field,
      scai.dat_processing dat_snap,
      isnull(a.cod_source_system,13) cod_source_system,
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
						    base.cod_contact,
							base.cod_contact_parent,
							web.server_date_day,
							dat_snap,
							cod_source_system,
							count(*) nbr_views
						from
							hydra.web web,
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
									atlas_user.cod_source_system = 9
									and base_contact.cod_source_system = 13
									and base_contact.cod_atlas_user = atlas_user.cod_atlas_user
									and atlas_user.valid_to = 20991231
									and base_contact.valid_to = 20991231
									and scai.cod_integration = 50000
									and scai.cod_country = 2
									--and base_contact.cod_contact_parent = 306798
							) base,
              db_atlas.olxpl_ads ads
						where
							web.server_date_day >= dateadd(day,-30,sysdate)
							and web.country_code = 'PL'
							and web.host like '%olx.pl%'
							--and web.user_id = base.opr_atlas_user
							and action_type = 'ad_page'
              and web.ad_id = ads.id
              and ads.user_id = base.opr_atlas_user
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
							hydra.ios ios,
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
									atlas_user.cod_source_system = 9
									and base_contact.cod_source_system = 13
									and base_contact.cod_atlas_user = atlas_user.cod_atlas_user
									and atlas_user.valid_to = 20991231
									and base_contact.valid_to = 20991231
									and scai.cod_integration = 50000
									and scai.cod_country = 2
									--and base_contact.cod_contact_parent = 306798
							) base,
              db_atlas.olxpl_ads ads
						where
							ios.server_date_day >= dateadd(day,-30,sysdate)
							and ios.country_code = 'PL'
							--and ios.user_id = base.opr_atlas_user
							and action_type = 'ad_page'
              and ios.ad_id = ads.id
              and ads.user_id = base.opr_atlas_user
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
							hydra.android android,
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
									atlas_user.cod_source_system = 9
									and base_contact.cod_source_system = 13
									and base_contact.cod_atlas_user = atlas_user.cod_atlas_user
									and atlas_user.valid_to = 20991231
									and base_contact.valid_to = 20991231
									and scai.cod_integration = 50000
									and scai.cod_country = 2
									--and base_contact.cod_contact_parent = 306798
							) base,
              db_atlas.olxpl_ads ads
						where
							android.server_date_day >= dateadd(day,-30,sysdate)
							and android.country_code = 'PL'
							--and android.user_id = base.opr_atlas_user
							and action_type = 'ad_page'
              and android.ad_id = ads.id
              and ads.user_id = base.opr_atlas_user
						group by
						  base.cod_contact,
							base.cod_contact_parent,
							android.server_date_day,
							dat_snap,
							cod_source_system
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
					and rel.cod_source_system = 13
			) kpi_custom_field
    where
      1=1
      and scai.cod_integration = 50000
	  and kpi_custom_field.flg_active = 1
	  and scai.cod_country = 2
	  ;

--$$$

--Calculate for employees
create table crm_integration_anlt.tmp_pl_olx_calc_views_2 as
   select source.cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	cast(coalesce(source.custom_field_value, '-') as varchar) custom_field_value
	from crm_integration_anlt.tmp_pl_olx_calc_views_1 source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where 1 = 1
  and source.cod_contact_parent is not null
  and source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);

--$$$

--Calculate for companies and contacts not associated with companies
create table crm_integration_anlt.tmp_pl_olx_calc_views_3 as
   select nvl(source.cod_contact_parent, source.cod_contact) as cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	coalesce(cast(sum(source.custom_field_value) as varchar), '-') custom_field_value
	from crm_integration_anlt.tmp_pl_olx_calc_views_1 source,
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

--$$$
	
-- HST INSERT - KPI OLX.BASE.084 (# Views)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in
			(select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_views_2
			union
			select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_views_3)
			;

--$$$

-- SNAP DELETE - KPI OLX.BASE.084 (# Views)
delete from crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in
			(select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_views_2
			union
			select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_views_3)
			;

--$$$

-- OLX.BASE.084 (# Views)
insert into crm_integration_anlt.t_fac_base_integration_snap
  select
    *
  from
    (select * from crm_integration_anlt.tmp_pl_olx_calc_views_2
		union
		select * from crm_integration_anlt.tmp_pl_olx_calc_views_3);

drop table if exists crm_integration_anlt.tmp_pl_olx_calc_views_1;
drop table if exists crm_integration_anlt.tmp_pl_olx_calc_views_2;
drop table if exists crm_integration_anlt.tmp_pl_olx_calc_views_3;

--$$$

-- CREATE TMP - KPI OLX.BASE.014 (Max days since last call)
create table crm_integration_anlt.tmp_pl_olx_calc_max_days_since_last_call_1 as
    select
      a.cod_contact,
	  a.cod_contact_parent,
      kpi_custom_field.cod_custom_field,
      scai.dat_processing dat_snap,
      isnull(a.cod_source_system,13) cod_source_system,
      a.custom_field_value custom_field_value
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
							crm_integration_anlt.t_lkp_contact lkp_contact,
							crm_integration_anlt.t_rel_scai_country_integration scai
						where
							lkp_contact.cod_source_system = 13
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
			  and rel.cod_source_system = 13
		) kpi_custom_field
    where 1=1      
	  and scai.cod_integration = 50000
      and scai.cod_country = 2
	  and kpi_custom_field.flg_active = 1
	  ;
	  
--$$$
	  
--Calculate for employees
create table crm_integration_anlt.tmp_pl_olx_calc_max_days_since_last_call_2 as
   select source.cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	cast(coalesce(source.custom_field_value, '-') as varchar) custom_field_value
	from crm_integration_anlt.tmp_pl_olx_calc_max_days_since_last_call_1 source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where 1 = 1
  and source.cod_contact_parent is not null
  and source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);

--$$$

--Calculate for companies and contacts not associated with companies
create table crm_integration_anlt.tmp_pl_olx_calc_max_days_since_last_call_3 as
   select nvl(source.cod_contact_parent, source.cod_contact) as cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	coalesce(cast(min(cast(source.custom_field_value as int)) as varchar), '-') custom_field_value
	from crm_integration_anlt.tmp_pl_olx_calc_max_days_since_last_call_1 source,
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

--$$$

-- HST INSERT - KPI OLX.BASE.014 (Max days since last call)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field)  in
			(select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_max_days_since_last_call_2
			union
			select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_max_days_since_last_call_3);

--$$$

-- SNAP DELETE - KPI OLX.BASE.014 (Max days since last call)
delete from crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field)  in
			(select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_max_days_since_last_call_2
			union
			select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_max_days_since_last_call_3);

--$$$

--KPI OLX.BASE.014 (Max days since last call)
insert into crm_integration_anlt.t_fac_base_integration_snap
  select
    *
  from
    (select * from crm_integration_anlt.tmp_pl_olx_calc_max_days_since_last_call_2
		union
		select * from crm_integration_anlt.tmp_pl_olx_calc_max_days_since_last_call_3);

--$$$

drop table if exists crm_integration_anlt.tmp_pl_olx_calc_max_days_since_last_call_1;
drop table if exists crm_integration_anlt.tmp_pl_olx_calc_max_days_since_last_call_2;
drop table if exists crm_integration_anlt.tmp_pl_olx_calc_max_days_since_last_call_3;

--$$$

-- CREATE TMP - KPI OLX.BASE.XYZ (Max Value Package)
create table crm_integration_anlt.tmp_pl_olx_calc_max_value_package_1 as
	SELECT
	  a.cod_contact,
	  a.cod_contact_parent,
	  kpi_custom_field.cod_custom_field,
	  scai.dat_processing dat_snap,
	  coalesce(a.cod_source_system,13) cod_source_system,
	  a.custom_field_value custom_field_value
	FROM
		(
		select
				cod_contact,
				cod_contact_parent,
				cod_source_system,
				round((max(case when cod_index_type = 2 /* package */then price else 0 end)),2) custom_field_value
			from
				(
					select
						base_contact.cod_contact_parent ,
						base_contact.cod_contact,
						base_contact.cod_source_system,
						atlas_user.dsc_atlas_user,
						pup.name,
						to_char(date,'yyyymm') cod_month,
						id_index,
						ads.category_id,
						c.name_pl,
						payment_provider,
						case
							when pb.from_bonus_credits>0 then 'bonus_points'
							when pb.from_refund_credits>0 then 'refund'
							when pb.from_account>0 then 'wallet'
								else 'regular'
						end as wallet,
						case
							when
								(
									u.email like '%sunfra%'
									or lower(u.email) like '%_deleted_%'
									or lower(u.email) like '%shanthi.p667%'
									or lower(u.email) like '%@olx.pl%'
									or lower(u.email) like '%@olx.com%'
									or lower(u.email) like '%satheeshmtiet1993%'
									or lower(u.email) like '%testolxpawel%'
								) then 'test_users'
										else 'regular_users'
						end as user_test,
						sum((pup.price*(-1)) ) as price, count(*) as transactions,
						idx.cod_index_type
					from
						db_atlas.olxpl_paidads_user_payments as pup
						left outer join crm_integration_anlt.v_lkp_paidad_index_new as idx on pup.id_index = idx.opr_paidad_index and idx.cod_source_system = 9
						left outer join db_atlas.olxpl_ads as ads on ads.id=pup.id_ad
						left outer join db_atlas.olxpl_categories as c on c.id=ads.category_id
						left outer join db_atlas.olxpl_users as u on pup.id_user=u.id
						left outer join crm_integration_anlt.t_lkp_atlas_user atlas_user on atlas_user.valid_to = 20991231 and atlas_user.cod_source_system = 9 and u.id = atlas_user.opr_atlas_user
						left outer join db_atlas.olxpl_payment_session as ps on ps.id = pup.id_transaction
						left outer join db_atlas.olxpl_payment_basket as pb
						on pup.id_transaction = pb.session_id
						and pup.id_index = pb.index_id
						and coalesce(pup.id_ad, -1) = coalesce(pb.ad_id, -1)
						and pup.id_user = pb.user_id
						and abs(coalesce(pup.price, 0)) = abs(coalesce(pb.price, 0))
						left outer join (select * from (select *,row_number() over (partition by cod_atlas_user order by cod_contact desc) rn from crm_integration_anlt.t_lkp_contact where valid_to = 20991231) core where rn = 1) base_contact on atlas_user.cod_atlas_user = base_contact.cod_atlas_user and base_contact.cod_source_system = 13
					where
					1=1
					group by
						base_contact.cod_contact_parent,
						base_contact.cod_contact,
						base_contact.cod_source_system,
						u.id,
						atlas_user.dsc_atlas_user,
						name,
						to_char(date,'yyyymm'),
						id_index,
						category_id,
						c.name_pl,
						payment_provider,
						wallet,user_test,
						cod_index_type
				) core
			where
				cod_contact is not null
        and cod_month in (to_char( add_months( sysdate, -1),'YYYYMM') ,  to_char( add_months( sysdate, -2),'YYYYMM'), to_char( add_months( sysdate, -3),'YYYYMM') )
			group by
				cod_contact,
				cod_contact_parent,
				cod_source_system
		  ) A,
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
				  and lower(kpi.dsc_kpi) = 'max package value last 3 months'
				  and rel.cod_source_system = 13
			) kpi_custom_field
		WHERE
		  1 = 1
		  and scai.cod_integration = 50000
		  and scai.cod_country = 2
		  and kpi_custom_field.flg_active = 1
		  ;

--$$$
		  
--Calculate for employees
create table crm_integration_anlt.tmp_pl_olx_calc_max_value_package_2 as
   select source.cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	cast(coalesce(cast(source.custom_field_value as varchar), '') as varchar) custom_field_value
	from crm_integration_anlt.tmp_pl_olx_calc_max_value_package_1 source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where 1 = 1
  and source.cod_contact_parent is not null
  and source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);

--$$$

--Calculate for companies and contacts not associated with companies
create table crm_integration_anlt.tmp_pl_olx_calc_max_value_package_3 as
   select nvl(source.cod_contact_parent, source.cod_contact) as cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	coalesce(cast(max(cast(source.custom_field_value as numeric(15,2) )) as varchar), '') custom_field_value
	from crm_integration_anlt.tmp_pl_olx_calc_max_value_package_1 source,
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

--$$$
	 
-- HST INSERT - KPI OLX.BASE.XYZ (Max Value Package)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in
			(select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_max_value_package_2
			union
			select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_max_value_package_3)
			;

--$$$

-- SNAP DELETE - KPI OLX.BASE.XYZ (Max Value Package)
delete from crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in
			(select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_max_value_package_2
			union
			select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_max_value_package_3)
			;

--$$$

--KPI OLX.BASE.XYZ (Max Value Package)
insert into crm_integration_anlt.t_fac_base_integration_snap
  select
    *
  from
    (select * from crm_integration_anlt.tmp_pl_olx_calc_max_value_package_2
	union
	select * from crm_integration_anlt.tmp_pl_olx_calc_max_value_package_3);

--$$$

drop table if exists crm_integration_anlt.tmp_pl_olx_calc_max_value_package_1;
drop table if exists crm_integration_anlt.tmp_pl_olx_calc_max_value_package_2;
drop table if exists crm_integration_anlt.tmp_pl_olx_calc_max_value_package_3;

--$$$
-- CREATE TMP - KPI OLX.BASE.XXX (Revenue Total / VAS / Listings)
create table crm_integration_anlt.tmp_pl_olx_calc_revenue as
select
	inner_core.cod_contact,
	inner_core.cod_contact_parent,
	inner_core.cod_source_system,
	inner_core.wallet,
	scai.dat_processing dat_snap,
	inner_core.cod_month,
	inner_core.val_revenue_vas_gross,
	inner_core.val_revenue_listings_gross
	from
		(
			select
				cod_contact,
				cod_contact_parent,
				cod_source_system,
				cod_month,
				wallet,
				round((sum(case when cod_index_type = 1 /* vas */ then price else 0 end)),2) val_revenue_vas_gross,
				round((sum(case when cod_index_type = 2 /* package */then price else 0 end)),2) val_revenue_listings_gross
			from
				(
					select
						base_contact.cod_contact_parent ,
						base_contact.cod_contact,
						base_contact.cod_source_system,
						atlas_user.dsc_atlas_user,
						pup.name,
						to_char(date,'yyyymm') cod_month,
						id_index,
						ads.category_id,
						c.name_pl,
						payment_provider,
						case
							when pb.from_bonus_credits>0 then 'bonus_points'
							when pb.from_refund_credits>0 then 'refund'
							when pb.from_account>0 then 'wallet'
								else 'regular'
						end as wallet,
						case
							when
								(
									u.email like '%sunfra%'
									or lower(u.email) like '%_deleted_%'
									or lower(u.email) like '%shanthi.p667%'
									or lower(u.email) like '%@olx.pl%'
									or lower(u.email) like '%@olx.com%'
									or lower(u.email) like '%satheeshmtiet1993%'
									or lower(u.email) like '%testolxpawel%'
								) then 'test_users'
										else 'regular_users'
						end as user_test,
						sum((pup.price*(-1)) ) as price, count(*) as transactions,
						idx.cod_index_type
					from
						db_atlas.olxpl_paidads_user_payments as pup
						left outer join crm_integration_anlt.v_lkp_paidad_index_new as idx on pup.id_index = idx.opr_paidad_index and idx.cod_source_system = 9
						left outer join db_atlas.olxpl_ads as ads on ads.id=pup.id_ad
						left outer join db_atlas.olxpl_categories as c on c.id=ads.category_id
						left outer join db_atlas.olxpl_users as u on pup.id_user=u.id
						left outer join crm_integration_anlt.t_lkp_atlas_user atlas_user on atlas_user.valid_to = 20991231 and atlas_user.cod_source_system = 9 and u.id = atlas_user.opr_atlas_user
						left outer join db_atlas.olxpl_payment_session as ps on ps.id = pup.id_transaction
						left outer join db_atlas.olxpl_payment_basket as pb
						on pup.id_transaction = pb.session_id
						and pup.id_index = pb.index_id
						and coalesce(pup.id_ad, -1) = coalesce(pb.ad_id, -1)
						and pup.id_user = pb.user_id
						and abs(coalesce(pup.price, 0)) = abs(coalesce(pb.price, 0))
						left outer join (select * from (select *,row_number() over (partition by cod_atlas_user order by cod_contact desc) rn from crm_integration_anlt.t_lkp_contact where valid_to = 20991231) core where rn = 1) base_contact on atlas_user.cod_atlas_user = base_contact.cod_atlas_user and base_contact.cod_source_system = 13
					where
					1=1
					group by
						base_contact.cod_contact_parent,
						base_contact.cod_contact,
						base_contact.cod_source_system,
						u.id,
						atlas_user.dsc_atlas_user,
						name,
						to_char(date,'yyyymm'),
						id_index,
						category_id,
						c.name_pl,
						payment_provider,
						wallet,user_test,
						cod_index_type
				) core
			where
				cod_contact is not null
			group by
				cod_contact,
				cod_contact_parent,
				cod_source_system,
				wallet,
				cod_month
	) inner_core,
	crm_integration_anlt.t_rel_scai_country_integration scai
where
	scai.cod_integration = 50000
	and scai.cod_country = 2
	and cod_month between to_char(date_trunc('month',add_months(sysdate,-5)),'yyyymm') and to_char(date_trunc('month', sysdate),'yyyymm')
	;

--$$$

-- CREATE TMP - KPI OLX.BASE.099 (Revenue (0) - Total)
create table crm_integration_anlt.tmp_pl_olx_calc_revenue_0_total_1 as
		select
			cod_contact,
			cod_contact_parent,
			cod_custom_field,
			dat_snap,
			cod_source_system,
			cast(round(nvl(val_revenue_listings_gross,0) + nvl(val_revenue_vas_gross,0),2) as varchar) custom_field_value
		from
			(
				select
					rev_olx.cod_contact,
					rev_olx.cod_contact_parent,
					kpi_custom_field.cod_custom_field,
					rev_olx.dat_snap,
					rev_olx.cod_source_system,
					rev_olx.val_revenue_listings_gross,
					rev_olx.val_revenue_vas_gross
				from
					crm_integration_anlt.tmp_pl_olx_calc_revenue rev_olx,
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
							and rel.cod_source_system = 13
					) kpi_custom_field
				where
					kpi_custom_field.flg_active = 1
					and cod_month = to_char(add_months(sysdate,0),'yyyymm')
					and rev_olx.wallet = 'regular'
			) core
;

--$$$

--Calculate for employees
create table crm_integration_anlt.tmp_pl_olx_calc_revenue_0_total_2 as
   select source.cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	source.custom_field_value
	from crm_integration_anlt.tmp_pl_olx_calc_revenue_0_total_1 source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where 1 = 1
  and source.cod_contact_parent is not null
  and source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);

--$$$

--Calculate for companies and contacts not associated with companies
create table crm_integration_anlt.tmp_pl_olx_calc_revenue_0_total_3 as
   select nvl(source.cod_contact_parent, source.cod_contact) as cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system
	,cast(sum(CAST(source.custom_field_value AS NUMERIC(15,2))) as varchar) custom_field_value
	from crm_integration_anlt.tmp_pl_olx_calc_revenue_0_total_1 source,
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

--$$$

-- HST INSERT - KPI OLX.BASE.099 (Revenue (0) - Total)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in
			(select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_revenue_0_total_2
			union
			select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_revenue_0_total_3);

--$$$

-- SNAP DELETE - KPI OLX.BASE.099 (Revenue (0) - Total)
delete from crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in
			(select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_revenue_0_total_2
			union
			select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_revenue_0_total_3);

--$$$

--KPI OLX.BASE.099 (Revenue (0) - Total)
insert into crm_integration_anlt.t_fac_base_integration_snap
	select
		*
	from
		(select * from crm_integration_anlt.tmp_pl_olx_calc_revenue_0_total_2
		union
		select * from crm_integration_anlt.tmp_pl_olx_calc_revenue_0_total_3);

--$$$

drop table if exists crm_integration_anlt.tmp_pl_olx_calc_revenue_0_total_1;
drop table if exists crm_integration_anlt.tmp_pl_olx_calc_revenue_0_total_2;
drop table if exists crm_integration_anlt.tmp_pl_olx_calc_revenue_0_total_3;

--$$$

-- CREATE TMP - KPI OLX.BASE.100 (Revenue (0) - Listings)
create table crm_integration_anlt.tmp_pl_olx_calc_revenue_0_listings_1 as
		select
			cod_contact,
			cod_contact_parent,
			cod_custom_field,
			dat_snap,
			cod_source_system,
			cast(round(nvl(val_revenue_listings_gross,0),2) as varchar) custom_field_value
		from
			(
				select
					rev_olx.cod_contact,
					rev_olx.cod_contact_parent,
					kpi_custom_field.cod_custom_field,
					rev_olx.dat_snap,
					rev_olx.cod_source_system,
					rev_olx.val_revenue_listings_gross,
					rev_olx.val_revenue_vas_gross
				from
					crm_integration_anlt.tmp_pl_olx_calc_revenue rev_olx,
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
							and rel.cod_source_system = 13
					) kpi_custom_field
				where
					kpi_custom_field.flg_active = 1
					and cod_month = to_char(add_months(sysdate,0),'yyyymm')
					and rev_olx.wallet = 'regular'
			) core
			;

--$$$
			
--Calculate for employees
create table crm_integration_anlt.tmp_pl_olx_calc_revenue_0_listings_2 as
   select source.cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	source.custom_field_value
	from crm_integration_anlt.tmp_pl_olx_calc_revenue_0_listings_1 source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where 1 = 1
  and source.cod_contact_parent is not null
  and source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);

--$$$

--Calculate for companies and contacts not associated with companies
create table crm_integration_anlt.tmp_pl_olx_calc_revenue_0_listings_3 as
   select nvl(source.cod_contact_parent, source.cod_contact) as cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system
	,cast(sum(CAST(source.custom_field_value AS NUMERIC(15,2))) as varchar) custom_field_value
	from crm_integration_anlt.tmp_pl_olx_calc_revenue_0_listings_1 source,
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

--$$$

-- HST INSERT - KPI OLX.BASE.100 (Revenue (0) - Listings)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in
			(select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_revenue_0_listings_2
			union
			select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_revenue_0_listings_3);

--$$$

-- SNAP DELETE - KPI OLX.BASE.100 (Revenue (0) - Listings)
delete from crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in
			(select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_revenue_0_listings_2
			union
			select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_revenue_0_listings_3);

--$$$

--KPI OLX.BASE.100 (Revenue (0) - Listings)
insert into crm_integration_anlt.t_fac_base_integration_snap
	select
		*
	from
		(select * from crm_integration_anlt.tmp_pl_olx_calc_revenue_0_listings_2
		union
		select * from crm_integration_anlt.tmp_pl_olx_calc_revenue_0_listings_3);

--$$$

drop table if exists crm_integration_anlt.tmp_pl_olx_calc_revenue_0_listings_1;
drop table if exists crm_integration_anlt.tmp_pl_olx_calc_revenue_0_listings_2;
drop table if exists crm_integration_anlt.tmp_pl_olx_calc_revenue_0_listings_3;

--$$$

-- CREATE TMP - KPI OLX.BASE.101 (Revenue (0) - VAS)
create table crm_integration_anlt.tmp_pl_olx_calc_revenue_0_vas_1 as
		select
			cod_contact,
			cod_contact_parent,
			cod_custom_field,
			dat_snap,
			cod_source_system,
			cast(round(nvl(val_revenue_vas_gross,0),2) as varchar) custom_field_value
		from
			(
				select
					rev_olx.cod_contact,
					rev_olx.cod_contact_parent,
					kpi_custom_field.cod_custom_field,
					rev_olx.dat_snap,
					rev_olx.cod_source_system,
					rev_olx.val_revenue_listings_gross,
					rev_olx.val_revenue_vas_gross
				from
					crm_integration_anlt.tmp_pl_olx_calc_revenue rev_olx,
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
							and rel.cod_source_system = 13
					) kpi_custom_field
				where
					kpi_custom_field.flg_active = 1
					and cod_month = to_char(add_months(sysdate,0),'yyyymm')
					and rev_olx.wallet = 'regular'
			) core
			;

--$$$
			
--Calculate for employees
create table crm_integration_anlt.tmp_pl_olx_calc_revenue_0_vas_2 as
   select source.cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	source.custom_field_value
	from crm_integration_anlt.tmp_pl_olx_calc_revenue_0_vas_1 source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where 1 = 1
  and source.cod_contact_parent is not null
  and source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);

--$$$

--Calculate for companies and contacts not associated with companies
create table crm_integration_anlt.tmp_pl_olx_calc_revenue_0_vas_3 as
   select nvl(source.cod_contact_parent, source.cod_contact) as cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system
	,cast(sum(CAST(source.custom_field_value AS NUMERIC(15,2))) as varchar) custom_field_value
	from crm_integration_anlt.tmp_pl_olx_calc_revenue_0_vas_1 source,
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
	 
--$$$
	 
-- HST INSERT - KPI OLX.BASE.101 (Revenue (0) - VAS)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in
			(select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_revenue_0_vas_2
			union
			select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_revenue_0_vas_3);

--$$$

-- SNAP DELETE - KPI OLX.BASE.101 (Revenue (0) - VAS)
delete from crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in
			(select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_revenue_0_vas_2
			union
			select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_revenue_0_vas_3);

--$$$			

--KPI OLX.BASE.101 (Revenue (0) - VAS)
insert into crm_integration_anlt.t_fac_base_integration_snap
	select
		*
	from
		(select * from crm_integration_anlt.tmp_pl_olx_calc_revenue_0_vas_2
		union
		select * from crm_integration_anlt.tmp_pl_olx_calc_revenue_0_vas_3);

drop table if exists crm_integration_anlt.tmp_pl_olx_calc_revenue_0_vas_1;
drop table if exists crm_integration_anlt.tmp_pl_olx_calc_revenue_0_vas_2;
drop table if exists crm_integration_anlt.tmp_pl_olx_calc_revenue_0_vas_3;

--$$$

-- CREATE TMP - KPI OLX.BASE.102 (Revenue (-1) - Total)
create table crm_integration_anlt.tmp_pl_olx_calc_revenue_1_total_1 as
		select
			cod_contact,
			cod_contact_parent,
			cod_custom_field,
			dat_snap,
			cod_source_system,
			cast(round(nvl(val_revenue_listings_gross,0) + nvl(val_revenue_vas_gross,0),2) as varchar) custom_field_value
		from
			(
				select
					rev_olx.cod_contact,
					rev_olx.cod_contact_parent,
					kpi_custom_field.cod_custom_field,
					rev_olx.dat_snap,
					rev_olx.cod_source_system,
					rev_olx.val_revenue_listings_gross,
					rev_olx.val_revenue_vas_gross
				from
					crm_integration_anlt.tmp_pl_olx_calc_revenue rev_olx,
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
							and rel.cod_source_system = 13
					) kpi_custom_field
				where
					kpi_custom_field.flg_active = 1
					and cod_month = to_char(add_months(sysdate,-1),'yyyymm')
					and rev_olx.wallet = 'regular'
			) core
			;

--$$$
			
--Calculate for employees
create table crm_integration_anlt.tmp_pl_olx_calc_revenue_1_total_2 as
   select source.cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	source.custom_field_value
	from crm_integration_anlt.tmp_pl_olx_calc_revenue_1_total_1 source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where 1 = 1
  and source.cod_contact_parent is not null
  and source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);

--$$$

--Calculate for companies and contacts not associated with companies
create table crm_integration_anlt.tmp_pl_olx_calc_revenue_1_total_3 as
   select nvl(source.cod_contact_parent, source.cod_contact) as cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system
	,cast(sum(CAST(source.custom_field_value AS NUMERIC(15,2))) as varchar) custom_field_value
	from crm_integration_anlt.tmp_pl_olx_calc_revenue_1_total_1 source,
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

--$$$

-- HST INSERT - KPI OLX.BASE.102 (Revenue (-1) - Total)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in
			(select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_revenue_1_total_2
			union
			select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_revenue_1_total_3);

--$$$

-- SNAP DELETE - KPI OLX.BASE.102 (Revenue (-1) - Total)
delete from crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in
			(select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_revenue_1_total_2
			union
			select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_revenue_1_total_3);

--$$$

--KPI OLX.BASE.102 (Revenue (-1) - Total)
insert into crm_integration_anlt.t_fac_base_integration_snap
	select
		*
	from
		(select * from crm_integration_anlt.tmp_pl_olx_calc_revenue_1_total_2
		union
		select * from crm_integration_anlt.tmp_pl_olx_calc_revenue_1_total_3);

--$$$

drop table if exists crm_integration_anlt.tmp_pl_olx_calc_revenue_1_total_1;
drop table if exists crm_integration_anlt.tmp_pl_olx_calc_revenue_1_total_2;
drop table if exists crm_integration_anlt.tmp_pl_olx_calc_revenue_1_total_3;

--$$$

-- CREATE TMP - KPI OLX.BASE.103 (Revenue (-1) - Listings)
create table crm_integration_anlt.tmp_pl_olx_calc_revenue_1_listings_1 as
		select
			cod_contact,
			cod_contact_parent,
			cod_custom_field,
			dat_snap,
			cod_source_system,
			cast(round(nvl(val_revenue_listings_gross,0),2) as varchar) custom_field_value
		from
			(
				select
					rev_olx.cod_contact,
					rev_olx.cod_contact_parent,
					kpi_custom_field.cod_custom_field,
					rev_olx.dat_snap,
					rev_olx.cod_source_system,
					rev_olx.val_revenue_listings_gross,
					rev_olx.val_revenue_vas_gross
				from
					crm_integration_anlt.tmp_pl_olx_calc_revenue rev_olx,
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
							and rel.cod_source_system = 13
					) kpi_custom_field
				where
					kpi_custom_field.flg_active = 1
					and cod_month = to_char(add_months(sysdate,-1),'yyyymm')
					and rev_olx.wallet = 'regular'
			) core
			;

--$$$
			
--Calculate for employees
create table crm_integration_anlt.tmp_pl_olx_calc_revenue_1_listings_2 as
   select source.cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	source.custom_field_value
	from crm_integration_anlt.tmp_pl_olx_calc_revenue_1_listings_1 source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where 1 = 1
  and source.cod_contact_parent is not null
  and source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);

--$$$

--Calculate for companies and contacts not associated with companies
create table crm_integration_anlt.tmp_pl_olx_calc_revenue_1_listings_3 as
   select nvl(source.cod_contact_parent, source.cod_contact) as cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system
	,cast(sum(CAST(source.custom_field_value AS NUMERIC(15,2))) as varchar) custom_field_value
	from crm_integration_anlt.tmp_pl_olx_calc_revenue_1_listings_1 source,
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

--$$$

-- HST INSERT - KPI OLX.BASE.103 (Revenue (-1) - Listings)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in
			(select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_revenue_1_listings_2
			union
			select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_revenue_1_listings_3);

--$$$

-- SNAP DELETE - KPI OLX.BASE.103 (Revenue (-1) - Listings)
delete from crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in
			(select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_revenue_1_listings_2
			union
			select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_revenue_1_listings_3);

--$$$


--KPI OLX.BASE.103 (Revenue (-1) - Listings)
insert into crm_integration_anlt.t_fac_base_integration_snap
	select
		*
	from
		(select * from crm_integration_anlt.tmp_pl_olx_calc_revenue_1_listings_2
		union
		select * from crm_integration_anlt.tmp_pl_olx_calc_revenue_1_listings_3);

--$$$

drop table if exists crm_integration_anlt.tmp_pl_olx_calc_revenue_1_listings_1;
drop table if exists crm_integration_anlt.tmp_pl_olx_calc_revenue_1_listings_2;
drop table if exists crm_integration_anlt.tmp_pl_olx_calc_revenue_1_listings_3;

--$$$

-- CREATE TMP - KPI OLX.BASE.104 (Revenue (-1) - VAS)
create table crm_integration_anlt.tmp_pl_olx_calc_revenue_1_vas_1 as
		select
			cod_contact,
			cod_contact_parent,
			cod_custom_field,
			dat_snap,
			cod_source_system,
			cast(round(nvl(val_revenue_vas_gross,0),2) as varchar) custom_field_value
		from
			(
				select
					rev_olx.cod_contact,
					rev_olx.cod_contact_parent,
					kpi_custom_field.cod_custom_field,
					rev_olx.dat_snap,
					rev_olx.cod_source_system,
					rev_olx.val_revenue_listings_gross,
					rev_olx.val_revenue_vas_gross
				from
					crm_integration_anlt.tmp_pl_olx_calc_revenue rev_olx,
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
							and rel.cod_source_system = 13
					) kpi_custom_field
				where
					kpi_custom_field.flg_active = 1
					and cod_month = to_char(add_months(sysdate,-1),'yyyymm')
					and rev_olx.wallet = 'regular'
			) core
			;

--$$$
			
--Calculate for employees
create table crm_integration_anlt.tmp_pl_olx_calc_revenue_1_vas_2 as
   select source.cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	source.custom_field_value
	from crm_integration_anlt.tmp_pl_olx_calc_revenue_1_vas_1 source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where 1 = 1
  and source.cod_contact_parent is not null
  and source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);

--$$$

--Calculate for companies and contacts not associated with companies
create table crm_integration_anlt.tmp_pl_olx_calc_revenue_1_vas_3 as
   select nvl(source.cod_contact_parent, source.cod_contact) as cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system
	,cast(sum(CAST(source.custom_field_value AS NUMERIC(15,2))) as varchar) custom_field_value
	from crm_integration_anlt.tmp_pl_olx_calc_revenue_1_vas_1 source,
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

--$$$

-- HST INSERT - KPI OLX.BASE.104 (Revenue (-1) - VAS)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in
			(select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_revenue_1_vas_2
			union
			select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_revenue_1_vas_3);

--$$$

-- SNAP DELETE - KPI OLX.BASE.104 (Revenue (-1) - VAS)
delete from crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in
			(select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_revenue_1_vas_2
			union
			select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_revenue_1_vas_3);

--$$$

--KPI OLX.BASE.104 (Revenue (-1) - VAS)
insert into crm_integration_anlt.t_fac_base_integration_snap
	select
		*
	from
		(select * from crm_integration_anlt.tmp_pl_olx_calc_revenue_1_vas_2
		union
		select * from crm_integration_anlt.tmp_pl_olx_calc_revenue_1_vas_3);

--$$$

drop table if exists crm_integration_anlt.tmp_pl_olx_calc_revenue_1_vas_1;
drop table if exists crm_integration_anlt.tmp_pl_olx_calc_revenue_1_vas_2;
drop table if exists crm_integration_anlt.tmp_pl_olx_calc_revenue_1_vas_3;

--$$$

-- CREATE TMP - KPI OLX.BASE.106 (Revenue (-2) - Total)
create table crm_integration_anlt.tmp_pl_olx_calc_revenue_2_total_1 as
		select
			cod_contact,
			cod_contact_parent,
			cod_custom_field,
			dat_snap,
			cod_source_system,
			cast(round(nvl(val_revenue_listings_gross,0) + nvl(val_revenue_vas_gross,0),2) as varchar) custom_field_value
		from
			(
				select
					rev_olx.cod_contact,
					rev_olx.cod_contact_parent,
					kpi_custom_field.cod_custom_field,
					rev_olx.dat_snap,
					rev_olx.cod_source_system,
					rev_olx.val_revenue_listings_gross,
					rev_olx.val_revenue_vas_gross
				from
					crm_integration_anlt.tmp_pl_olx_calc_revenue rev_olx,
					(
						select
							rel.cod_custom_field,
							rel.flg_active
						from
							crm_integration_anlt.t_lkp_kpi kpi,
							crm_integration_anlt.t_rel_kpi_custom_field rel
						where
							kpi.cod_kpi = rel.cod_kpi
							and lower(kpi.dsc_kpi) = 'revenue (-2) - total'
							and rel.cod_source_system = 13
					) kpi_custom_field
				where
					kpi_custom_field.flg_active = 1
					and cod_month = to_char(add_months(sysdate,-2),'yyyymm')
					and rev_olx.wallet = 'regular'
			) core
			;

--$$$
			
--Calculate for employees
create table crm_integration_anlt.tmp_pl_olx_calc_revenue_2_total_2 as
   select source.cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	source.custom_field_value
	from crm_integration_anlt.tmp_pl_olx_calc_revenue_2_total_1 source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where 1 = 1
  and source.cod_contact_parent is not null
  and source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);

--$$$

--Calculate for companies and contacts not associated with companies
create table crm_integration_anlt.tmp_pl_olx_calc_revenue_2_total_3 as
   select nvl(source.cod_contact_parent, source.cod_contact) as cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system
	,cast(sum(CAST(source.custom_field_value AS NUMERIC(15,2))) as varchar) custom_field_value
	from crm_integration_anlt.tmp_pl_olx_calc_revenue_2_total_1 source,
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

--$$$

-- HST INSERT - KPI OLX.BASE.106 (Revenue (-2) - Total)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in
			(select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_revenue_2_total_2
			union
			select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_revenue_2_total_3);

--$$$

-- SNAP DELETE - KPI OLX.BASE.106 (Revenue (-2) - Total)
delete from crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in
			(select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_revenue_2_total_2
			union
			select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_revenue_2_total_3);

--$$$

--KPI OLX.BASE.106 (Revenue (-2) - Total)
insert into crm_integration_anlt.t_fac_base_integration_snap
	select
		*
	from
		(select * from crm_integration_anlt.tmp_pl_olx_calc_revenue_2_total_2
		union
		select * from crm_integration_anlt.tmp_pl_olx_calc_revenue_2_total_3);

--$$$

drop table if exists crm_integration_anlt.tmp_pl_olx_calc_revenue_2_total_1;
drop table if exists crm_integration_anlt.tmp_pl_olx_calc_revenue_2_total_2;
drop table if exists crm_integration_anlt.tmp_pl_olx_calc_revenue_2_total_3;

--$$$

-- CREATE TMP - KPI OLX.BASE.107 (Revenue (-2) - Listings)
create table crm_integration_anlt.tmp_pl_olx_calc_revenue_2_listings_1 as
		select
			cod_contact,
			cod_contact_parent,
			cod_custom_field,
			dat_snap,
			cod_source_system,
			cast(round(nvl(val_revenue_listings_gross,0),2) as varchar) custom_field_value
		from
			(
				select
					rev_olx.cod_contact,
					rev_olx.cod_contact_parent,
					kpi_custom_field.cod_custom_field,
					rev_olx.dat_snap,
					rev_olx.cod_source_system,
					rev_olx.val_revenue_listings_gross,
					rev_olx.val_revenue_vas_gross
				from
					crm_integration_anlt.tmp_pl_olx_calc_revenue rev_olx,
					(
						select
							rel.cod_custom_field,
							rel.flg_active
						from
							crm_integration_anlt.t_lkp_kpi kpi,
							crm_integration_anlt.t_rel_kpi_custom_field rel
						where
							kpi.cod_kpi = rel.cod_kpi
							and lower(kpi.dsc_kpi) = 'revenue (-2) - listings'
							and rel.cod_source_system = 13
					) kpi_custom_field
				where
					kpi_custom_field.flg_active = 1
					and cod_month = to_char(add_months(sysdate,-2),'yyyymm')
					and rev_olx.wallet = 'regular'
			) core
			;

--$$$
			
--Calculate for employees
create table crm_integration_anlt.tmp_pl_olx_calc_revenue_2_listings_2 as
   select source.cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	source.custom_field_value
	from crm_integration_anlt.tmp_pl_olx_calc_revenue_2_listings_1 source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where 1 = 1
  and source.cod_contact_parent is not null
  and source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);

--$$$

--Calculate for companies and contacts not associated with companies
create table crm_integration_anlt.tmp_pl_olx_calc_revenue_2_listings_3 as
   select nvl(source.cod_contact_parent, source.cod_contact) as cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system
	,cast(sum(CAST(source.custom_field_value AS NUMERIC(15,2))) as varchar) custom_field_value
	from crm_integration_anlt.tmp_pl_olx_calc_revenue_2_listings_1 source,
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

--$$$

-- HST INSERT - KPI OLX.BASE.107 (Revenue (-2) - Listings)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in
			(select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_revenue_2_listings_2
			union
			select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_revenue_2_listings_3);

--$$$

-- SNAP DELETE - KPI OLX.BASE.107 (Revenue (-2) - Listings)
delete from crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in
			(select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_revenue_2_listings_2
			union
			select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_revenue_2_listings_3);

--$$$

--KPI OLX.BASE.107 (Revenue (-2) - Listings)
insert into crm_integration_anlt.t_fac_base_integration_snap
	select
		*
	from
		(select * from crm_integration_anlt.tmp_pl_olx_calc_revenue_2_listings_2
		union
		select * from crm_integration_anlt.tmp_pl_olx_calc_revenue_2_listings_3);

--$$$

drop table if exists crm_integration_anlt.tmp_pl_olx_calc_revenue_2_listings_1;
drop table if exists crm_integration_anlt.tmp_pl_olx_calc_revenue_2_listings_2;
drop table if exists crm_integration_anlt.tmp_pl_olx_calc_revenue_2_listings_3;

--$$$

-- CREATE TMP - KPI OLX.BASE.108 (Revenue (-2) - VAS)
create table crm_integration_anlt.tmp_pl_olx_calc_revenue_2_vas_1 as
		select
			cod_contact,
			cod_contact_parent,
			cod_custom_field,
			dat_snap,
			cod_source_system,
			cast(round(nvl(val_revenue_vas_gross,0),2) as varchar) custom_field_value
		from
			(
				select
					rev_olx.cod_contact,
					rev_olx.cod_contact_parent,
					kpi_custom_field.cod_custom_field,
					rev_olx.dat_snap,
					rev_olx.cod_source_system,
					rev_olx.val_revenue_listings_gross,
					rev_olx.val_revenue_vas_gross
				from
					crm_integration_anlt.tmp_pl_olx_calc_revenue rev_olx,
					(
						select
							rel.cod_custom_field,
							rel.flg_active
						from
							crm_integration_anlt.t_lkp_kpi kpi,
							crm_integration_anlt.t_rel_kpi_custom_field rel
						where
							kpi.cod_kpi = rel.cod_kpi
							and lower(kpi.dsc_kpi) = 'revenue (-2) - vas'
							and rel.cod_source_system = 13
					) kpi_custom_field
				where
					kpi_custom_field.flg_active = 1
					and cod_month = to_char(add_months(sysdate,-2),'yyyymm')
					and rev_olx.wallet = 'regular'
			) core
			;

--$$$
			
--Calculate for employees
create table crm_integration_anlt.tmp_pl_olx_calc_revenue_2_vas_2 as
   select source.cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	source.custom_field_value
	from crm_integration_anlt.tmp_pl_olx_calc_revenue_2_vas_1 source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where 1 = 1
  and source.cod_contact_parent is not null
  and source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);

--$$$

--Calculate for companies and contacts not associated with companies
create table crm_integration_anlt.tmp_pl_olx_calc_revenue_2_vas_3 as
   select nvl(source.cod_contact_parent, source.cod_contact) as cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system
	,cast(sum(CAST(source.custom_field_value AS NUMERIC(15,2))) as varchar) custom_field_value
	from crm_integration_anlt.tmp_pl_olx_calc_revenue_2_vas_1 source,
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

--$$$

-- HST INSERT - KPI OLX.BASE.108 (Revenue (-2) - VAS)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in
			(select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_revenue_2_vas_2
			union
			select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_revenue_2_vas_3);

--$$$

-- SNAP DELETE - KPI OLX.BASE.108 (Revenue (-2) - VAS)
delete from crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in
			(select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_revenue_2_vas_2
			union
			select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_revenue_2_vas_3);

--$$$

--KPI OLX.BASE.108 (Revenue (-2) - VAS)
insert into crm_integration_anlt.t_fac_base_integration_snap
	select
		*
	from
		(select * from crm_integration_anlt.tmp_pl_olx_calc_revenue_2_vas_2
		union
		select * from crm_integration_anlt.tmp_pl_olx_calc_revenue_2_vas_3);

--$$$

drop table if exists crm_integration_anlt.tmp_pl_olx_calc_revenue_2_vas_1;
drop table if exists crm_integration_anlt.tmp_pl_olx_calc_revenue_2_vas_2;
drop table if exists crm_integration_anlt.tmp_pl_olx_calc_revenue_2_vas_3;

--$$$

-- CREATE TMP - KPI OLX.BASE.109 (Revenue (-3) - Total)
create table crm_integration_anlt.tmp_pl_olx_calc_revenue_3_total_1 as
		select
			cod_contact,
			cod_contact_parent,
			cod_custom_field,
			dat_snap,
			cod_source_system,
			cast(round(nvl(val_revenue_listings_gross,0) + nvl(val_revenue_vas_gross,0),2) as varchar) custom_field_value
		from
			(
				select
					rev_olx.cod_contact,
					rev_olx.cod_contact_parent,
					kpi_custom_field.cod_custom_field,
					rev_olx.dat_snap,
					rev_olx.cod_source_system,
					rev_olx.val_revenue_listings_gross,
					rev_olx.val_revenue_vas_gross
				from
					crm_integration_anlt.tmp_pl_olx_calc_revenue rev_olx,
					(
						select
							rel.cod_custom_field,
							rel.flg_active
						from
							crm_integration_anlt.t_lkp_kpi kpi,
							crm_integration_anlt.t_rel_kpi_custom_field rel
						where
							kpi.cod_kpi = rel.cod_kpi
							and lower(kpi.dsc_kpi) = 'revenue (-3) - total'
							and rel.cod_source_system = 13
					) kpi_custom_field
				where
					kpi_custom_field.flg_active = 1
					and cod_month = to_char(add_months(sysdate,-3),'yyyymm')
					and rev_olx.wallet = 'regular'
			) core
			;
	
--$$$
	
--Calculate for employees
create table crm_integration_anlt.tmp_pl_olx_calc_revenue_3_total_2 as
   select source.cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	source.custom_field_value
	from crm_integration_anlt.tmp_pl_olx_calc_revenue_3_total_1 source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where 1 = 1
  and source.cod_contact_parent is not null
  and source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);

--$$$

--Calculate for companies and contacts not associated with companies
create table crm_integration_anlt.tmp_pl_olx_calc_revenue_3_total_3 as
   select nvl(source.cod_contact_parent, source.cod_contact) as cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system
	,cast(sum(CAST(source.custom_field_value AS NUMERIC(15,2))) as varchar) custom_field_value
	from crm_integration_anlt.tmp_pl_olx_calc_revenue_3_total_1 source,
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

--$$$

-- HST INSERT - KPI OLX.BASE.109 (Revenue (-3) - Total)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in
			(select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_revenue_3_total_2
			union
			select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_revenue_3_total_3);

--$$$

-- SNAP DELETE - KPI OLX.BASE.109 (Revenue (-3) - Total)
delete from crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in
			(select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_revenue_3_total_2
			union
			select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_revenue_3_total_3);

--$$$

--KPI OLX.BASE.109 (Revenue (-3) - Total)
insert into crm_integration_anlt.t_fac_base_integration_snap
	select
		*
	from
		(select * from crm_integration_anlt.tmp_pl_olx_calc_revenue_3_total_2
		union
		select * from crm_integration_anlt.tmp_pl_olx_calc_revenue_3_total_3);
--$$$

drop table if exists crm_integration_anlt.tmp_pl_olx_calc_revenue_3_total_1;
drop table if exists crm_integration_anlt.tmp_pl_olx_calc_revenue_3_total_2;
drop table if exists crm_integration_anlt.tmp_pl_olx_calc_revenue_3_total_3;

--$$$

-- CREATE TMP - KPI OLX.BASE.110 (Revenue (-3) - Listings)
create table crm_integration_anlt.tmp_pl_olx_calc_revenue_3_listings_1 as
		select
			cod_contact,
			cod_contact_parent,
			cod_custom_field,
			dat_snap,
			cod_source_system,
			cast(round(nvl(val_revenue_listings_gross,0),2) as varchar) custom_field_value
		from
			(
				select
					rev_olx.cod_contact,
					rev_olx.cod_contact_parent,
					kpi_custom_field.cod_custom_field,
					rev_olx.dat_snap,
					rev_olx.cod_source_system,
					rev_olx.val_revenue_listings_gross,
					rev_olx.val_revenue_vas_gross
				from
					crm_integration_anlt.tmp_pl_olx_calc_revenue rev_olx,
					(
						select
							rel.cod_custom_field,
							rel.flg_active
						from
							crm_integration_anlt.t_lkp_kpi kpi,
							crm_integration_anlt.t_rel_kpi_custom_field rel
						where
							kpi.cod_kpi = rel.cod_kpi
							and lower(kpi.dsc_kpi) = 'revenue (-3) - listings'
							and rel.cod_source_system = 13
					) kpi_custom_field
				where
					kpi_custom_field.flg_active = 1
					and cod_month = to_char(add_months(sysdate,-3),'yyyymm')
					and rev_olx.wallet = 'regular'
			) core
			;

--$$$
			
--Calculate for employees
create table crm_integration_anlt.tmp_pl_olx_calc_revenue_3_listings_2 as
   select source.cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	source.custom_field_value
	from crm_integration_anlt.tmp_pl_olx_calc_revenue_3_listings_1 source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where 1 = 1
  and source.cod_contact_parent is not null
  and source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);

--$$$

--Calculate for companies and contacts not associated with companies
create table crm_integration_anlt.tmp_pl_olx_calc_revenue_3_listings_3 as
   select nvl(source.cod_contact_parent, source.cod_contact) as cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system
	,cast(sum(CAST(source.custom_field_value AS NUMERIC(15,2))) as varchar) custom_field_value
	from crm_integration_anlt.tmp_pl_olx_calc_revenue_3_listings_1 source,
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

--$$$

-- HST INSERT - KPI OLX.BASE.110 (Revenue (-3) - Listings)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in
			(select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_revenue_3_listings_2
			union
			select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_revenue_3_listings_3);

--$$$

-- SNAP DELETE - KPI OLX.BASE.110 (Revenue (-3) - Listings)
delete from crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in
			(select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_revenue_3_listings_2
			union
			select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_revenue_3_listings_3);

--$$$

--KPI OLX.BASE.110 (Revenue (-3) - Listings)
insert into crm_integration_anlt.t_fac_base_integration_snap
	select
		*
	from
		(select * from crm_integration_anlt.tmp_pl_olx_calc_revenue_3_listings_2
		union
		select * from crm_integration_anlt.tmp_pl_olx_calc_revenue_3_listings_3);

--$$$

drop table if exists crm_integration_anlt.tmp_pl_olx_calc_revenue_3_listings_1;
drop table if exists crm_integration_anlt.tmp_pl_olx_calc_revenue_3_listings_2;
drop table if exists crm_integration_anlt.tmp_pl_olx_calc_revenue_3_listings_3;

--$$$

-- CREATE TMP - KPI OLX.BASE.111 (Revenue (-3) - VAS)
create table crm_integration_anlt.tmp_pl_olx_calc_revenue_3_vas_1 as
		select
			cod_contact,
			cod_contact_parent,
			cod_custom_field,
			dat_snap,
			cod_source_system,
			cast(round(nvl(val_revenue_vas_gross,0),2) as varchar) custom_field_value
		from
			(
				select
					rev_olx.cod_contact,
					rev_olx.cod_contact_parent,
					kpi_custom_field.cod_custom_field,
					rev_olx.dat_snap,
					rev_olx.cod_source_system,
					rev_olx.val_revenue_listings_gross,
					rev_olx.val_revenue_vas_gross
				from
					crm_integration_anlt.tmp_pl_olx_calc_revenue rev_olx,
					(
						select
							rel.cod_custom_field,
							rel.flg_active
						from
							crm_integration_anlt.t_lkp_kpi kpi,
							crm_integration_anlt.t_rel_kpi_custom_field rel
						where
							kpi.cod_kpi = rel.cod_kpi
							and lower(kpi.dsc_kpi) = 'revenue (-3) - vas'
							and rel.cod_source_system = 13
					) kpi_custom_field
				where
					kpi_custom_field.flg_active = 1
					and cod_month = to_char(add_months(sysdate,-3),'yyyymm')
					and rev_olx.wallet = 'regular'
			) core
			;

--$$$
			
--Calculate for employees
create table crm_integration_anlt.tmp_pl_olx_calc_revenue_3_vas_2 as
   select source.cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	source.custom_field_value
	from crm_integration_anlt.tmp_pl_olx_calc_revenue_3_vas_1 source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where 1 = 1
  and source.cod_contact_parent is not null
  and source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);

--$$$

--Calculate for companies and contacts not associated with companies
create table crm_integration_anlt.tmp_pl_olx_calc_revenue_3_vas_3 as
   select nvl(source.cod_contact_parent, source.cod_contact) as cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system
	,cast(sum(CAST(source.custom_field_value AS NUMERIC(15,2))) as varchar) custom_field_value
	from crm_integration_anlt.tmp_pl_olx_calc_revenue_3_vas_1 source,
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

--$$$

-- HST INSERT - KPI OLX.BASE.111 (Revenue (-3) - VAS)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in
			(select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_revenue_3_vas_2
			union
			select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_revenue_3_vas_3);

--$$$

-- SNAP DELETE - KPI OLX.BASE.111 (Revenue (-3) - VAS)
delete from crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in
			(select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_revenue_3_vas_2
			union
			select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_revenue_3_vas_3);

--$$$

--KPI OLX.BASE.111 (Revenue (-3) - VAS)
insert into crm_integration_anlt.t_fac_base_integration_snap
	select
		*
	from
		(select * from crm_integration_anlt.tmp_pl_olx_calc_revenue_3_vas_2
		union
		select * from crm_integration_anlt.tmp_pl_olx_calc_revenue_3_vas_3);

--$$$

drop table if exists crm_integration_anlt.tmp_pl_olx_calc_revenue_3_vas_1;
drop table if exists crm_integration_anlt.tmp_pl_olx_calc_revenue_3_vas_2;
drop table if exists crm_integration_anlt.tmp_pl_olx_calc_revenue_3_vas_3;
--drop table if exists crm_integration_anlt.tmp_pl_olx_calc_revenue; (drop will be executed after wallet)

--$$$

-- CREATE TMP - KPI OLX.BASE.091 (Wallet)
create table crm_integration_anlt.tmp_pl_olx_calc_wallet_1 as
		select
			cod_contact,
			cod_contact_parent,
			cod_custom_field,
			dat_snap,
			cod_source_system,
			cast(round(nvl(val_revenue_listings_gross,0) + nvl(val_revenue_vas_gross,0),2) as varchar) custom_field_value
		from
			(
				select
					rev_olx.cod_contact,
					rev_olx.cod_contact_parent,
					kpi_custom_field.cod_custom_field,
					rev_olx.dat_snap,
					rev_olx.cod_source_system,
					rev_olx.val_revenue_listings_gross,
					rev_olx.val_revenue_vas_gross
				from
					crm_integration_anlt.tmp_pl_olx_calc_revenue rev_olx,
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
							and rel.cod_source_system = 13
					) kpi_custom_field
				where
					kpi_custom_field.flg_active = 1
					and cod_month = to_char(add_months(sysdate,0),'yyyymm')
					and rev_olx.wallet = 'wallet'
			) core
			;
	
--$$$
	
--Calculate for employees
create table crm_integration_anlt.tmp_pl_olx_calc_wallet_2 as
   select source.cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	source.custom_field_value
	from crm_integration_anlt.tmp_pl_olx_calc_wallet_1 source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where 1 = 1
  and source.cod_contact_parent is not null
  and source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);

--$$$

--Calculate for companies and contacts not associated with companies
create table crm_integration_anlt.tmp_pl_olx_calc_wallet_3 as
   select nvl(source.cod_contact_parent, source.cod_contact) as cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system
	,cast(sum(source.custom_field_value) as varchar) custom_field_value
	from crm_integration_anlt.tmp_pl_olx_calc_wallet_1 source,
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

--$$$

-- HST INSERT - KPI OLX.BASE.091 (Wallet)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in
			(select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_wallet_2
			union
			select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_wallet_3);

--$$$

-- SNAP DELETE - KPI OLX.BASE.091 (Wallet)
delete from crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in
			(select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_wallet_2
			union
			select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_wallet_3);

--$$$

-- KPI OLX.BASE.091 (Wallet)
insert into crm_integration_anlt.t_fac_base_integration_snap
  select
    *
  from
    (select * from crm_integration_anlt.tmp_pl_olx_calc_wallet_2
		union
		select * from crm_integration_anlt.tmp_pl_olx_calc_wallet_3);

--$$$

drop table if exists crm_integration_anlt.tmp_pl_olx_calc_wallet_1;
drop table if exists crm_integration_anlt.tmp_pl_olx_calc_wallet_2;
drop table if exists crm_integration_anlt.tmp_pl_olx_calc_wallet_3;

--$$$

-- CREATE TMP - KPI OLX.BASE.091 (refund)
create table crm_integration_anlt.tmp_pl_olx_calc_refund_1 as
		select
			cod_contact,
			cod_contact_parent,
			cod_custom_field,
			dat_snap,
			cod_source_system,
			cast(round(nvl(val_revenue_listings_gross,0) + nvl(val_revenue_vas_gross,0),2) as varchar) custom_field_value
		from
			(
				select
					rev_olx.cod_contact,
					rev_olx.cod_contact_parent,
					kpi_custom_field.cod_custom_field,
					rev_olx.dat_snap,
					rev_olx.cod_source_system,
					rev_olx.val_revenue_listings_gross,
					rev_olx.val_revenue_vas_gross
				from
					crm_integration_anlt.tmp_pl_olx_calc_revenue rev_olx,
					(
						select
							rel.cod_custom_field,
							rel.flg_active
						from
							crm_integration_anlt.t_lkp_kpi kpi,
							crm_integration_anlt.t_rel_kpi_custom_field rel
						where
							kpi.cod_kpi = rel.cod_kpi
							and lower(kpi.dsc_kpi) = 'refund'
							and rel.cod_source_system = 13
					) kpi_custom_field
				where
					kpi_custom_field.flg_active = 1
					and cod_month = to_char(add_months(sysdate,0),'yyyymm')
					and rev_olx.wallet = 'refund'
			) core
			;

--$$$
			
--Calculate for employees
create table crm_integration_anlt.tmp_pl_olx_calc_refund_2 as
   select source.cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	source.custom_field_value
	from crm_integration_anlt.tmp_pl_olx_calc_refund_1 source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where 1 = 1
  and source.cod_contact_parent is not null
  and source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);

--$$$

--Calculate for companies and contacts not associated with companies
create table crm_integration_anlt.tmp_pl_olx_calc_refund_3 as
   select nvl(source.cod_contact_parent, source.cod_contact) as cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system
	,cast(sum(source.custom_field_value) as varchar) custom_field_value
	from crm_integration_anlt.tmp_pl_olx_calc_refund_1 source,
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

--$$$

-- HST INSERT - KPI OLX.BASE.091 (Wallet)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in
			(select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_refund_2
			union
			select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_refund_3);

--$$$

-- SNAP DELETE - KPI OLX.BASE.091 (Wallet)
delete from crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in
			(select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_refund_2
			union
			select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_refund_3);

--$$$

-- KPI OLX.BASE.091 (Wallet)
insert into crm_integration_anlt.t_fac_base_integration_snap
  select
    *
  from
    (select * from crm_integration_anlt.tmp_pl_olx_calc_refund_2
		union
		select * from crm_integration_anlt.tmp_pl_olx_calc_refund_3);

--$$$

drop table if exists crm_integration_anlt.tmp_pl_olx_calc_refund_1;
drop table if exists crm_integration_anlt.tmp_pl_olx_calc_refund_2;
drop table if exists crm_integration_anlt.tmp_pl_olx_calc_refund_3;

--$$$

-- CREATE TMP - KPI OLX.BASE.091 (bonus points)
create table crm_integration_anlt.tmp_pl_olx_calc_bonus_1 as
		select
			cod_contact,
			cod_contact_parent,
			cod_custom_field,
			dat_snap,
			cod_source_system,
			cast(round(nvl(val_revenue_listings_gross,0) + nvl(val_revenue_vas_gross,0),2) as varchar) custom_field_value
		from
			(
				select
					rev_olx.cod_contact,
					rev_olx.cod_contact_parent,
					kpi_custom_field.cod_custom_field,
					rev_olx.dat_snap,
					rev_olx.cod_source_system,
					rev_olx.val_revenue_listings_gross,
					rev_olx.val_revenue_vas_gross
				from
					crm_integration_anlt.tmp_pl_olx_calc_revenue rev_olx,
					(
						select
							rel.cod_custom_field,
							rel.flg_active
						from
							crm_integration_anlt.t_lkp_kpi kpi,
							crm_integration_anlt.t_rel_kpi_custom_field rel
						where
							kpi.cod_kpi = rel.cod_kpi
							and lower(kpi.dsc_kpi) = 'bonus points'
							and rel.cod_source_system = 13
					) kpi_custom_field
				where
					kpi_custom_field.flg_active = 1
					and cod_month = to_char(add_months(sysdate,0),'yyyymm')
					and rev_olx.wallet = 'bonus points'
			) core
			;
	
--$$$
	
--Calculate for employees
create table crm_integration_anlt.tmp_pl_olx_calc_bonus_2 as
   select source.cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	source.custom_field_value
	from crm_integration_anlt.tmp_pl_olx_calc_bonus_1 source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where 1 = 1
  and source.cod_contact_parent is not null
  and source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);

--$$$

--Calculate for companies and contacts not associated with companies
create table crm_integration_anlt.tmp_pl_olx_calc_bonus_3 as
   select nvl(source.cod_contact_parent, source.cod_contact) as cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system
	,cast(sum(source.custom_field_value) as varchar) custom_field_value
	from crm_integration_anlt.tmp_pl_olx_calc_bonus_1 source,
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

--$$$

-- HST INSERT - KPI OLX.BASE.091 (Wallet)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in
			(select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_bonus_2
			union
			select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_bonus_3);

--$$$

-- SNAP DELETE - KPI OLX.BASE.091 (Wallet)
delete from crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in
			(select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_bonus_2
			union
			select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_bonus_3);

--$$$

-- KPI OLX.BASE.091 (Wallet)
insert into crm_integration_anlt.t_fac_base_integration_snap
  select
    *
  from
    (select * from crm_integration_anlt.tmp_pl_olx_calc_bonus_2
		union
		select * from crm_integration_anlt.tmp_pl_olx_calc_bonus_3);

--$$$

drop table if exists crm_integration_anlt.tmp_pl_olx_calc_bonus_1;
drop table if exists crm_integration_anlt.tmp_pl_olx_calc_bonus_2;
drop table if exists crm_integration_anlt.tmp_pl_olx_calc_bonus_3;
drop table if exists crm_integration_anlt.tmp_pl_olx_calc_revenue;

--$$$

-- CREATE TMP - KPI OLX.BASE.006 (# Active ads per category)
create table crm_integration_anlt.tmp_pl_olx_calc_active_ads_per_category_core as
select
	cod_contact,
	cod_contact_parent,
	lower(email) email,
	kpi_custom_field.cod_custom_field,
	scai.dat_snap,
	core.cod_source_system,
	'' + cast(nvl(core.custom_field_value,0) as varchar) custom_field_value
from
	(
		select
			base_contact.cod_contact,
			base_contact.cod_contact_parent,
			base_contact.email,
			max(scai.dat_processing) dat_snap,
			base_contact.cod_source_system,
			count(ads.status) custom_field_value
		from
			crm_integration_anlt.t_lkp_atlas_user atlas_user,
			crm_integration_anlt.t_lkp_contact base_contact,
			db_atlas.olxpl_ads ads, 
			crm_integration_anlt.t_rel_scai_country_integration scai
		where
			atlas_user.cod_source_system = 9
			and base_contact.cod_source_system = 13
			and lower(base_contact.email) = lower(atlas_user.dsc_atlas_user)
			and atlas_user.valid_to = 20991231
			and base_contact.valid_to = 20991231
			and scai.cod_integration = 50000 
			--and atlas_user.valid_from = scai.dat_processing 
			and ads.user_id = atlas_user.opr_atlas_user
			and ads.status = 'active' 
			and scai.cod_country = 1
		group by
			base_contact.cod_contact,
			base_contact.cod_contact_parent,
			base_contact.email,
			base_contact.cod_source_system
	) core, 
	(select max(dat_processing) dat_snap from crm_integration_anlt.t_rel_scai_integration_process where cod_integration = 50000 and cod_country = 1) scai,
(
	select
		rel.cod_custom_field,
		rel.flg_active
	from
		crm_integration_anlt.t_lkp_kpi kpi,
		crm_integration_anlt.t_rel_kpi_custom_field rel
	where
		kpi.cod_kpi = rel.cod_kpi
		and lower(kpi.dsc_kpi) = '# active ads per category'
		and rel.cod_source_system = 13
) kpi_custom_field
where
	1=1 
	and kpi_custom_field.flg_active = 1;

--$$$
	
create table crm_integration_anlt.tmp_pl_otodom_calc_active_ads_per_category_core as
select
	cod_contact,
	cod_contact_parent,
	lower(email) email,
	kpi_custom_field.cod_custom_field,
	scai.dat_snap,
	core.cod_source_system,
	'' + cast(nvl(core.custom_field_value,0) as varchar) custom_field_value
from
	(
		select
			base_contact.cod_contact,
			base_contact.cod_contact_parent,
			base_contact.email,
			max(scai.dat_processing) dat_snap,
			base_contact.cod_source_system,
			count(ads.status) custom_field_value
		from
			crm_integration_anlt.t_lkp_atlas_user atlas_user,
			crm_integration_anlt.t_lkp_contact base_contact,
			db_atlas.olxpl_ads ads, 
			crm_integration_anlt.t_rel_scai_country_integration scai
		where
			atlas_user.cod_source_system = 3
			and base_contact.cod_source_system = 14
			and lower(base_contact.email) = lower(atlas_user.dsc_atlas_user)
			and atlas_user.valid_to = 20991231
			and base_contact.valid_to = 20991231
			and scai.cod_integration = 50000 
			--and atlas_user.valid_from = scai.dat_processing 
			and ads.user_id = atlas_user.cod_atlas_user
			and ads.status = 'active'
			and scai.cod_country = 1
		group by
			base_contact.cod_contact,
			base_contact.cod_contact_parent,
			base_contact.email,
			base_contact.cod_source_system
	) core,
	(select max(dat_processing) dat_snap from crm_integration_anlt.t_rel_scai_integration_process where cod_integration = 50000 and cod_country = 1) scai,
(
	select
		rel.cod_custom_field,
		rel.flg_active
	from
		crm_integration_anlt.t_lkp_kpi kpi,
		crm_integration_anlt.t_rel_kpi_custom_field rel
	where
		kpi.cod_kpi = rel.cod_kpi
		and lower(kpi.dsc_kpi) = '# active ads per category'
		and rel.cod_source_system = 14
) kpi_custom_field
where
	1=1
	and kpi_custom_field.flg_active = 1;

--$$$

create table crm_integration_anlt.tmp_pl_otomoto_calc_active_ads_per_category_core as
select
	cod_contact,
	cod_contact_parent,
	lower(email) email,
	kpi_custom_field.cod_custom_field,
	scai.dat_snap,
	core.cod_source_system,
	'' + cast(nvl(core.custom_field_value,0) as varchar) custom_field_value
from
	(
		select
			base_contact.cod_contact,
			base_contact.cod_contact_parent,
			base_contact.email,
			max(scai.dat_processing) dat_snap,
			base_contact.cod_source_system,
			count(ads.status) custom_field_value
		from
			crm_integration_anlt.t_lkp_atlas_user atlas_user,
			crm_integration_anlt.t_lkp_contact base_contact,
			db_atlas.olxpl_ads ads,
			crm_integration_anlt.t_rel_scai_country_integration scai
		where
			atlas_user.cod_source_system = 7
			and base_contact.cod_source_system = 12
			and lower(base_contact.email) = lower(atlas_user.dsc_atlas_user)
			and atlas_user.valid_to = 20991231
			and base_contact.valid_to = 20991231
			and scai.cod_integration = 50000 
			--and atlas_user.valid_from = scai.dat_processing
			and ads.user_id = atlas_user.cod_atlas_user
			and ads.status = 'active'
			and scai.cod_country = 1
		group by
			base_contact.cod_contact,
			base_contact.cod_contact_parent,
			base_contact.email,
			base_contact.cod_source_system
	) core,
	(select max(dat_processing) dat_snap from crm_integration_anlt.t_rel_scai_integration_process where cod_integration = 50000 and cod_country = 1) scai,
(
	select
		rel.cod_custom_field,
		rel.flg_active
	from
		crm_integration_anlt.t_lkp_kpi kpi,
		crm_integration_anlt.t_rel_kpi_custom_field rel
	where
		kpi.cod_kpi = rel.cod_kpi
		and lower(kpi.dsc_kpi) = '# active ads per category'
		and rel.cod_source_system = 12
) kpi_custom_field
where 1=1
	and kpi_custom_field.flg_active = 1;

--$$$

create table crm_integration_anlt.tmp_pl_all_calc_active_ads_per_category_final as
	select
		distinct source_otodom.cod_contact,
		source_otodom.cod_contact_parent,
		source_otodom.cod_custom_field,
		source_otodom.dat_snap,
		source_otodom.cod_source_system,
		'OTD: ' + source_otodom.custom_field_value + case when source_olx.cod_contact is not null then ' || OLX: ' + source_olx.custom_field_value else '' end custom_field_value
	from
		crm_integration_anlt.tmp_pl_otodom_calc_active_ads_per_category_core source_otodom,
		crm_integration_anlt.tmp_pl_olx_calc_active_ads_per_category_core source_olx 
	 where
		source_otodom.email = source_olx.email(+)

	union  all

	select
		distinct source_otomoto.cod_contact,
		source_otomoto.cod_contact_parent,
		source_otomoto.cod_custom_field,
		source_otomoto.dat_snap,
		source_otomoto.cod_source_system,
		'OTM: ' + source_otomoto.custom_field_value + case when source_olx.cod_contact is not null then ' || OLX: ' + source_olx.custom_field_value else '' end custom_field_value
	from
		crm_integration_anlt.tmp_pl_otomoto_calc_active_ads_per_category_core source_otomoto,
		crm_integration_anlt.tmp_pl_olx_calc_active_ads_per_category_core source_olx 
	 where
		source_otomoto.email = source_olx.email(+) 

	union all

	select
		distinct source_olx.cod_contact,
		source_olx.cod_contact_parent,
		source_olx.cod_custom_field,
		source_olx.dat_snap,
		source_olx.cod_source_system,
		'OLX: ' + source_olx.custom_field_value
		+ case
				when source_otodom.cod_contact is not null then ' || OTD: ' + source_otodom.custom_field_value
					else ''
			end
		+ case
				when source_otomoto.cod_contact is not null then ' || OTM: ' + source_otomoto.custom_field_value
					else ''
			end custom_field_value
	from
		crm_integration_anlt.tmp_pl_olx_calc_active_ads_per_category_core source_olx,
		crm_integration_anlt.tmp_pl_otodom_calc_active_ads_per_category_core source_otodom,
		crm_integration_anlt.tmp_pl_otomoto_calc_active_ads_per_category_core source_otomoto 
	 where
		source_olx.email = source_otodom.email(+) 
		;
		
--$$$

--Calculate for employees
create table crm_integration_anlt.tmp_pl_all_calc_active_ads_per_category_final_2 as
   select source.cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	source.custom_field_value
	from crm_integration_anlt.tmp_pl_all_calc_active_ads_per_category_final source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where 1 = 1
  and source.cod_contact_parent is not null
  and source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);

--$$$

--Calculate for companies and contacts not associated with companies
create table crm_integration_anlt.tmp_pl_all_calc_active_ads_per_category_final_3 as
  select source.cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system
	,source.custom_field_value
	from (
			select
		 cod_contact,
		 cod_custom_field,
		 dat_snap,
		 cod_source_system,
		 'OLX: ' || custom_field_value_olx  || ' || OTD: ' ||   sum(custom_field_value_otd) || ' || OTM: ' || sum(custom_field_value_otm) custom_field_value
	from (
			select
				distinct nvl(source_otodom.cod_contact_parent, source_otodom.cod_contact) cod_contact,
				source_otodom.cod_custom_field,
				source_otodom.dat_snap,
				source_otodom.cod_source_system,
				case when source_olx.cod_contact is not null then  cast(sum(source_olx.custom_field_value) as varchar) else '0' end custom_field_value_olx,
				cast(sum(source_otodom.custom_field_value) as varchar)  custom_field_value_otd,
			  null as custom_field_value_otm
			from
				crm_integration_anlt.tmp_pl_otodom_calc_active_ads_per_category_core source_otodom,
				crm_integration_anlt.tmp_pl_olx_calc_active_ads_per_category_core source_olx
			 where
				source_otodom.email = source_olx.email(+)
			group by
			  nvl(source_otodom.cod_contact_parent, source_otodom.cod_contact),
				source_olx.cod_contact,
			  source_otodom.cod_custom_field,
			  source_otodom.dat_snap,
			  source_otodom.cod_source_system


			union  all

			select
				distinct nvl(source_otomoto.cod_contact_parent, source_otomoto.cod_contact) cod_contact,
				source_otomoto.cod_custom_field,
				source_otomoto.dat_snap,
				source_otomoto.cod_source_system,
				case when source_olx.cod_contact is not null then   cast(sum(source_olx.custom_field_value) as varchar) else '0' end custom_field_value_olx,
				null as custom_field_value_otd,
				cast(sum(source_otomoto.custom_field_value) as varchar) custom_field_value
			from
				crm_integration_anlt.tmp_pl_otomoto_calc_active_ads_per_category_core source_otomoto,
				crm_integration_anlt.tmp_pl_olx_calc_active_ads_per_category_core source_olx
			 where
				source_otomoto.email = source_olx.email(+)
			  group by
			nvl(source_otomoto.cod_contact_parent, source_otomoto.cod_contact),
			source_olx.cod_contact,
			source_otomoto.cod_custom_field,
			source_otomoto.dat_snap,
			source_otomoto.cod_source_system

			union all

			select
				distinct nvl(source_olx.cod_contact_parent, source_olx.cod_contact) cod_contact,
				source_olx.cod_custom_field,
				source_olx.dat_snap,
				source_olx.cod_source_system,
				cast(sum(source_olx.custom_field_value) as varchar) custom_field_value_olx,
				case when source_otodom.cod_contact is not null then   cast(sum(source_otodom.custom_field_value) as varchar)	else '0'	end custom_field_value_old,
				case when source_otomoto.cod_contact is not null then   cast(sum(source_otomoto.custom_field_value) as varchar)	else '0'	end custom_field_value_otm
			from
				crm_integration_anlt.tmp_pl_olx_calc_active_ads_per_category_core source_olx,
				crm_integration_anlt.tmp_pl_otodom_calc_active_ads_per_category_core source_otodom,
				crm_integration_anlt.tmp_pl_otomoto_calc_active_ads_per_category_core source_otomoto
			 where
				source_olx.email = source_otodom.email(+)
			group by
			  nvl(source_olx.cod_contact_parent, source_olx.cod_contact),
				source_otomoto.cod_contact,
				source_otodom.cod_contact,
			  source_olx.cod_custom_field,
			  source_olx.dat_snap,
			  source_olx.cod_source_system
	  ) source
	 where 1=1
	group by cod_contact,
			 cod_custom_field,
			 dat_snap,
			 cod_source_system,
			 custom_field_value_olx
	  ) source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
	where 1 = 1
  and source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null)
	 ;		

--$$$

-- HST INSERT - KPI OLX.BASE.006 (# Active ads per category)
insert into crm_integration_anlt.t_hst_base_integration_snap
select *
from crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field)  in
			(select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_all_calc_active_ads_per_category_final_2
			union
			select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_all_calc_active_ads_per_category_final_3);

--$$$

-- SNAP DELETE - KPI OLX.BASE.006 (# Active ads per category)
delete from crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field)  in
			(select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_all_calc_active_ads_per_category_final_2
			union
			select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_all_calc_active_ads_per_category_final_3);
			
--$$$

--KPI OLX.BASE.006 (# Active ads per category)
insert into crm_integration_anlt.t_fac_base_integration_snap
  select
    *
  from
    (select * from crm_integration_anlt.tmp_pl_all_calc_active_ads_per_category_final_2
		union
		select * from crm_integration_anlt.tmp_pl_all_calc_active_ads_per_category_final_3);
		
--$$$

drop table crm_integration_anlt.tmp_pl_olx_calc_active_ads_per_category_core;
drop table crm_integration_anlt.tmp_pl_otodom_calc_active_ads_per_category_core;
drop table crm_integration_anlt.tmp_pl_otomoto_calc_active_ads_per_category_core;
drop table crm_integration_anlt.tmp_pl_all_calc_active_ads_per_category_final;
drop table crm_integration_anlt.tmp_pl_all_calc_active_ads_per_category_final_2;
drop table crm_integration_anlt.tmp_pl_all_calc_active_ads_per_category_final_3;

--$$$

-- CREATE TMP - KPI OLX.BASE.105 (User_ID)
create table crm_integration_anlt.tmp_pl_olx_calc_user_id as
	SELECT
	  a.cod_contact, 
	  kpi_custom_field.cod_custom_field,
	  scai.dat_processing dat_snap,
	  isnull(a.cod_source_system,13) cod_source_system,
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
		atlas_user.cod_source_system = 9
		AND base_contact.cod_source_system = 13
		AND lower(base_contact.email) = lower(atlas_user.dsc_atlas_user)
		AND atlas_user.valid_to = 20991231
		AND base_contact.valid_to = 20991231
		AND scai.cod_integration = 50000
		and scai.cod_country = 2
	  ) A, 
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
			  and rel.cod_source_system = 13
		) kpi_custom_field
	WHERE 1=1 
	  and scai.cod_integration = 50000
	  and kpi_custom_field.flg_active = 1
		and scai.cod_country = 2
		;

--$$$

-- HST INSERT - KPI OLX.BASE.105 (User_ID)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_user_id);

--$$$

-- SNAP DELETE - KPI OLX.BASE.105 (User_ID)
DELETE FROM crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_user_id);

--$$$

--KPI OLX.BASE.105 (User_ID)
insert into crm_integration_anlt.t_fac_base_integration_snap
	select
		*
	from
		crm_integration_anlt.tmp_pl_olx_calc_user_id;

--$$$

drop table if exists crm_integration_anlt.tmp_pl_olx_calc_user_id;

--$$$

-- CREATE TMP - KPI OLX.BASE.113 (# of ads expiring in next 5 DAYS)
create table crm_integration_anlt.tmp_pl_olx_calc_ads_expiring_5d_1 as
    select
      a.cod_contact,
	  a.cod_contact_parent,
      kpi_custom_field.cod_custom_field,
      scai.dat_processing dat_snap,
      coalesce(a.cod_source_system,13) cod_source_system,
      a.custom_field_value custom_field_value
    from
      (
        select
          source.cod_contact,
		  source.cod_contact_parent,
          source.dat_processing dat_snap,
          source.cod_source_system,
          cast(sum(nr_ads_expriring) as varchar) custom_field_value --nr_replies,
        from
          (
            select
							lkp_contact.cod_contact_parent,
              lkp_contact.cod_contact,
              scai.dat_processing,
              lkp_contact.cod_source_system,
              count(distinct ads.id) nr_ads_expriring
            from
              crm_integration_anlt.t_lkp_source_system lkp_source_system,
              db_atlas.olxpl_ads ads,
              crm_integration_anlt.t_lkp_atlas_user lkp_user,
              crm_integration_anlt.t_lkp_contact lkp_contact,
              crm_integration_anlt.t_rel_scai_country_integration scai
            where
              lkp_user.cod_source_system = 9
              and lkp_contact.cod_source_system = 13
              and lkp_user.cod_source_system = lkp_source_system.cod_source_system
              and ads.user_id = lkp_user.opr_atlas_user
              and lkp_user.valid_to = 20991231
			  			and lower(lkp_contact.email) = lower(lkp_user.dsc_atlas_user)
              and lkp_contact.valid_to = 20991231
              and scai.cod_integration = 50000
							and scai.cod_country = 2
              and ads.valid_to between sysdate and sysdate + 5
						group by
							lkp_contact.cod_contact_parent,
              lkp_contact.cod_contact,
              scai.dat_processing,
              lkp_contact.cod_source_system
          ) source
        group by
		  source.cod_contact,
          source.cod_source_system,
          source.cod_contact_parent,
          source.dat_processing
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
					and lower(kpi.dsc_kpi) = '# of ads expiring in next 5 days'
					and rel.cod_source_system = 13
			) kpi_custom_field
			where 1=1
				and scai.cod_integration = 50000
			and kpi_custom_field.flg_active = 1
			and scai.cod_country = 2
  ;
 
--$$$
 
--Calculate for employees
create table crm_integration_anlt.tmp_pl_olx_calc_ads_expiring_5d_2 as
   select source.cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	cast(coalesce(source.custom_field_value, '-') as varchar) custom_field_value
	from crm_integration_anlt.tmp_pl_olx_calc_ads_expiring_5d_1 source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where 1 = 1
  and source.cod_contact_parent is not null
  and source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);

--$$$

--Calculate for companies and contacts not associated with companies
create table crm_integration_anlt.tmp_pl_olx_calc_ads_expiring_5d_3 as
   select nvl(source.cod_contact_parent, source.cod_contact) as cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	coalesce(cast(sum(source.custom_field_value) as varchar), '-') custom_field_value
	from crm_integration_anlt.tmp_pl_olx_calc_ads_expiring_5d_1 source,
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

--$$$

-- HST INSERT - KPI OLX.BASE.113 (# of ads expiring in next 5 DAYS)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field)  in
			(select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_ads_expiring_5d_2
			union
			select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_ads_expiring_5d_3);

--$$$

-- SNAP DELETE - KPI OLX.BASE.113 (# of ads expiring in next 5 DAYS)
delete from crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field)  in
			(select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_ads_expiring_5d_2
			union
			select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_ads_expiring_5d_3);

--$$$

-- KPI OLX.BASE.113 (# of ads expiring in next 5 DAYS)
insert into crm_integration_anlt.t_fac_base_integration_snap
  select
    *
  from
    (select * from crm_integration_anlt.tmp_pl_olx_calc_ads_expiring_5d_2
		union
		select * from crm_integration_anlt.tmp_pl_olx_calc_ads_expiring_5d_3);

--$$$

drop table if exists crm_integration_anlt.tmp_pl_olx_calc_ads_expiring_5d_1;
drop table if exists crm_integration_anlt.tmp_pl_olx_calc_ads_expiring_5d_2;
drop table if exists crm_integration_anlt.tmp_pl_olx_calc_ads_expiring_5d_3;

--$$$

--(Chat response %)
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
		  and lower(kpi.dsc_kpi) = 'chat response %'
		  and rel.cod_source_system = 13
	) kpi_custom_field
  WHERE
    base_contact.cod_source_system = 13
    AND base_contact.valid_to = 20991231
    AND scai.cod_integration = 50000
	and kpi_custom_field.flg_active = 1
	and scai.cod_country = 2
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

--(Scheduled package) (NOT IN t_rel_kpi_custom_field YET)
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
		  and rel.cod_source_system = 13
	) kpi_custom_field
  WHERE
    base_contact.cod_source_system = 13
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

-- CREATE TMP - KPI OLX.BASE.114 (Permission to email/call / marketing_email)
create table crm_integration_anlt.tmp_pl_olx_calc_marketing_email as
select
	source.cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	source.custom_field_value
from
	(
select
	a.cod_contact,
	kpi_custom_field.cod_custom_field,
	scai.dat_processing dat_snap,
	isnull(a.cod_source_system,13) cod_source_system,
	coalesce(case when a.custom_field_value = '1' then 'Yes'
		 when a.custom_field_value = '0' then 'No'
		 else a.custom_field_value end ,'-') custom_field_value
from
	(
select
					*
				from
					(
						select
							base_contact.cod_contact,
							scai.dat_processing                                        dat_snap,
							base_contact.cod_source_system,
							cast(user_notifications.marketing_email as varchar) custom_field_value,
							row_number()
							over (
								partition by cod_contact
								order by coalesce(atlas_user.created_at, '1900-01-01') ) rn
						from db_atlas.olxpl_users_notifications user_notifications,
							crm_integration_anlt.t_lkp_atlas_user atlas_user,
							crm_integration_anlt.t_lkp_contact base_contact,
							crm_integration_anlt.t_rel_scai_country_integration scai
						where 1 = 1
									and user_notifications.id = atlas_user.opr_atlas_user
									and lower(base_contact.email) = lower(atlas_user.dsc_atlas_user)
									and atlas_user.cod_source_system = 9
									and base_contact.cod_source_system = 13
									and atlas_user.valid_to = 20991231
									and base_contact.valid_to = 20991231
									and scai.cod_integration = 50000
									and scai.cod_country = 2
					)
	WHERE 1=1
	and rn = 1) a,
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
			  and lower(kpi.dsc_kpi) = 'marketing email'
			  and rel.cod_source_system = 13
		) kpi_custom_field
    where 1=1
	  and scai.cod_integration = 50000
    and scai.cod_country = 2
	  and kpi_custom_field.flg_active = 1 )  source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null)
  ;

--$$$

-- HST INSERT - KPI OLX.BASE.114 (Permission to email/call)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_marketing_email);

--$$$

-- SNAP DELETE - KPI OLX.BASE.114 (Permission to email/call)
DELETE FROM crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_marketing_email);

--$$$

--KPI OLX.BASE.114 (Permission to email/call) 
insert into crm_integration_anlt.t_fac_base_integration_snap
	SELECT
		*
	from
		crm_integration_anlt.tmp_pl_olx_calc_marketing_email;

--$$$

drop table if exists crm_integration_anlt.tmp_pl_olx_calc_marketing_email;

--$$$ 

-- CREATE TMP - KPI OLX.BASE.117 KPI OLX.BASE.118 KPI OLX.BASE.119 (# of calls 0 / -1 / -2)
create table crm_integration_anlt.tmp_pl_olx_calc_number_calls_1 as
	select
	  base_contact.cod_contact,
	  base_contact.cod_contact_parent,
	  to_char(call.created_at,'YYYYMM') as custom_field_value,
	  scai.dat_processing dat_snap,
	  coalesce(base_contact.cod_source_system,13) cod_source_system
	from
	  crm_integration_anlt.t_fac_call call,
	  crm_integration_anlt.t_lkp_contact base_contact,
	  crm_integration_anlt.t_rel_scai_country_integration scai
	where 1=1
	  and call.cod_source_system = 13
	  and call.cod_source_system = base_contact.cod_source_system
	  and base_contact.valid_to = 20991231
	  and scai.cod_integration = 50000
		and scai.cod_country = 2
	  and call.cod_contact = base_contact.cod_contact
	  and call.created_at > sysdate - 90
	  ;

--$$$
	  
--KPI OLX.BASE.117 (# of calls 0)  	  
create table crm_integration_anlt.tmp_pl_olx_calc_number_calls_2 as
    select
      a.cod_contact,
	  a.cod_contact_parent,
      kpi_custom_field.cod_custom_field,
      scai.dat_processing dat_snap,
      coalesce(a.cod_source_system,13) cod_source_system,
      a.custom_field_value custom_field_value
    from
    (	select
		 cod_contact,
		 cod_contact_parent,
		 count(custom_field_value) custom_field_value,
		 dat_snap,
		 cod_source_system
		from  crm_integration_anlt.tmp_pl_olx_calc_number_calls_1
		where 1=1
		and custom_field_value = to_char(sysdate, 'YYYYMM')
		group by cod_contact,
		 cod_contact_parent,
		 dat_snap,
		 cod_source_system ) a,
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
			and lower(kpi.dsc_kpi) = '# of calls (0)'
			and rel.cod_source_system = 13
	) kpi_custom_field
	where 1=1 
	and scai.cod_integration = 50000
	and kpi_custom_field.flg_active = 1
	and scai.cod_country = 2
  ;

--$$$

--Calculate for employees
create table crm_integration_anlt.tmp_pl_olx_calc_number_calls_3 as
   select source.cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	cast(coalesce(source.custom_field_value, '0') as varchar) custom_field_value
	from crm_integration_anlt.tmp_pl_olx_calc_number_calls_2 source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where 1 = 1
  and source.cod_contact_parent is not null
  and source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);

--$$$

--Calculate for companies and contacts not associated with companies
create table crm_integration_anlt.tmp_pl_olx_calc_number_calls_4 as
   select nvl(source.cod_contact_parent, source.cod_contact) as cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	coalesce(cast(sum(source.custom_field_value) as varchar), '0') custom_field_value
	from crm_integration_anlt.tmp_pl_olx_calc_number_calls_2 source,
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

--$$$

-- HST INSERT - KPI OLX.BASE.117 (# of calls 0)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field)  in
			(select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_number_calls_3
			union
			select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_number_calls_4);

--$$$

-- SNAP DELETE - KPI OLX.BASE.117 (# of calls 0)
delete from crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field)  in
			(select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_number_calls_3
			union
			select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_number_calls_4);

--$$$

-- KPI OLX.BASE.117 (# of calls 0)
insert into crm_integration_anlt.t_fac_base_integration_snap
  select
    *
  from
    (select * from crm_integration_anlt.tmp_pl_olx_calc_number_calls_3
		union
		select * from crm_integration_anlt.tmp_pl_olx_calc_number_calls_4);

--$$$

drop table if exists crm_integration_anlt.tmp_pl_olx_calc_number_calls_2;
drop table if exists crm_integration_anlt.tmp_pl_olx_calc_number_calls_3;
drop table if exists crm_integration_anlt.tmp_pl_olx_calc_number_calls_4;

--$$$

--KPI OLX.BASE.118 (# of calls -1)  	  
create table crm_integration_anlt.tmp_pl_olx_calc_number_calls_2 as
    select
      a.cod_contact,
	  a.cod_contact_parent,
      kpi_custom_field.cod_custom_field,
      scai.dat_processing dat_snap,
      coalesce(a.cod_source_system,13) cod_source_system,
      a.custom_field_value custom_field_value
    from
    (	select
		 cod_contact,
		 cod_contact_parent,
		 count(custom_field_value) custom_field_value,
		 dat_snap,
		 cod_source_system
		from  crm_integration_anlt.tmp_pl_olx_calc_number_calls_1
		where 1=1
		and custom_field_value = to_char(dateadd(month,-1,sysdate), 'YYYYMM')
		group by cod_contact,
		 cod_contact_parent,
		 dat_snap,
		 cod_source_system ) a,
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
			and lower(kpi.dsc_kpi) = '# of calls (-1)'
			and rel.cod_source_system = 13
	) kpi_custom_field
	where 1=1 
	and scai.cod_integration = 50000
	and kpi_custom_field.flg_active = 1
	and scai.cod_country = 2
  ;

--$$$

--Calculate for employees
create table crm_integration_anlt.tmp_pl_olx_calc_number_calls_3 as
   select source.cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	cast(coalesce(source.custom_field_value, '0') as varchar) custom_field_value
	from crm_integration_anlt.tmp_pl_olx_calc_number_calls_2 source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where 1 = 1
  and source.cod_contact_parent is not null
  and source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);

--$$$

--Calculate for companies and contacts not associated with companies
create table crm_integration_anlt.tmp_pl_olx_calc_number_calls_4 as
   select nvl(source.cod_contact_parent, source.cod_contact) as cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	coalesce(cast(sum(source.custom_field_value) as varchar), '0') custom_field_value
	from crm_integration_anlt.tmp_pl_olx_calc_number_calls_2 source,
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

--$$$
	 
-- HST INSERT - KPI OLX.BASE.118 (# of calls -1)  
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field)  in
			(select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_number_calls_3
			union
			select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_number_calls_4);

--$$$

-- SNAP DELETE - KPI OLX.BASE.118 (# of calls -1)  
delete from crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field)  in
			(select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_number_calls_3
			union
			select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_number_calls_4);

--$$$

-- KPI OLX.BASE.118 (# of calls -1)  
insert into crm_integration_anlt.t_fac_base_integration_snap
  select
    *
  from
    (select * from crm_integration_anlt.tmp_pl_olx_calc_number_calls_3
		union
		select * from crm_integration_anlt.tmp_pl_olx_calc_number_calls_4);

--$$$

drop table if exists crm_integration_anlt.tmp_pl_olx_calc_number_calls_2;
drop table if exists crm_integration_anlt.tmp_pl_olx_calc_number_calls_3;
drop table if exists crm_integration_anlt.tmp_pl_olx_calc_number_calls_4;

--$$$

--KPI OLX.BASE.119 (# of calls -2)  	  
create table crm_integration_anlt.tmp_pl_olx_calc_number_calls_2 as
    select
      a.cod_contact,
	  a.cod_contact_parent,
      kpi_custom_field.cod_custom_field,
      scai.dat_processing dat_snap,
      coalesce(a.cod_source_system,13) cod_source_system,
      a.custom_field_value custom_field_value
    from
    (	select
		 cod_contact,
		 cod_contact_parent,
		 count(custom_field_value) custom_field_value,
		 dat_snap,
		 cod_source_system
		from  crm_integration_anlt.tmp_pl_olx_calc_number_calls_1
		where 1=1
		and custom_field_value = to_char(dateadd(month,-2,sysdate), 'YYYYMM')
		group by cod_contact,
		 cod_contact_parent,
		 dat_snap,
		 cod_source_system ) a,
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
			and lower(kpi.dsc_kpi) = '# of calls (-2)'
			and rel.cod_source_system = 13
	) kpi_custom_field
	where 1=1 
	and scai.cod_integration = 50000
	and kpi_custom_field.flg_active = 1
	and scai.cod_country = 2
  ;

--$$$
  
--Calculate for employees
create table crm_integration_anlt.tmp_pl_olx_calc_number_calls_3 as
   select source.cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	cast(coalesce(source.custom_field_value, '0') as varchar) custom_field_value
	from crm_integration_anlt.tmp_pl_olx_calc_number_calls_2 source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where 1 = 1
  and source.cod_contact_parent is not null
  and source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);

--$$$

--Calculate for companies and contacts not associated with companies
create table crm_integration_anlt.tmp_pl_olx_calc_number_calls_4 as
   select nvl(source.cod_contact_parent, source.cod_contact) as cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	coalesce(cast(sum(source.custom_field_value) as varchar), '0') custom_field_value
	from crm_integration_anlt.tmp_pl_olx_calc_number_calls_2 source,
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

--$$$

-- HST INSERT - KPI OLX.BASE.119 (# of calls -2)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field)  in
			(select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_number_calls_3
			union
			select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_number_calls_4);

--$$$

-- SNAP DELETE - KPI OLX.BASE.119 (# of calls -2)
delete from crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field)  in
			(select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_number_calls_3
			union
			select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_olx_calc_number_calls_4);

--$$$

-- KPI OLX.BASE.119 (# of calls -2)
insert into crm_integration_anlt.t_fac_base_integration_snap
  select
    *
  from
    (select * from crm_integration_anlt.tmp_pl_olx_calc_number_calls_3
		union
		select * from crm_integration_anlt.tmp_pl_olx_calc_number_calls_4);

--$$$
		
drop table if exists crm_integration_anlt.tmp_pl_olx_calc_number_calls_2;
drop table if exists crm_integration_anlt.tmp_pl_olx_calc_number_calls_3;
drop table if exists crm_integration_anlt.tmp_pl_olx_calc_number_calls_4;
drop table if exists crm_integration_anlt.tmp_pl_olx_calc_number_calls_1;

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
	and proc.dsc_process_short = 't_fac_base_integration_snap_plhorizontal';

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
and proc.dsc_process_short = 't_fac_base_integration_snap_plhorizontal'
and t_rel_scai_integration_process.ind_active = 1;


/*
delete from crm_integration_anlt.t_fac_base_integration_snap
where cod_source_system = 13
and cod_contact not in (211643575,
213429447,
216566553,
219430575,
203709531,
203708750, 
203706164,
203706013, 
203697399,
203696675,
203696041,
203695854,
203695759,
203695638, 
203695249,
203692346,
203691072,
203691044,
203628279, 
203627224, 
203627007,
203626973,
203622080,
203620470,
203620044,
203620044,
203619985,
203618715,
203618459,
203617265,
203617098,
203615578,
203615171,
203613884,
203611769,
203611490,
203610109,
203609936,
203609915,
203608744,
203608744,
203608691,
203608619,
203608602,
203550708, 
203546719,
203545050,
203544678,
203543011,
203540725,
203540362,
203540350,
203540052, 
203539407,
203535663,
203535663,
203535517,
203535442,
203535337,
203491162,
203490886,
203489416,
203489347,
203489347,
203488901,
203487048,
203486700,
203486358,
203486298,
203486141,
203486057,
203485685,
203485097,
203484769,  
203483562, 
203482028,
203481762,
203481737,
203481201,
203481126,
203481101, 
203481045,
203477165, 
203476598,
203476533, 
203475955
);*/