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
create table crm_integration_anlt.tmp_pl_otomoto_calc_last_login as
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
							row_number() over (partition by cod_contact_parent order by coalesce(atlas_user.last_login_at,'1900-01-01') desc) rn
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
					)
			  where
					rn = 1
		  ) a,
			(select * from crm_integration_anlt.t_lkp_contact where valid_to = 20991231 and cod_source_system = 12 and cod_contact_parent is null) b,
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
		  b.cod_contact = a.cod_contact_parent (+)
		  and scai.cod_integration = 50000
		  and kpi_custom_field.flg_active = 1
			and scai.cod_country = 2
	) source,
    crm_integration_anlt.t_fac_base_integration_snap fac_snap
where source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);

-- HST INSERT - KPI OLX.BASE.085 (Last login)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_otomoto_calc_last_login);

-- SNAP DELETE - KPI OLX.BASE.085 (Last login)
DELETE FROM crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_otomoto_calc_last_login);

--KPI OLX.BASE.085 (Last login)
insert into crm_integration_anlt.t_fac_base_integration_snap
	select
		*
	from
		crm_integration_anlt.tmp_pl_otomoto_calc_last_login;

drop table if exists crm_integration_anlt.tmp_pl_otomoto_calc_last_login;

--$$$

-- CREATE TMP - KPI OLX.BASE.031 (Created date)
create table crm_integration_anlt.tmp_pl_otomoto_calc_created_date as
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
							row_number() over (partition by cod_contact_parent order by coalesce(atlas_user.created_at,'1900-01-01')) rn
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
			(select * from crm_integration_anlt.t_lkp_contact where valid_to = 20991231 and cod_source_system = 12 and cod_contact_parent is null) b,
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
		  b.cod_contact = a.cod_contact_parent (+)
		  and scai.cod_integration = 50000
		  and kpi_custom_field.flg_active = 1
			and scai.cod_country = 2
	) source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);

-- HST INSERT - KPI OLX.BASE.031 (Created date)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_otomoto_calc_created_date);

-- SNAP DELETE - KPI OLX.BASE.031 (Created date)
DELETE FROM crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_otomoto_calc_created_date);

--KPI OLX.BASE.031 (Created date)
insert into crm_integration_anlt.t_fac_base_integration_snap
	select
		*
	from
		crm_integration_anlt.tmp_pl_otomoto_calc_created_date;

drop table if exists crm_integration_anlt.tmp_pl_otomoto_calc_created_date;

--$$$

-- CREATE TMP - KPI OLX.BASE.086 (# Logins last 30 days)
create table crm_integration_anlt.tmp_pl_otomoto_calc_logins_last_30_days as
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
					cod_contact_parent,
					dat_snap,
					cod_source_system,
					count(distinct server_date_day) custom_field_value
				from
					(
						select
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
							--and trackname like 'login%'
						group by
							base.cod_contact_parent,
							dat_snap,
							cod_source_system,
							web.server_date_day

						union all

						select
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
							--and trackname like 'login%'
						group by
							base.cod_contact_parent,
							ios.server_date_day,
							dat_snap,
							cod_source_system

						union all

						select
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
							--and trackname like 'login%'
						group by
							base.cod_contact_parent,
							android.server_date_day,
							dat_snap,
							cod_source_system
					) core
				group by
					cod_contact_parent,
					dat_snap,
					cod_source_system
			) a,
			(select * from crm_integration_anlt.t_lkp_contact where valid_to = 20991231 and cod_source_system = 12 and cod_contact_parent is null) B,
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
			b.cod_contact = a.cod_contact_parent (+)
			and scai.cod_integration = 50000
			and kpi_custom_field.flg_active = 1
			and scai.cod_country = 2
	) source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);

-- HST INSERT - KPI OLX.BASE.086 (# Logins last 30 days)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_otomoto_calc_logins_last_30_days);

-- SNAP DELETE - KPI OLX.BASE.086 (# Logins last 30 days)
DELETE FROM crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_otomoto_calc_logins_last_30_days);

--KPI OLX.BASE.086 (# Logins last 30 days)
insert into crm_integration_anlt.t_fac_base_integration_snap
	select
		*
	from
		crm_integration_anlt.tmp_pl_otomoto_calc_logins_last_30_days;

drop table if exists crm_integration_anlt.tmp_pl_otomoto_calc_logins_last_30_days;

--$$$

-- CREATE TMP - KPI OLX.BASE.012 (Last package purchased)
create table crm_integration_anlt.tmp_pl_otomoto_calc_last_package_purchased as
select
	source.cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	source.custom_field_value
from
	(
		select
		  cod_contact,
		  kpi_custom_field.cod_custom_field,
		  scai.dat_processing dat_snap,
		  contact.cod_source_system cod_source_system,
		  coalesce(core.custom_field_value, ' ') custom_field_value
		from
		  (
				select
					coalesce(dsc_atlas_user,'unknown') dsc_atlas_user,
					dat_snap,
					custom_field_value
				from
					(
						select
							dsc_atlas_user,
							inner_core.dat_snap,
							inner_core.custom_field_value,
							row_number() over (partition by dsc_atlas_user order by inner_core.date desc, inner_core.id desc) rn
						from
							(
								select
									atlas_user.dsc_atlas_user,
									idx_type.dsc_index_type,
									scai.dat_processing dat_snap,
									fac.name custom_field_value,
									fac.date,
									fac.id
								from
									crm_integration_anlt.t_lkp_atlas_user atlas_user,
									db_atlas_verticals.paidads_user_payments fac,
									crm_integration_anlt.t_rel_scai_country_integration scai,
									crm_integration_anlt.v_lkp_paidad_index idx,
									crm_integration_anlt.v_lkp_paidad_index_type idx_type 
								where
									atlas_user.cod_source_system = 7
									and atlas_user.valid_to = 20991231
									and scai.cod_integration = 50000
									and atlas_user.cod_atlas_user = fac.id_user (+)
									and fac.id_index = idx.opr_paidad_index (+)
									and 7 = idx.cod_source_system (+)
									and idx.cod_index_type = idx_type.cod_index_type(+) 
									and lower(fac.payment_provider) != 'admin'
									and lower(idx_type.dsc_index_type) = 'package'
									and fac.livesync_dbname = 'otomotopl'
									and scai.cod_country = 2
							) inner_core
					)
				where
					rn = 1
		  ) core,
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
				  and rel.cod_source_system = 12
			) kpi_custom_field,
			(
				select
					cod_contact,
					coalesce(email,'unknown') email,
					cod_source_system
				from
					crm_integration_anlt.t_lkp_contact
				where
					valid_to = 20991231
					and cod_source_system = 12
			) contact
	where
	  scai.cod_integration = 50000
	  and kpi_custom_field.flg_active = 1
		and lower(contact.email) = lower(dsc_atlas_user (+))
		and scai.cod_country = 2
	) source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);

-- HST INSERT - KPI OLX.BASE.012 (Last package purchased)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_otomoto_calc_last_package_purchased);

-- SNAP DELETE - KPI OLX.BASE.012 (Last package purchased)
DELETE FROM crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_otomoto_calc_last_package_purchased);

--KPI OLX.BASE.012 (Last package purchased)
/*XXXXX: Como identificar um package?*/
insert into crm_integration_anlt.t_fac_base_integration_snap
	SELECT
		*
	from
		crm_integration_anlt.tmp_pl_otomoto_calc_last_package_purchased;

drop table if exists crm_integration_anlt.tmp_pl_otomoto_calc_last_package_purchased;

--$$$

-- CREATE TMP - KPI OLX.BASE.091 (Wallet)
create table crm_integration_anlt.tmp_pl_otomoto_calc_wallet as
select
 core.cod_contact,
 core.cod_custom_field,
 core.dat_snap,
 core.cod_source_system,
 core.custom_field_value
from
  (
    select
      cod_contact,
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
      and scai.cod_country = 2
  ) core,
 crm_integration_anlt.t_fac_base_integration_snap fac_snap
where
 core.cod_source_system = fac_snap.cod_source_system (+)
 and core.cod_custom_field = fac_snap.cod_custom_field (+)
 and core.cod_contact = fac_snap.cod_contact (+)
 and (core.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);

-- HST INSERT - KPI OLX.BASE.091 (Wallet)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_otomoto_calc_wallet);

-- SNAP DELETE - KPI OLX.BASE.091 (Wallet)
delete from crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_otomoto_calc_wallet);

-- KPI OLX.BASE.091 (Wallet)
insert into crm_integration_anlt.t_fac_base_integration_snap
  select
    *
  from
    crm_integration_anlt.tmp_pl_otomoto_calc_wallet;

drop table if exists crm_integration_anlt.tmp_pl_otomoto_calc_wallet;

--$$$ -- 10

-- CREATE TMP - KPI OLX.BASE.023 (# Replies)
create table crm_integration_anlt.tmp_pl_otomoto_calc_replies as
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
      coalesce(a.cod_source_system,12) cod_source_system,
      coalesce(a.custom_field_value, '-') custom_field_value
    from
      (
        select
          source.cod_contact_parent,
          source.dat_processing dat_snap,
          source.cod_source_system,
          cast(sum(nr_replies) as varchar) custom_field_value --nr_replies,
        from
          (
            select
							lkp_contact.cod_contact_parent,
              --lkp_contact.cod_contact,
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
              --lkp_contact.cod_contact,
              scai.dat_processing,
              lkp_contact.cod_source_system,
              ads.id
          ) source
        group by
          source.cod_source_system,
          source.cod_contact_parent,
          source.dat_processing
      ) a,
			(select * from crm_integration_anlt.t_lkp_contact where valid_to = 20991231 and cod_source_system = 12 and cod_contact_parent is null) b,
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
				b.cod_contact = a.cod_contact_parent(+)
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

-- HST INSERT - KPI OLX.BASE.023 (# Replies)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_otomoto_calc_replies);

--$$$

-- SNAP DELETE - KPI OLX.BASE.023 (# Replies)
delete from crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_otomoto_calc_replies);

-- OLX.BASE.023 (# Replies)
insert into crm_integration_anlt.t_fac_base_integration_snap
  select
    *
  from
    crm_integration_anlt.tmp_pl_otomoto_calc_replies;

drop table if exists crm_integration_anlt.tmp_pl_otomoto_calc_replies;

--$$$

-- CREATE TMP - KPI OLX.BASE.081 (# Replies per Ad)
create table crm_integration_anlt.tmp_pl_otomoto_calc_replies_per_ad as
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
      coalesce(a.cod_source_system,12) cod_source_system,
      coalesce(a.custom_field_value, '-') custom_field_value
    from
      (
        select
          source.cod_contact_parent,
          source.dat_processing dat_snap,
          source.cod_source_system,
          cast(sum(nr_replies) / count(distinct source.id) as varchar) custom_field_value --nr_replies_per_ad,
        from
          (
            select
							lkp_contact.cod_contact_parent,
              --lkp_contact.cod_contact,
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
              --lkp_contact.cod_contact,
              scai.dat_processing,
              lkp_contact.cod_source_system,
              ads.id
          ) source
        group by
          source.cod_source_system,
          source.cod_contact_parent,
          source.dat_processing
      ) a,
			(select * from crm_integration_anlt.t_lkp_contact where valid_to = 20991231 and cod_source_system = 12 and cod_contact_parent is null) b,
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
				b.cod_contact = a.cod_contact_parent(+)
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


-- HST INSERT - KPI OLX.BASE.081 (# Replies per Ad)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_otomoto_calc_replies_per_ad);

-- SNAP DELETE - KPI OLX.BASE.081 (# Replies per Ad)
DELETE FROM crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_otomoto_calc_replies_per_ad);

-- OLX.BASE.081 (# Replies per Ad)
insert into crm_integration_anlt.t_fac_base_integration_snap
  select
    *
  from
    crm_integration_anlt.tmp_pl_otomoto_calc_replies_per_ad;

drop table if exists crm_integration_anlt.tmp_pl_otomoto_calc_replies_per_ad;

--$$$

-- CREATE TMP - KPI OLX.BASE.082 (# Ads with replies)
create table crm_integration_anlt.tmp_pl_otomoto_calc_ads_with_replies as
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
      coalesce(a.cod_source_system,12) cod_source_system,
      coalesce(a.custom_field_value, '-') custom_field_value
    from
      (
        select
          source.cod_contact_parent,
          source.dat_processing dat_snap,
          source.cod_source_system,
          cast(count(distinct source.id) as varchar) custom_field_value --nr_replies_per_ad,
        from
          (
            select
							lkp_contact.cod_contact_parent,
              --lkp_contact.cod_contact,
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
              --lkp_contact.cod_contact,
              scai.dat_processing,
              lkp_contact.cod_source_system,
              ads.id
          ) source
        group by
          source.cod_source_system,
          source.cod_contact_parent,
          source.dat_processing
      ) a,
			(select * from crm_integration_anlt.t_lkp_contact where valid_to = 20991231 and cod_source_system = 12 and cod_contact_parent is null) b,
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
				b.cod_contact = a.cod_contact_parent(+)
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

-- HST INSERT - KPI OLX.BASE.082 (# Ads with replies)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_otomoto_calc_ads_with_replies);

-- SNAP DELETE - KPI OLX.BASE.082 (# Ads with replies)
delete from crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_otomoto_calc_ads_with_replies);

 -- OLX.BASE.082 (# Ads with replies)
insert into crm_integration_anlt.t_fac_base_integration_snap
  select
      *
  from
    crm_integration_anlt.tmp_pl_otomoto_calc_ads_with_replies;

drop table if exists crm_integration_anlt.tmp_pl_otomoto_calc_ads_with_replies;

--$$$

-- CREATE TMP - KPI OLX.BASE.084 (# Views)
create table crm_integration_anlt.tmp_pl_otomoto_calc_views as
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
      isnull(a.custom_field_value, '-') custom_field_value
    from
      (
				select
					cod_contact_parent,
					dat_snap,
					cod_source_system,
					cast(sum(nbr_views) as varchar) custom_field_value
				from
					(
						select
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
							) base
						where
							web.server_date_day >= dateadd(day,-30,sysdate)
							and web.country_code = 'PL'
							and web.stream = 'v-otomoto-web'
							and web.user_id = base.opr_atlas_user
							and trackname = 'ad_page'
						group by
							base.cod_contact_parent,
							dat_snap,
							cod_source_system,
							web.server_date_day

						union all

						select
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
							and trackname = 'ad_page'
						group by
							base.cod_contact_parent,
							ios.server_date_day,
							dat_snap,
							cod_source_system

						union all

						select
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
							and trackname = 'ad_page'
						group by
							base.cod_contact_parent,
							android.server_date_day,
							dat_snap,
							cod_source_system
					) core
				group by
					cod_contact_parent,
					dat_snap,
					cod_source_system
      ) a,
      (select * from crm_integration_anlt.t_lkp_contact where valid_to = 20991231 and cod_source_system = 12 and cod_contact_parent is null) B,
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
      b.cod_contact = a.cod_contact_parent (+)
      and scai.cod_integration = 50000
	  and kpi_custom_field.flg_active = 1
	  and scai.cod_country = 2
  ) source,
  crm_integration_anlt.t_fac_base_integration_snap fac_snap
where source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);

-- HST INSERT - KPI OLX.BASE.084 (# Views)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_otomoto_calc_views);

-- SNAP DELETE - KPI OLX.BASE.084 (# Views)
delete from crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_otomoto_calc_views);

-- OLX.BASE.084 (# Views)
insert into crm_integration_anlt.t_fac_base_integration_snap
  select
    *
  from
    crm_integration_anlt.tmp_pl_otomoto_calc_views;

drop table if exists crm_integration_anlt.tmp_pl_otomoto_calc_views;

--$$$

-- CREATE TEMPORARY TABLE - KPI OLX.BASE.088 (Active package expiry date)
create table crm_integration_anlt.tmp_pl_otomoto_calc_active_package_expiry_date as
select
	source.cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	source.custom_field_value
from
	(
		select
		  cod_contact,
		  kpi_custom_field.cod_custom_field,
		  scai.dat_processing dat_snap,
		  contact.cod_source_system cod_source_system,
		  coalesce(core.custom_field_value, '1900-01-01 00:00:00') custom_field_value
		from
		  (
        select
          coalesce(dsc_atlas_user,'unknown') dsc_atlas_user,
          inner_core.dat_snap,
          cast(max(paidads_valid_to) as varchar) custom_field_value
        from
          (
            select
              atlas_user.dsc_atlas_user,
              idx_type.dsc_index_type,
              scai.dat_processing dat_snap,
              fac.paidads_valid_to,
              fac.date
            from
              crm_integration_anlt.t_lkp_atlas_user atlas_user,
              db_atlas_verticals.paidads_user_payments fac,
              crm_integration_anlt.t_rel_scai_country_integration scai,
              crm_integration_anlt.v_lkp_paidad_index idx,
              crm_integration_anlt.v_lkp_paidad_index_type idx_type
            where
              atlas_user.cod_source_system = 7
              and atlas_user.valid_to = 20991231
              and scai.cod_integration = 50000
              and atlas_user.cod_atlas_user = fac.id_user (+)
              and fac.id_index = idx.opr_paidad_index (+)
              and 7 = idx.cod_source_system (+)
			  and fac.livesync_dbname = 'otomotopl'
              and idx.cod_index_type = idx_type.cod_index_type(+)
              and lower(idx_type.dsc_index_type) = 'package'
							and scai.cod_country = 2
					) inner_core
        group by
          coalesce(dsc_atlas_user,'unknown'),
          inner_core.dat_snap
		  ) core,
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
				  and lower(kpi.dsc_kpi) = 'active package expiry date'
				  and rel.cod_source_system = 12
			) kpi_custom_field,
			(
				select
					cod_contact,
					coalesce(email,'unknown') email,
					cod_source_system
				from
					crm_integration_anlt.t_lkp_contact
				where
					valid_to = 20991231
					and cod_source_system = 12
			) contact
	where
	  scai.cod_integration = 50000
	  and kpi_custom_field.flg_active = 1
		and lower(contact.email) = lower(dsc_atlas_user (+))
		and scai.cod_country = 2
	) source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);

-- HST INSERT - KPI OLX.BASE.088 (Active package expiry date)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_otomoto_calc_active_package_expiry_date);

-- SNAP DELETE - KPI OLX.BASE.088 (Active package expiry date)
delete from crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in
  (select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_otomoto_calc_active_package_expiry_date);

--KPI OLX.BASE.088 (Active package expiry date)
insert into crm_integration_anlt.t_fac_base_integration_snap
  select
    *
  from
    crm_integration_anlt.tmp_pl_otomoto_calc_active_package_expiry_date;
-- 8106 Active package expiry date

drop table if exists crm_integration_anlt.tmp_pl_otomoto_calc_active_package_expiry_date;

--$$$

-- CREATE TMP - KPI OLX.BASE.014 (Max days since last call)
create table crm_integration_anlt.tmp_pl_otomoto_calc_max_days_since_last_call as
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
      isnull(a.custom_field_value, '-') custom_field_value
    from
      (
				select
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
							lkp_contact.cod_source_system = 12
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
					cod_contact_parent,
					dat_snap,
					cod_source_system
      ) a,
      (select * from crm_integration_anlt.t_lkp_contact where valid_to = 20991231 and cod_source_system = 12 and cod_contact_parent is null) B,
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
    where
      b.cod_contact = a.cod_contact_parent (+)
			and scai.cod_integration = 50000
      and scai.cod_country = 2
	  and kpi_custom_field.flg_active = 1
  ) source,
  crm_integration_anlt.t_fac_base_integration_snap fac_snap
where
  source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);

-- HST INSERT - KPI OLX.BASE.014 (Max days since last call)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_otomoto_calc_max_days_since_last_call);

-- SNAP DELETE - KPI OLX.BASE.014 (Max days since last call)
delete from crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_otomoto_calc_max_days_since_last_call);

--KPI OLX.BASE.014 (Max days since last call)
insert into crm_integration_anlt.t_fac_base_integration_snap
  select
    *
  from
    crm_integration_anlt.tmp_pl_otomoto_calc_max_days_since_last_call;

drop table if exists crm_integration_anlt.tmp_pl_otomoto_calc_max_days_since_last_call;

-- CREATE TMP - KPI OLX.BASE.XYZ (Max Value Package)
create table crm_integration_anlt.tmp_pl_otomoto_calc_max_value_package as
SELECT
	source.cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	source.custom_field_value
FROM
	(
	SELECT
	  b.cod_contact,
	  kpi_custom_field.cod_custom_field,
	  scai.dat_processing dat_snap,
	  coalesce(a.cod_source_system,12) cod_source_system,
	  coalesce(a.custom_field_value, '') custom_field_value
	FROM
		(
		SELECT
			--fac.dat_payment,
			base_contact.cod_contact,
			scai.dat_processing dat_snap,
			base_contact.cod_source_system,
			cast(min(payments.price) as varchar) custom_field_value
		FROM
			db_atlas_verticals.paidads_user_payments payments,
			db_atlas_verticals.payment_session session,
			crm_integration_anlt.t_lkp_atlas_user atlas_user,
			crm_integration_anlt.t_lkp_contact base_contact,
			crm_integration_anlt.t_lkp_paidad_index index,
			crm_integration_anlt.v_lkp_paidad_index v_index,
			crm_integration_anlt.v_lkp_paidad_index_type v_index_type,
			crm_integration_anlt.t_rel_scai_country_integration scai
		WHERE
			payments.id_user = atlas_user.opr_atlas_user
			and atlas_user.cod_source_system = 7
			AND base_contact.cod_source_system = 12
			AND base_contact.cod_atlas_user = atlas_user.cod_atlas_user
			and payments.id_transaction = session.id
			and payments.id_index = index.opr_paidad_index
			and index.cod_paidad_index = v_index.cod_paidad_index
			and v_index.cod_index_type = v_index_type.cod_index_type
			and payments.date > dateadd(month,-3,to_date(sysdate,'yyyy-mm-dd'))
			and v_index_type.cod_index_type = 2
			and session.provider not in ('admin','volume')
			and session.status = 'finished'
			AND atlas_user.valid_to = 20991231
			AND base_contact.valid_to = 20991231
			AND scai.cod_integration = 50000
			and scai.cod_country = 2
		group BY
			--fac.dat_payment,
			base_contact.cod_contact,
			scai.dat_processing,
			base_contact.cod_source_system
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
				  and lower(kpi.dsc_kpi) = 'max package value last 3 months'
				  and rel.cod_source_system = 12
			) kpi_custom_field
		WHERE
		  B.cod_contact = A.cod_contact (+)
		  and b.valid_to = 20991231
		  and b.cod_source_system = 12
		  and scai.cod_integration = 50000
		  and scai.cod_country = 2
		  and kpi_custom_field.flg_active = 1
		) source,
		crm_integration_anlt.t_fac_base_integration_snap fac_snap
where source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);
  
 -- HST INSERT - KPI OLX.BASE.XYZ (Max Value Package)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_otomoto_calc_max_value_package);

-- SNAP DELETE - KPI OLX.BASE.XYZ (Max Value Package)
delete from crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_otomoto_calc_max_value_package);

--KPI OLX.BASE.XYZ (Max Value Package)
insert into crm_integration_anlt.t_fac_base_integration_snap
  select
    *
  from
    crm_integration_anlt.tmp_pl_otomoto_calc_max_value_package;

drop table if exists crm_integration_anlt.tmp_pl_otomoto_calc_max_value_package;

--$$$
-- CREATE TMP - KPI OLX.BASE.XXX (Revenue (0) - Total / VAS / Listings)
create table crm_integration_anlt.tmp_pl_otomoto_calc_revenue_0 as
	select
		base_contact.cod_contact,
		base_contact.cod_source_system,
		scai.dat_processing dat_snap,
		inner_core.*,
		0 revenue_month
	from
		(
			select
				cod_atlas_user,
				dsc_atlas_user,
				cod_month,
				round((sum(case when cod_index_type = 1 /* vas */ then price else 0 end)),2) val_revenue_vas_net,
				round((sum(case when cod_index_type = 2 /* package */then price else 0 end)),2) val_revenue_listings_net
			from
				(
					select
						to_char(b.last_status_date,'yyyymm') cod_month,
						g.cod_atlas_user,
						g.dsc_atlas_user,
						a.user_id atlas_user_id,
						f.dsc_index_type,
						f.cod_index_type,
						sum(a.price) price,
						sum(a.from_account) from_account
					from
						db_atlas_verticals.payment_basket a,
						db_atlas_verticals.payment_session b,
						crm_integration_anlt.t_lkp_paidad_index c,
						crm_integration_anlt.t_lkp_paidad_index_type d,
						crm_integration_anlt.v_lkp_paidad_index e,
						crm_integration_anlt.v_lkp_paidad_index_type f,
						crm_integration_anlt.t_lkp_atlas_user g,
						crm_integration_anlt.t_lkp_source_system h
					where
						a.session_id = b.id
						and b.provider not in ('admin','volume')
						and a.price > 0
						and a.index_id = c.opr_paidad_index
						and c.cod_source_system = 7
						and c.valid_to = 20991231
						--and d.dsc_paidad_index_type not like 'topup%'
						and f.cod_index_type in (1,2)
						and c.cod_paidad_index_type = d.cod_paidad_index_type
						and d.valid_to = 20991231
						and c.cod_paidad_index = e.cod_paidad_index
						and c.cod_source_system = e.cod_source_system
						and e.cod_index_type = f.cod_index_type
						and b.status = 'finished'
						and g.opr_atlas_user = a.user_id
						and g.cod_source_system = c.cod_source_system
						and g.valid_to = 20991231
						and h.opr_source_system = a.livesync_dbname
						and a.livesync_dbname = b.livesync_dbname
						and h.cod_source_system = c.cod_source_system
						and date_trunc('month',b.last_status_date) = date_trunc('month',sysdate)
					group by
						to_char(b.last_status_date,'yyyymm'),
						g.cod_atlas_user,
						g.dsc_atlas_user,
						a.user_id,
						f.dsc_index_type,
						f.cod_index_type
			) core
		group by
			cod_atlas_user,
			dsc_atlas_user,
			cod_month
	) inner_core,
	crm_integration_anlt.t_lkp_contact base_contact,
	crm_integration_anlt.t_rel_scai_country_integration scai
where
	lower(inner_core.dsc_atlas_user(+)) = lower(base_contact.email)
	and base_contact.valid_to = 20991231
	and base_contact.cod_source_system = 12
	and scai.cod_integration = 50000
	and scai.cod_country = 2;

-- CREATE TMP - KPI OLX.BASE.XXX (Revenue (-1) - Total / VAS / Listings)
create table crm_integration_anlt.tmp_pl_otomoto_calc_revenue_1 as
	select
		base_contact.cod_contact,
		base_contact.cod_source_system,
		scai.dat_processing dat_snap,
		inner_core.*,
		-1 revenue_month
	from
		(
			select
				cod_atlas_user,
				dsc_atlas_user,
				cod_month,
				round((sum(case when cod_index_type = 1 /* vas */ then price else 0 end)),2) val_revenue_vas_net,
				round((sum(case when cod_index_type = 2 /* package */then price else 0 end)),2) val_revenue_listings_net
			from
				(
					select
						to_char(b.last_status_date,'yyyymm') cod_month,
						g.cod_atlas_user,
						g.dsc_atlas_user,
						a.user_id atlas_user_id,
						f.dsc_index_type,
						f.cod_index_type,
						sum(a.price) price,
						sum(a.from_account) from_account
					from
						db_atlas_verticals.payment_basket a,
						db_atlas_verticals.payment_session b,
						crm_integration_anlt.t_lkp_paidad_index c,
						crm_integration_anlt.t_lkp_paidad_index_type d,
						crm_integration_anlt.v_lkp_paidad_index e,
						crm_integration_anlt.v_lkp_paidad_index_type f,
						crm_integration_anlt.t_lkp_atlas_user g,
						crm_integration_anlt.t_lkp_source_system h
					where
						a.session_id = b.id
						and b.provider not in ('admin','volume')
						and a.price > 0
						and a.index_id = c.opr_paidad_index
						and c.cod_source_system = 7
						and c.valid_to = 20991231
						--and d.dsc_paidad_index_type not like 'topup%'
						and f.cod_index_type in (1,2)
						and c.cod_paidad_index_type = d.cod_paidad_index_type
						and d.valid_to = 20991231
						and c.cod_paidad_index = e.cod_paidad_index
						and c.cod_source_system = e.cod_source_system
						and e.cod_index_type = f.cod_index_type
						and b.status = 'finished'
						and g.opr_atlas_user = a.user_id
						and g.cod_source_system = c.cod_source_system
						and g.valid_to = 20991231
						and h.opr_source_system = a.livesync_dbname
						and a.livesync_dbname = b.livesync_dbname
						and h.cod_source_system = c.cod_source_system
						and date_trunc('month',b.last_status_date) = date_trunc('month',add_months(sysdate,-1))
					group by
						to_char(b.last_status_date,'yyyymm'),
						g.cod_atlas_user,
						g.dsc_atlas_user,
						a.user_id,
						f.dsc_index_type,
						f.cod_index_type
			) core
		group by
			cod_atlas_user,
			dsc_atlas_user,
			cod_month
	) inner_core,
	crm_integration_anlt.t_lkp_contact base_contact,
	crm_integration_anlt.t_rel_scai_country_integration scai
where
	lower(inner_core.dsc_atlas_user(+)) = lower(base_contact.email)
	and base_contact.valid_to = 20991231
	and base_contact.cod_source_system = 12
	and scai.cod_integration = 50000
	and scai.cod_country = 2;

-- CREATE TMP - KPI OLX.BASE.099 (Revenue (0) - Total)
create table crm_integration_anlt.tmp_pl_otomoto_calc_revenue_0_total as
select
	core.cod_contact,
	core.cod_custom_field,
	core.dat_snap,
	core.cod_source_system,
	core.custom_field_value
from
	(
		select
			cod_contact,
			cod_custom_field,
			dat_snap,
			cod_source_system,
			cast(round(nvl(val_revenue_listings_net,0) + nvl(val_revenue_vas_net,0),0) as varchar) custom_field_value
		from
			(
				select
					rev_cars.cod_contact,
					kpi_custom_field.cod_custom_field,
					rev_cars.dat_snap,
					rev_cars.cod_source_system,
					rev_cars.val_revenue_listings_net,
					rev_cars.val_revenue_vas_net
				from
					crm_integration_anlt.tmp_pl_otomoto_calc_revenue_0 rev_cars,
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

-- HST INSERT - KPI OLX.BASE.099 (Revenue (0) - Total)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_otomoto_calc_revenue_0_total);

-- SNAP DELETE - KPI OLX.BASE.099 (Revenue (0) - Total)
DELETE FROM crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_otomoto_calc_revenue_0_total);

--KPI OLX.BASE.099 (Revenue (0) - Total)
insert into crm_integration_anlt.t_fac_base_integration_snap
	select
		*
	from
		crm_integration_anlt.tmp_pl_otomoto_calc_revenue_0_total;

drop table if exists crm_integration_anlt.tmp_pl_otomoto_calc_revenue_0_total;

--$$$

-- CREATE TMP - KPI OLX.BASE.099 (Revenue (-1) - Total)
create table crm_integration_anlt.tmp_pl_otomoto_calc_revenue_1_total as
select
	core.cod_contact,
	core.cod_custom_field,
	core.dat_snap,
	core.cod_source_system,
	core.custom_field_value
from
	(
		select
			cod_contact,
			cod_custom_field,
			dat_snap,
			cod_source_system,
			cast(round(nvl(val_revenue_listings_net,0) + nvl(val_revenue_vas_net,0),0) as varchar) custom_field_value
		from
			(
				select
					rev_cars.cod_contact,
					kpi_custom_field.cod_custom_field,
					rev_cars.dat_snap,
					rev_cars.cod_source_system,
					rev_cars.val_revenue_listings_net,
					rev_cars.val_revenue_vas_net
				from
					crm_integration_anlt.tmp_pl_otomoto_calc_revenue_1 rev_cars,
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

-- HST INSERT - KPI OLX.BASE.102 (Revenue (-1) - Total)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_otomoto_calc_revenue_1_total);

-- SNAP DELETE - KPI OLX.BASE.102 (Revenue (-1) - Total)
DELETE FROM crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_otomoto_calc_revenue_1_total);

--KPI OLX.BASE.102 (Revenue (-1) - Total)
insert into crm_integration_anlt.t_fac_base_integration_snap
	select
		*
	from
		crm_integration_anlt.tmp_pl_otomoto_calc_revenue_1_total;

drop table if exists crm_integration_anlt.tmp_pl_otomoto_calc_revenue_1_total;

--$$$

-- CREATE TMP - KPI OLX.BASE.101 (Revenue (0) - VAS)
create table crm_integration_anlt.tmp_pl_otomoto_calc_revenue_0_vas as
select
	core.cod_contact,
	core.cod_custom_field,
	core.dat_snap,
	core.cod_source_system,
	core.custom_field_value
from
	(
		select
			cod_contact,
			cod_custom_field,
			dat_snap,
			cod_source_system,
			cast(round(nvl(val_revenue_vas_net,0),0) as varchar) custom_field_value
		from
			(
				select
					rev_cars.cod_contact,
					kpi_custom_field.cod_custom_field,
					rev_cars.dat_snap,
					rev_cars.cod_source_system,
					rev_cars.val_revenue_listings_net,
					rev_cars.val_revenue_vas_net
				from
					crm_integration_anlt.tmp_pl_otomoto_calc_revenue_0 rev_cars,
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

-- HST INSERT - KPI OLX.BASE.101 (Revenue (0) - VAS)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_otomoto_calc_revenue_0_vas);

-- SNAP DELETE - KPI OLX.BASE.101 (Revenue (0) - VAS)
DELETE FROM crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_otomoto_calc_revenue_0_vas);

--KPI OLX.BASE.101 (Revenue (0) - VAS)
insert into crm_integration_anlt.t_fac_base_integration_snap
	select
		*
	from
		crm_integration_anlt.tmp_pl_otomoto_calc_revenue_0_vas;

drop table if exists crm_integration_anlt.tmp_pl_otomoto_calc_revenue_0_vas;

--$$$

-- CREATE TMP - KPI OLX.BASE.104 (Revenue (-1) - VAS)
create table crm_integration_anlt.tmp_pl_otomoto_calc_revenue_1_vas as
select
	core.cod_contact,
	core.cod_custom_field,
	core.dat_snap,
	core.cod_source_system,
	core.custom_field_value
from
	(
		select
			cod_contact,
			cod_custom_field,
			dat_snap,
			cod_source_system,
			cast(round(nvl(val_revenue_vas_net,0),0) as varchar) custom_field_value
		from
			(
				select
					rev_cars.cod_contact,
					kpi_custom_field.cod_custom_field,
					rev_cars.dat_snap,
					rev_cars.cod_source_system,
					rev_cars.val_revenue_listings_net,
					rev_cars.val_revenue_vas_net
				from
					crm_integration_anlt.tmp_pl_otomoto_calc_revenue_1 rev_cars,
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

-- HST INSERT - KPI OLX.BASE.104 (Revenue (-1) - VAS)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_otomoto_calc_revenue_1_vas);

-- SNAP DELETE - KPI OLX.BASE.104 (Revenue (-1) - VAS)
DELETE FROM crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_otomoto_calc_revenue_1_vas);

--KPI OLX.BASE.104 (Revenue (-1) - VAS)
insert into crm_integration_anlt.t_fac_base_integration_snap
	select
		*
	from
		crm_integration_anlt.tmp_pl_otomoto_calc_revenue_1_vas;

drop table if exists crm_integration_anlt.tmp_pl_otomoto_calc_revenue_1_vas;

--$$$ -- 20

-- CREATE TMP - KPI OLX.BASE.100 (Revenue (0) - Listings)
create table crm_integration_anlt.tmp_pl_otomoto_calc_revenue_0_listings as
select
	core.cod_contact,
	core.cod_custom_field,
	core.dat_snap,
	core.cod_source_system,
	core.custom_field_value
from
	(
		select
			cod_contact,
			cod_custom_field,
			dat_snap,
			cod_source_system,
			cast(round(nvl(val_revenue_listings_net,0),0) as varchar) custom_field_value
		from
			(
				select
					rev_cars.cod_contact,
					kpi_custom_field.cod_custom_field,
					rev_cars.dat_snap,
					rev_cars.cod_source_system,
					rev_cars.val_revenue_listings_net,
					rev_cars.val_revenue_vas_net
				from
					crm_integration_anlt.tmp_pl_otomoto_calc_revenue_0 rev_cars,
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

-- HST INSERT - KPI OLX.BASE.100 (Revenue (0) - Listings)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_otomoto_calc_revenue_0_listings);

-- SNAP DELETE - KPI OLX.BASE.100 (Revenue (0) - Listings)
DELETE FROM crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_otomoto_calc_revenue_0_listings);

--KPI OLX.BASE.100 (Revenue (0) - Listings)
insert into crm_integration_anlt.t_fac_base_integration_snap
	select
		*
	from
		crm_integration_anlt.tmp_pl_otomoto_calc_revenue_0_listings;

drop table if exists crm_integration_anlt.tmp_pl_otomoto_calc_revenue_0_listings;

--$$$

-- CREATE TMP - KPI OLX.BASE.103 (Revenue (-1) - Listings)
create table crm_integration_anlt.tmp_pl_otomoto_calc_revenue_1_listings as
select
	core.cod_contact,
	core.cod_custom_field,
	core.dat_snap,
	core.cod_source_system,
	core.custom_field_value
from
	(
		select
			cod_contact,
			cod_custom_field,
			dat_snap,
			cod_source_system,
			cast(round(nvl(val_revenue_listings_net,0),0) as varchar) custom_field_value
		from
			(
				select
					rev_cars.cod_contact,
					kpi_custom_field.cod_custom_field,
					rev_cars.dat_snap,
					rev_cars.cod_source_system,
					rev_cars.val_revenue_listings_net,
					rev_cars.val_revenue_vas_net
				from
					crm_integration_anlt.tmp_pl_otomoto_calc_revenue_1 rev_cars,
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

-- HST INSERT - KPI OLX.BASE.103 (Revenue (-1) - Listings)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_otomoto_calc_revenue_1_listings);

-- SNAP DELETE - KPI OLX.BASE.103 (Revenue (-1) - Listings)
DELETE FROM crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_otomoto_calc_revenue_1_listings);

--KPI OLX.BASE.103 (Revenue (-1) - Listings)
insert into crm_integration_anlt.t_fac_base_integration_snap
	select
		*
	from
		crm_integration_anlt.tmp_pl_otomoto_calc_revenue_1_listings;

drop table if exists crm_integration_anlt.tmp_pl_otomoto_calc_revenue_1_listings;

drop table if exists crm_integration_anlt.tmp_pl_otomoto_calc_revenue_0;
drop table if exists crm_integration_anlt.tmp_pl_otomoto_calc_revenue_1;

--$$$


-- CREATE TMP - KPI OLX.BASE.105 (User_ID)
create table crm_integration_anlt.tmp_pl_otomoto_calc_user_id as
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
    where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_otomoto_calc_user_id);

-- SNAP DELETE - KPI OLX.BASE.105 (User_ID)
DELETE FROM crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from crm_integration_anlt.tmp_pl_otomoto_calc_user_id);

--KPI OLX.BASE.105 (User_ID)
insert into crm_integration_anlt.t_fac_base_integration_snap
	select
		*
	from
		crm_integration_anlt.tmp_pl_otomoto_calc_user_id;

drop table if exists crm_integration_anlt.tmp_pl_otomoto_calc_user_id;

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
		  and rel.cod_source_system = 12
	) kpi_custom_field
  WHERE
    base_contact.cod_source_system = 12
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

--$$$ -- 30
	
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



delete from crm_integration_anlt.t_fac_base_integration_snap
where cod_source_system = 12
and cod_contact not in (310039,
317701,
271614,
249342,
307758,
303599,
235221,
182196,
288033,
317289,
231843,
304674,
322250,
214602,
308615,
967784,
310823,
285250,
872429,
310305,
287197,
1002499,
314844,
232516,
912325,
838863,
851153,
263169,
318377,
286187,
934039,
998643,
115045,
305086,
966663,
307546,
240455,
274381,
307533,
232231,
306317,
247988,
250614,
286079,
308690,
327478,
263415,
313994,
246699,
276095,
960379,
968671,
261435,
322395,
205948,
304792,
305436,
157061,
310595,
110784,
289305,
313865,
327461,
310475,
315426,
274044,
251627,
288095,
934047,
308717,
247176,
231863,
321864,
998251,
311723,
289959,
318219,
312292,
198080,
319699);