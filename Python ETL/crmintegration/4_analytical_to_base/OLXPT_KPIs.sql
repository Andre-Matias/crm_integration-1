-- #######################
-- ####    PASSO 3    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set dat_processing = source.dat_processing, execution_nbr = source.execution_nbr, cod_status = 2 -- Running
from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc, crm_integration_anlt.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_fac_base_integration_snap_pthorizontal'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 1
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
    and rel_country_integr.cod_country = 1 -- Portugal
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_fac_base_integration_snap_pthorizontal';

--$$$

--(--------REVENUE--------)
insert into crm_integration_anlt.t_fac_base_integration_snap (
select source.* from (
  select
    base_contact.cod_contact,
    170 cod_custom_field,
    scai.dat_processing dat_snap,
    base_contact.cod_source_system,
    ' ' as custom_field_value
  from
    crm_integration_anlt.t_lkp_contact base_contact,
    crm_integration_anlt.t_rel_scai_country_integration scai
  where
    base_contact.cod_source_system = 16
    and base_contact.valid_to = 20991231
    and scai.cod_integration = 50000
    and scai.cod_country = 1
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
    173 cod_custom_field,
    scai.dat_processing dat_snap,
    base_contact.cod_source_system,
    ' ' as custom_field_value
  FROM
    crm_integration_anlt.t_lkp_contact base_contact,
    crm_integration_anlt.t_rel_scai_country_integration scai
  WHERE
    base_contact.cod_source_system = 16
    AND base_contact.valid_to = 20991231
    AND scai.cod_integration = 50000
    and scai.cod_country = 1
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
    176 cod_custom_field,
    scai.dat_processing dat_snap,
    base_contact.cod_source_system,
    ' ' as custom_field_value
  FROM
    crm_integration_anlt.t_lkp_contact base_contact,
    crm_integration_anlt.t_rel_scai_country_integration scai
  WHERE
    base_contact.cod_source_system = 16
    AND base_contact.valid_to = 20991231
    AND scai.cod_integration = 50000
    and scai.cod_country = 1
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
    182 cod_custom_field,
    scai.dat_processing dat_snap,
    base_contact.cod_source_system,
    ' ' as custom_field_value
  FROM
    crm_integration_anlt.t_lkp_contact base_contact,
    crm_integration_anlt.t_rel_scai_country_integration scai
  WHERE
    base_contact.cod_source_system = 16
    AND base_contact.valid_to = 20991231
    AND scai.cod_integration = 50000
    and scai.cod_country = 1
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
    179 cod_custom_field,
    scai.dat_processing dat_snap,
    base_contact.cod_source_system,
    ' ' as custom_field_value
  FROM
    crm_integration_anlt.t_lkp_contact base_contact,
    crm_integration_anlt.t_rel_scai_country_integration scai
  WHERE
    base_contact.cod_source_system = 16
    AND base_contact.valid_to = 20991231
    AND scai.cod_integration = 50000
    and scai.cod_country = 1
) source,
    crm_integration_anlt.t_fac_base_integration_snap fac_snap
where source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and fac_snap.cod_contact is null
);

--$$$

-- CREATE TMP - KPI OLX.BASE.084 (Last login)
create temp table tmp_pt_olx_calc_last_login as
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
		  isnull(a.cod_source_system,16) cod_source_system,
		  isnull(a.custom_field_value, '1900-01-01 00:00:00') custom_field_value
		FROM
		  (
			  select
				*
			  from
				(
				  SELECT
				  base_contact.cod_contact,
				  scai.dat_processing dat_snap,
				  base_contact.cod_source_system,
				  cast(atlas_user.last_login_at as varchar) custom_field_value,
				  row_number() over (partition by atlas_user.dsc_atlas_user order by atlas_user.created_at desc) rn
				  FROM
				  crm_integration_anlt.t_lkp_atlas_user atlas_user,
				  crm_integration_anlt.t_lkp_contact base_contact,
				  crm_integration_anlt.t_rel_scai_country_integration scai
				  WHERE
				  atlas_user.cod_source_system = 8
				  AND base_contact.cod_source_system = 16
				  AND lower(base_contact.email) = lower(atlas_user.dsc_atlas_user)
				  AND atlas_user.valid_to = 20991231
				  AND base_contact.valid_to = 20991231
				  AND scai.cod_integration = 50000
				  and scai.cod_country = 1
				)
			  where
				rn = 1
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
				  and lower(kpi.dsc_kpi) = 'last login'
				  and rel.cod_source_system = 16
			) kpi_custom_field
		WHERE
		  B.cod_contact = A.cod_contact (+)
		  and b.valid_to = 20991231
		  and b.cod_source_system = 16
		  and scai.cod_integration = 50000
      and scai.cod_country = 1
		  and kpi_custom_field.flg_active = 1
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
    where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from tmp_pt_olx_calc_last_login);



-- SNAP DELETE - KPI OLX.BASE.085 (Last login)
DELETE FROM crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from tmp_pt_olx_calc_last_login);



--KPI OLX.BASE.085 (Last login)
insert into crm_integration_anlt.t_fac_base_integration_snap
	select
		*
	from
		tmp_pt_olx_calc_last_login;



--$$$

-- CREATE TMP - KPI OLX.BASE.093 (City)
create temp table tmp_pt_olx_calc_city as
SELECT
	source.cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	source.custom_field_value
FROM
	(
		SELECT
		  B.COD_CONTACT,
		  kpi_custom_field.cod_custom_field,
		  scai.dat_processing dat_snap,
		  ISNULL(A.COD_SOURCE_SYSTEM,16) COD_SOURCE_SYSTEM,
		  ISNULL(A.custom_field_value, ' ') custom_field_value
		FROM
		  (
			select
			  base_contact.cod_contact,
			  scai.dat_processing dat_snap,
			  base_contact.cod_source_system,
			  city.name_pt custom_field_value --not using dsc_city_pl
			from
			  crm_integration_anlt.t_lkp_atlas_user atlas_user,
			  crm_integration_anlt.t_lkp_contact base_contact,
			  crm_integration_anlt.t_rel_scai_country_integration scai,
			  db_atlas.olxpt_cities city
			where
			  atlas_user.cod_source_system = 8
			  and base_contact.cod_source_system = 16
			  and lower(base_contact.email) = lower(atlas_user.dsc_atlas_user)
			  and atlas_user.valid_to = 20991231
			  and base_contact.valid_to = 20991231 
			  and scai.cod_integration = 50000
			  and city.id = atlas_user.opr_city
			  and scai.cod_country = 1
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
				  and lower(kpi.dsc_kpi) = 'city'
				  and rel.cod_source_system = 16
			) kpi_custom_field
		WHERE
		  B.COD_CONTACT = A.COD_CONTACT (+)
		  AND B.VALID_TO = 20991231
		  AND B.cod_source_system = 16
		  and scai.cod_integration = 50000
		  and kpi_custom_field.flg_active = 1
			and scai.cod_country = 1
	) source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);



-- HST INSERT - KPI OLX.BASE.093 (City)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from tmp_pt_olx_calc_city);



-- SNAP DELETE - KPI OLX.BASE.093 (City)
DELETE FROM crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from tmp_pt_olx_calc_city);



--KPI OLX.BASE.093 (City)
insert into crm_integration_anlt.t_fac_base_integration_snap
	select
		*
	from
		tmp_pt_olx_calc_city;



--$$$

-- CREATE TMP - KPI OLX.BASE.031 (Created date)
create temp table tmp_pt_olx_calc_created_date as
SELECT
	source.cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	source.custom_field_value
FROM
	(
		SELECT
		  B.COD_CONTACT,
		  kpi_custom_field.cod_custom_field,
		  scai.dat_processing dat_snap,
		  ISNULL(A.COD_SOURCE_SYSTEM,16) COD_SOURCE_SYSTEM,
		  ISNULL(A.custom_field_value, ' ') custom_field_value
		FROM
		  (
			select
			  base_contact.cod_contact,
			  scai.dat_processing dat_snap,
			  base_contact.cod_source_system,
			  CAST(atlas_user.created_at AS VARCHAR) custom_field_value
			from
			  crm_integration_anlt.t_lkp_atlas_user atlas_user,
			  crm_integration_anlt.t_lkp_contact base_contact,
			  crm_integration_anlt.t_rel_scai_country_integration scai
			where
			  atlas_user.cod_source_system = 8
			  and base_contact.cod_source_system = 16
			  and lower(base_contact.email) = lower(atlas_user.dsc_atlas_user)
			  and atlas_user.valid_to = 20991231
			  and base_contact.valid_to = 20991231
			  and scai.cod_integration = 50000
				and scai.cod_country = 1
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
				  and lower(kpi.dsc_kpi) = 'created date'
				  and rel.cod_source_system = 16
			) kpi_custom_field
		WHERE
		  B.COD_CONTACT = A.COD_CONTACT (+)
		  AND B.VALID_TO = 20991231
		  AND B.cod_source_system = 16
		  and scai.cod_integration = 50000
		  and kpi_custom_field.flg_active = 1
			and scai.cod_country = 1
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
    where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from tmp_pt_olx_calc_created_date);



-- SNAP DELETE - KPI OLX.BASE.031 (Created date)
DELETE FROM crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from tmp_pt_olx_calc_created_date);



--KPI OLX.BASE.031 (Created date)
insert into crm_integration_anlt.t_fac_base_integration_snap
	select
		*
	from
		tmp_pt_olx_calc_created_date;



--$$$

-- CREATE TMP - KPI OLX.BASE.086 (# Logins last 30 days)
create temp table tmp_pt_olx_calc_logins_last_30_days as
SELECT
	source.cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	source.custom_field_value
FROM
	(
		SELECT
		  B.COD_CONTACT,
		  kpi_custom_field.cod_custom_field,
		  scai.dat_processing dat_snap,
		  ISNULL(A.COD_SOURCE_SYSTEM,16) COD_SOURCE_SYSTEM,
		  ISNULL(A.custom_field_value, ' ') custom_field_value
		FROM
		  (
			select
			  base_contact.cod_contact,
			  scai.dat_processing dat_snap,
			  base_contact.cod_source_system,
			  cast(count(distinct last_login_at) as varchar) custom_field_value
			from
			  crm_integration_anlt.t_lkp_atlas_user atlas_user,
			  crm_integration_anlt.t_lkp_contact base_contact,
			  crm_integration_anlt.t_rel_scai_country_integration scai
			where
			  atlas_user.cod_source_system = 8
			  and base_contact.cod_source_system = 16
			  and lower(base_contact.email) = lower(atlas_user.dsc_atlas_user)
			  and base_contact.valid_to = 20991231
			  and scai.cod_integration = 50000
			  and atlas_user.last_login_at is not null
			  and trunc(last_login_at) between trunc(sysdate) - 30 and trunc(sysdate)
				and scai.cod_country = 1
			  group by base_contact.cod_contact,
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
				  and lower(kpi.dsc_kpi) = '# logins last 30 days'
				  and rel.cod_source_system = 16
			) kpi_custom_field
		WHERE
		  B.COD_CONTACT = A.COD_CONTACT (+)
		  AND B.VALID_TO = 20991231
		  AND B.cod_source_system = 16
		  and scai.cod_integration = 50000
		  and kpi_custom_field.flg_active = 1
			and scai.cod_country = 1
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
    where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from tmp_pt_olx_calc_logins_last_30_days);



-- SNAP DELETE - KPI OLX.BASE.086 (# Logins last 30 days)
DELETE FROM crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from tmp_pt_olx_calc_logins_last_30_days);



--KPI OLX.BASE.086 (# Logins last 30 days)
insert into crm_integration_anlt.t_fac_base_integration_snap
	select
		*
	from
		tmp_pt_olx_calc_logins_last_30_days;



--$$$

-- CREATE TMP - KPI OLX.BASE.012 (Last package purchased)
create temp table tmp_pt_olx_calc_last_package_purchased as
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
									db_atlas.olxpt_paidads_user_payments fac,
									crm_integration_anlt.t_rel_scai_country_integration scai,
									crm_integration_anlt.v_lkp_paidad_index idx,
									crm_integration_anlt.v_lkp_paidad_index_type idx_type 
								where
									atlas_user.cod_source_system = 8
									and atlas_user.valid_to = 20991231
									and scai.cod_integration = 50000
									and atlas_user.opr_atlas_user = fac.id_user (+)
									and fac.id_index = idx.opr_paidad_index (+)
									and 8 = idx.cod_source_system (+)
									and idx.cod_index_type = idx_type.cod_index_type(+) 
									and lower(fac.payment_provider) != 'admin'
									and lower(idx_type.dsc_index_type) = 'package'
									and scai.cod_country = 1
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
				  and rel.cod_source_system = 16
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
					and cod_source_system = 16
			) contact
	where
	  scai.cod_integration = 50000
	  and kpi_custom_field.flg_active = 1
		and lower(contact.email) = lower(dsc_atlas_user (+))
		and scai.cod_country = 1
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
    where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from tmp_pt_olx_calc_last_package_purchased);



-- SNAP DELETE - KPI OLX.BASE.012 (Last package purchased)
DELETE FROM crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from tmp_pt_olx_calc_last_package_purchased);



--KPI OLX.BASE.012 (Last package purchased)
/*XXXXX: Como identificar um package?*/
insert into crm_integration_anlt.t_fac_base_integration_snap
	SELECT
		*
	from
		tmp_pt_olx_calc_last_package_purchased;



--$$$

-- CREATE TMP - KPI OLX.BASE.091 (Wallet)
create temp table tmp_pt_olx_calc_wallet as
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
                  8 cod_source_system,
                  fac.current_credits val_current_credits,
                  row_number() OVER ( PARTITION BY fac.id_user ORDER BY fac.date DESC, fac.id DESC ) rn
                FROM
                  db_atlas.olxpt_paidads_user_payments fac
            )
          WHERE rn = 1
          ) i
        where
          i.cod_source_system = d.cod_source_system
          and i.opr_atlas_user = h.opr_atlas_user
          and i.cod_source_system = h.cod_source_system
          and d.cod_source_system = 8
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
          and rel.cod_source_system = 16
      ) kpi_custom_field
    where
      lower(inner_core.dsc_atlas_user(+)) = lower(base_contact.email)
      and base_contact.valid_to = 20991231
      and base_contact.cod_source_system = 16
      and scai.cod_integration = 50000
      and scai.cod_country = 1
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
    where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from tmp_pt_olx_calc_wallet);



-- SNAP DELETE - KPI OLX.BASE.091 (Wallet)
delete from crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from tmp_pt_olx_calc_wallet);


-- KPI OLX.BASE.091 (Wallet)
insert into crm_integration_anlt.t_fac_base_integration_snap
  select
    *
  from
    tmp_pt_olx_calc_wallet;



--$$$

-- CREATE TMP - KPI OLX.BASE.006 (# Active ads per category)
create temp table tmp_pt_olx_calc_active_ads_per_category_core as
select
	b.cod_contact,
	lower(b.email) email,
	kpi_custom_field.cod_custom_field,
	scai.dat_snap,
	nvl(b.cod_source_system,core.cod_source_system) cod_source_system,
	'OLX: ' + cast(nvl(core.custom_field_value,0) as varchar) custom_field_value
from
	(
		select
			base_contact.cod_contact,
			base_contact.email,
			max(scai.dat_processing) dat_snap,
			base_contact.cod_source_system,
			count(ads.status) custom_field_value
		from
			crm_integration_anlt.t_lkp_atlas_user atlas_user,
			crm_integration_anlt.t_lkp_contact base_contact,
			db_atlas.olxpt_ads ads, 
			crm_integration_anlt.t_rel_scai_country_integration scai
		where
			atlas_user.cod_source_system = 8
			and base_contact.cod_source_system = 16
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
			base_contact.email,
			base_contact.cod_source_system
	) core
	right outer join
	crm_integration_anlt.t_lkp_contact b
on
	core.cod_contact = b.cod_contact,
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
		and rel.cod_source_system = 16
) kpi_custom_field
where
	b.valid_to = 20991231
	and b.cod_source_system = 16
	and kpi_custom_field.flg_active = 1;



create temp table tmp_pt_imovirtual_calc_active_ads_per_category_core as
select
	b.cod_contact,
	lower(b.email) email,
	kpi_custom_field.cod_custom_field,
	scai.dat_snap,
	nvl(b.cod_source_system,core.cod_source_system) cod_source_system,
	'IMO: ' + cast(nvl(core.custom_field_value,0) as varchar) custom_field_value
from
	(
		select
			base_contact.cod_contact,
			base_contact.email,
			max(scai.dat_processing) dat_snap,
			base_contact.cod_source_system,
			count(ads.status) custom_field_value
		from
			crm_integration_anlt.t_lkp_atlas_user atlas_user,
			crm_integration_anlt.t_lkp_contact base_contact,
			db_atlas.olxpt_ads ads, 
			crm_integration_anlt.t_rel_scai_country_integration scai
		where
			atlas_user.cod_source_system = 3
			and base_contact.cod_source_system = 17
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
			base_contact.email,
			base_contact.cod_source_system
	) core
	right outer join
	crm_integration_anlt.t_lkp_contact b
on
	core.cod_contact = b.cod_contact,
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
		and rel.cod_source_system = 17
) kpi_custom_field
where
	b.valid_to = 20991231
	and b.cod_source_system = 17
	and kpi_custom_field.flg_active = 1;



create temp table tmp_pt_standvirtual_calc_active_ads_per_category_core as
select
	b.cod_contact,
	lower(b.email) email,
	kpi_custom_field.cod_custom_field,
	scai.dat_snap,
	nvl(b.cod_source_system,core.cod_source_system) cod_source_system,
	'STV: ' + cast(nvl(core.custom_field_value,0) as varchar) custom_field_value
from
	(
		select
			base_contact.cod_contact,
			base_contact.email,
			max(scai.dat_processing) dat_snap,
			base_contact.cod_source_system,
			count(ads.status) custom_field_value
		from
			crm_integration_anlt.t_lkp_atlas_user atlas_user,
			crm_integration_anlt.t_lkp_contact base_contact,
			db_atlas.olxpt_ads ads,
			crm_integration_anlt.t_rel_scai_country_integration scai
		where
			atlas_user.cod_source_system = 4
			and base_contact.cod_source_system = 15
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
			base_contact.email,
			base_contact.cod_source_system
	) core
	right outer join
	crm_integration_anlt.t_lkp_contact b
on
	core.cod_contact = b.cod_contact,
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
		and rel.cod_source_system = 15
) kpi_custom_field
where
	b.valid_to = 20991231
	and b.cod_source_system = 15
	and kpi_custom_field.flg_active = 1;



create temp table tmp_pt_all_calc_active_ads_per_category_final as
	select
		distinct source_imo.cod_contact,
		source_imo.cod_custom_field,
		source_imo.dat_snap,
		source_imo.cod_source_system,
		source_imo.custom_field_value + case when source_olx.cod_contact is not null then ' || ' + source_olx.custom_field_value else '' end custom_field_value
	from
		tmp_pt_imovirtual_calc_active_ads_per_category_core source_imo,
		tmp_pt_olx_calc_active_ads_per_category_core source_olx,
		crm_integration_anlt.t_fac_base_integration_snap fac_snap
	 where
		source_imo.email = source_olx.email(+)
		and source_imo.cod_source_system = fac_snap.cod_source_system (+)
		and source_imo.cod_custom_field = fac_snap.cod_custom_field (+)
		and source_imo.cod_contact = fac_snap.cod_contact (+)
		and (source_imo.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null)

	union  all

	select
		distinct source_stv.cod_contact,
		source_stv.cod_custom_field,
		source_stv.dat_snap,
		source_stv.cod_source_system,
		source_stv.custom_field_value + case when source_olx.cod_contact is not null then ' || ' + source_olx.custom_field_value else '' end custom_field_value
	from
		tmp_pt_standvirtual_calc_active_ads_per_category_core source_stv,
		tmp_pt_olx_calc_active_ads_per_category_core source_olx,
		crm_integration_anlt.t_fac_base_integration_snap fac_snap
	 where
		source_stv.email = source_olx.email(+)
		and source_stv.cod_source_system = fac_snap.cod_source_system (+)
		and source_stv.cod_custom_field = fac_snap.cod_custom_field (+)
		and source_stv.cod_contact = fac_snap.cod_contact (+)
		and (source_stv.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null)

	union all

	select
		distinct source_olx.cod_contact,
		source_olx.cod_custom_field,
		source_olx.dat_snap,
		source_olx.cod_source_system,
		source_olx.custom_field_value
		+ case
				when source_imo.cod_contact is not null then ' || ' + source_imo.custom_field_value
					else ''
			end
		+ case
				when source_stv.cod_contact is not null then ' || ' + source_stv.custom_field_value
					else ''
			end custom_field_value
	from
		tmp_pt_olx_calc_active_ads_per_category_core source_olx,
		tmp_pt_imovirtual_calc_active_ads_per_category_core source_imo,
		tmp_pt_standvirtual_calc_active_ads_per_category_core source_stv,
		crm_integration_anlt.t_fac_base_integration_snap fac_snap
	 where
		source_olx.email = source_imo.email(+)
		and source_olx.email = source_stv.email(+)
		and source_olx.cod_source_system = fac_snap.cod_source_system (+)
		and source_olx.cod_custom_field = fac_snap.cod_custom_field (+)
		and source_olx.cod_contact = fac_snap.cod_contact (+)
		and (source_olx.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);



-- HST INSERT - KPI OLX.BASE.006 (# Active ads per category)
insert into crm_integration_anlt.t_hst_base_integration_snap
select *
from crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in (
  select
    cod_contact,
    cod_custom_field
  from
    tmp_pt_all_calc_active_ads_per_category_final
  )
;



-- SNAP DELETE - KPI OLX.BASE.006 (# Active ads per category)
delete from crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in (
  select
    cod_contact,
    cod_custom_field
  from
    tmp_pt_all_calc_active_ads_per_category_final
  )
;



--KPI OLX.BASE.006 (# Active ads per category)
insert into crm_integration_anlt.t_fac_base_integration_snap
  select
    *
  from
    tmp_pt_all_calc_active_ads_per_category_final;



--$$$

-- CREATE TMP - KPI OLX.BASE.023 (# Replies)
create temp table tmp_pt_olx_calc_replies as
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
      isnull(a.cod_source_system,16) cod_source_system,
      isnull(a.custom_field_value, '-') custom_field_value
    from
      (
        select
          source.cod_contact,
          source.dat_processing dat_snap,
          source.cod_source_system,
          cast(sum(nr_replies) as varchar) custom_field_value --nr_replies,
        from
          (
            select
              lkp_contact.cod_contact,
              scai.dat_processing,
              lkp_contact.cod_source_system,
              ads.id,
              count(*) nr_replies
            from
              db_atlas.olxpt_answers fac,
              db_atlas.olxpt_ads ads,
              crm_integration_anlt.t_lkp_atlas_user lkp_user,
              crm_integration_anlt.t_lkp_contact lkp_contact, 
              crm_integration_anlt.t_rel_scai_country_integration scai
            where
              lkp_user.cod_source_system = 8
              and lkp_contact.cod_source_system = 16
              and fac.ad_id = ads.id 
              and ads.user_id = lkp_user.opr_atlas_user
              and lkp_user.valid_to = 20991231
              and lower(lkp_contact.email) = lower(lkp_user.dsc_atlas_user)
              and lkp_contact.valid_to = 20991231
              and scai.cod_integration = 50000 
              and trunc(fac.posted) between trunc(sysdate) - 30 and trunc(sysdate)
			  and scai.cod_country = 1
            group by
              lkp_contact.cod_contact,
              scai.dat_processing,
              lkp_contact.cod_source_system,
              ads.id
          ) source
        group by
          source.cod_source_system,
          source.cod_contact,
          source.dat_processing
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
			  and lower(kpi.dsc_kpi) = '# replies'
			  and rel.cod_source_system = 16
		) kpi_custom_field
    where
      b.cod_contact = a.cod_contact (+)
      and b.valid_to = 20991231
      and b.cod_source_system = 16
      and scai.cod_integration = 50000
	  and kpi_custom_field.flg_active = 1
		and scai.cod_country = 1
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
    where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from tmp_pt_olx_calc_replies);



-- SNAP DELETE - KPI OLX.BASE.023 (# Replies)
delete from crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from tmp_pt_olx_calc_replies);



-- OLX.BASE.023 (# Replies)
insert into crm_integration_anlt.t_fac_base_integration_snap
  select
    *
  from
    tmp_pt_olx_calc_replies;



--$$$

-- CREATE TMP - KPI OLX.BASE.081 (# Replies per Ad)
create temp table tmp_pt_olx_calc_replies_per_ad as
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
      isnull(a.cod_source_system,16) cod_source_system,
      isnull(a.custom_field_value, '-') custom_field_value
    from
      (
        select
          source.cod_contact,
          source.dat_processing dat_snap,
          source.cod_source_system,
          cast((sum(nr_replies) / count(distinct source.id)) as varchar) custom_field_value
        from
          (
            select
              lkp_contact.cod_source_system,
              lkp_contact.cod_contact,
              scai.dat_processing, 
              lkp_user.cod_atlas_user,
              ads.id,
              count(*) nr_replies
            from
              db_atlas.olxpt_answers  fac,
              db_atlas.olxpt_ads ads, 
              crm_integration_anlt.t_lkp_atlas_user lkp_user,
              crm_integration_anlt.t_lkp_contact lkp_contact,
              crm_integration_anlt.t_rel_scai_country_integration scai
            where
              lkp_user.cod_source_system = 8
              and lkp_contact.cod_source_system = 16
              and fac.ad_id = ads.id  
              and ads.status = 'active'
              and ads.user_id = lkp_user.opr_atlas_user
              and lkp_user.valid_to = 20991231
              and lower(lkp_contact.email) = lower(lkp_user.dsc_atlas_user)
              and lkp_contact.valid_to = 20991231
              and scai.cod_integration = 50000
			  and scai.cod_country = 1
            group by
              lkp_contact.cod_source_system,
              lkp_contact.cod_contact,
              scai.dat_processing, 
              lkp_user.cod_atlas_user,
              ads.id
          ) source
        group by
          source.cod_source_system,
          source.cod_contact,
          source.dat_processing
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
			  and lower(kpi.dsc_kpi) = '# replies per ad'
			  and rel.cod_source_system = 16
		) kpi_custom_field
    where
      b.cod_contact = a.cod_contact (+)
      and b.valid_to = 20991231
      and b.cod_source_system = 16
      and scai.cod_integration = 50000
	  and kpi_custom_field.flg_active = 1
		and scai.cod_country = 1
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
    where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from tmp_pt_olx_calc_replies_per_ad);



-- SNAP DELETE - KPI OLX.BASE.081 (# Replies per Ad)
DELETE FROM crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from tmp_pt_olx_calc_replies_per_ad);



-- OLX.BASE.081 (# Replies per Ad)
insert into crm_integration_anlt.t_fac_base_integration_snap
  select
    *
  from
    tmp_pt_olx_calc_replies_per_ad;



--$$$

-- CREATE TMP - KPI OLX.BASE.082 (# Ads with replies)
create temp table tmp_pt_olx_calc_ads_with_replies as
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
      isnull(a.cod_source_system,16) cod_source_system,
      isnull(a.custom_field_value, '-') custom_field_value
    from
      (
        select
          source.cod_contact,
          source.dat_processing dat_snap,
          source.cod_source_system,
          cast(count(distinct source.id) as varchar) custom_field_value --nr_ads_with_replies,
        from
         (
            select
              lkp_contact.cod_source_system,
              lkp_contact.cod_contact,
              scai.dat_processing, 
              lkp_user.cod_atlas_user,
              ads.id,
              count(*) nr_replies
            from
              db_atlas.olxpt_answers fac,
              db_atlas.olxpt_ads ads,
              crm_integration_anlt.t_lkp_atlas_user lkp_user,
              crm_integration_anlt.t_lkp_contact lkp_contact, 
              crm_integration_anlt.t_rel_scai_country_integration scai
            where
              lkp_user.cod_source_system = 8
              and lkp_contact.cod_source_system = 16
              and fac.ad_id = ads.id 
              and ads.user_id = lkp_user.opr_atlas_user
              and lkp_user.valid_to = 20991231
              and lower(lkp_contact.email) = lower(lkp_user.dsc_atlas_user)
              and lkp_contact.valid_to = 20991231
              and scai.cod_integration = 50000 
              and ads.status = 'active'
			  and scai.cod_country = 1
            group by
              lkp_contact.cod_source_system,
              lkp_contact.cod_contact,
              scai.dat_processing, 
              lkp_user.cod_atlas_user,
              ads.id
        ) source
        group by
          source.cod_source_system,
          source.cod_contact,
          source.dat_processing
      ) A,
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
			  and lower(kpi.dsc_kpi) = '# ads with replies'
			  and rel.cod_source_system = 16
		) kpi_custom_field
    where
      b.cod_contact = a.cod_contact (+)
      and b.valid_to = 20991231
      and b.cod_source_system = 16
      and scai.cod_integration = 50000
			and scai.cod_country = 1
	  and kpi_custom_field.flg_active = 1
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
    where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from tmp_pt_olx_calc_ads_with_replies);



-- SNAP DELETE - KPI OLX.BASE.082 (# Ads with replies)
delete from crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from tmp_pt_olx_calc_ads_with_replies);



 -- OLX.BASE.082 (# Ads with replies)
insert into crm_integration_anlt.t_fac_base_integration_snap
  select
      *
  from
    tmp_pt_olx_calc_ads_with_replies;



--$$$

-- CREATE TMP - KPI OLX.BASE.084 (# Views)
create temp table tmp_pt_olx_calc_views as
select
  source.cod_contact,
  source.cod_custom_field,
  source.dat_snap,
  source.cod_source_system,
  source.custom_field_value
from (
    select
      a.cod_contact, 
      kpi_custom_field.cod_custom_field,
      scai.dat_processing dat_snap,
      isnull(a.cod_source_system,16) cod_source_system,
      a.custom_field_value custom_field_value
    from
      (
				select
				    cod_contact, 
					dat_snap,
					cod_source_system,
					cast(sum(nbr_views) as varchar) custom_field_value
				from
					(
						select
						    base.cod_contact, 
							web.server_date_day,
							dat_snap,
							cod_source_system,
							count(*) nbr_views
						from
							hydra.web web,
							(
								select 
									base_contact.cod_contact,
									scai.dat_processing dat_snap,
									base_contact.cod_source_system,
									atlas_user.opr_atlas_user
								from
									crm_integration_anlt.t_lkp_atlas_user atlas_user,
									crm_integration_anlt.t_lkp_contact base_contact,
									crm_integration_anlt.t_rel_scai_country_integration scai
								where
									atlas_user.cod_source_system = 8
									and base_contact.cod_source_system = 16
									and base_contact.cod_atlas_user = atlas_user.cod_atlas_user
									and atlas_user.valid_to = 20991231
									and base_contact.valid_to = 20991231
									and scai.cod_integration = 50000
									and scai.cod_country = 1 
							) base,
              db_atlas.olxpt_ads ads
						where
							web.server_date_day >= dateadd(day,-30,sysdate)
							and web.country_code = 'PT'
							and web.host like '%olx.pt%'
							--and web.user_id = base.opr_atlas_user
							and action_type = 'ad_page'
              and web.ad_id = ads.id
              and web.seller_id = base.opr_atlas_user
						group by
							base.cod_contact, 
							dat_snap,
							cod_source_system,
							web.server_date_day

						union all

						select
						    base.cod_contact, 
							ios.server_date_day,
							dat_snap,
							cod_source_system,
							count(*) nbr_platform_interactions
						from
							hydra.ios ios,
							(
								select 
									base_contact.cod_contact,
									scai.dat_processing dat_snap,
									base_contact.cod_source_system,
									atlas_user.opr_atlas_user
								from
									crm_integration_anlt.t_lkp_atlas_user atlas_user,
									crm_integration_anlt.t_lkp_contact base_contact,
									crm_integration_anlt.t_rel_scai_country_integration scai
								where
									atlas_user.cod_source_system = 8
									and base_contact.cod_source_system = 16
									and base_contact.cod_atlas_user = atlas_user.cod_atlas_user
									and atlas_user.valid_to = 20991231
									and base_contact.valid_to = 20991231
									and scai.cod_integration = 50000
									and scai.cod_country = 1 
							) base,
              db_atlas.olxpt_ads ads
						where
							ios.server_date_day >= dateadd(day,-30,sysdate)
							and ios.country_code = 'PT'
							--and ios.user_id = base.opr_atlas_user
							and action_type = 'ad_page'
              and ios.ad_id = ads.id
              and ios.seller_id = base.opr_atlas_user
						group by
						    base.cod_contact, 
							ios.server_date_day,
							dat_snap,
							cod_source_system

						union all

						select
						  base.cod_contact, 
							android.server_date_day,
							dat_snap,
							cod_source_system,
							count(*) nbr_platform_interactions
						from
							hydra.android android,
							(
								select 
									base_contact.cod_contact,
									scai.dat_processing dat_snap,
									base_contact.cod_source_system,
									atlas_user.opr_atlas_user
								from
									crm_integration_anlt.t_lkp_atlas_user atlas_user,
									crm_integration_anlt.t_lkp_contact base_contact,
									crm_integration_anlt.t_rel_scai_country_integration scai
								where
									atlas_user.cod_source_system = 8
									and base_contact.cod_source_system = 16
									and base_contact.cod_atlas_user = atlas_user.cod_atlas_user
									and atlas_user.valid_to = 20991231
									and base_contact.valid_to = 20991231
									and scai.cod_integration = 50000
									and scai.cod_country = 1 
							) base,
              db_atlas.olxpt_ads ads
						where
							android.server_date_day >= dateadd(day,-30,sysdate)
							and android.country_code = 'PT'
							--and android.user_id = base.opr_atlas_user
							and action_type = 'ad_page'
              and android.ad_id = ads.id
              and android.seller_id = base.opr_atlas_user
						group by
						  base.cod_contact, 
							android.server_date_day,
							dat_snap,
							cod_source_system
					) core
				group by
				    cod_contact, 
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
					and rel.cod_source_system = 16
			) kpi_custom_field
    where
      1=1
      and scai.cod_integration = 50000
	  and kpi_custom_field.flg_active = 1
	  and scai.cod_country = 1
	  ) source,
  crm_integration_anlt.t_fac_base_integration_snap fac_snap
where source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null)
	  ;



-- HST INSERT - KPI OLX.BASE.084 (# Views)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from tmp_pt_olx_calc_views);



-- SNAP DELETE - KPI OLX.BASE.084 (# Views)
delete from crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from tmp_pt_olx_calc_views);


-- OLX.BASE.084 (# Views)
insert into crm_integration_anlt.t_fac_base_integration_snap
  select
    *
  from
    tmp_pt_olx_calc_views;



--$$$

-- CREATE TEMPORARY TABLE - KPI OLX.BASE.088 (Active package expiry date)
create temp table tmp_pt_olx_calc_active_package_expiry_date as
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
              fac.paidads_valid_to
            from
              crm_integration_anlt.t_lkp_atlas_user atlas_user,
              db_atlas.olxpt_paidads_user_payments fac,
              crm_integration_anlt.t_rel_scai_country_integration scai,
              crm_integration_anlt.v_lkp_paidad_index idx,
              crm_integration_anlt.v_lkp_paidad_index_type idx_type
            where
              atlas_user.cod_source_system = 8
              and atlas_user.valid_to = 20991231
              and scai.cod_integration = 50000
              and atlas_user.opr_atlas_user = fac.id_user (+)
              and fac.id_index = idx.opr_paidad_index (+)
              and 8 = idx.cod_source_system (+)
              and idx.cod_index_type = idx_type.cod_index_type(+)
              and lower(idx_type.dsc_index_type) = 'package'
							and scai.cod_country = 1
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
				  and rel.cod_source_system = 16
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
					and cod_source_system = 16
			) contact
	where
	  scai.cod_integration = 50000
	  and kpi_custom_field.flg_active = 1
		and lower(contact.email) = lower(dsc_atlas_user (+))
		and scai.cod_country = 1
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
    where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from tmp_pt_olx_calc_active_package_expiry_date);



-- SNAP DELETE - KPI OLX.BASE.088 (Active package expiry date)
delete from crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in
  (select cod_contact, cod_custom_field from tmp_pt_olx_calc_active_package_expiry_date);



--KPI OLX.BASE.088 (Active package expiry date)
insert into crm_integration_anlt.t_fac_base_integration_snap
  select
    *
  from
    tmp_pt_olx_calc_active_package_expiry_date;
-- 8106 Active package expiry date



--$$$

-- CREATE TMP - KPI OLX.BASE.014 (Max days since last call)
create temp table tmp_pt_olx_calc_max_days_since_last_call as
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
      isnull(a.cod_source_system,16) cod_source_system,
      isnull(a.custom_field_value, '-') custom_field_value
    from
      (
        select
          lkp_contact.cod_contact,
          scai.dat_processing dat_snap,
          lkp_contact.cod_source_system,
          case when (case when lkp_contact.cod_contact_parent is null then lkp_contact.cod_contact else lkp_contact.cod_contact_parent end) = lkp_contact.cod_contact
            then
              cast(min(datediff(days, trunc(max(fac.updated_at)), trunc(sysdate))) over (partition by case when lkp_contact.cod_contact_parent is null then lkp_contact.cod_contact else lkp_contact.cod_contact_parent end) as varchar)
            ELSE
              cast(min(datediff(days, trunc(fac.updated_at), trunc(sysdate))) as varchar)
          end custom_field_value
        from
          crm_integration_anlt.t_fac_call fac,
          crm_integration_anlt.t_lkp_contact lkp_contact,
          crm_integration_anlt.t_rel_scai_country_integration scai
        where
          lkp_contact.cod_source_system = 16
          and lkp_contact.cod_contact = fac.cod_contact
          and lkp_contact.valid_to = 20991231
          and scai.cod_integration = 50000
          and fac.flg_missed = 0
          and scai.cod_country = 1
        group by
          lkp_contact.cod_source_system,
		  lkp_contact.cod_contact_parent,
          lkp_contact.cod_contact,
          scai.dat_processing
      ) a,
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
			  and lower(kpi.dsc_kpi) = 'max days since last call'
			  and rel.cod_source_system = 16
		) kpi_custom_field
    where
      b.cod_contact = a.cod_contact (+)
      and b.valid_to = 20991231
      and b.cod_source_system = 16
      and scai.cod_integration = 50000
      and scai.cod_country = 1
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
    where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from tmp_pt_olx_calc_max_days_since_last_call);



-- SNAP DELETE - KPI OLX.BASE.014 (Max days since last call)
delete from crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from tmp_pt_olx_calc_max_days_since_last_call);



--KPI OLX.BASE.014 (Max days since last call)
insert into crm_integration_anlt.t_fac_base_integration_snap
  select
    *
  from
    tmp_pt_olx_calc_max_days_since_last_call;



--$$$
-- CREATE TMP - KPI OLX.BASE.XXX (Revenue (0) - Total / VAS / Listings) 
-- CREATE TMP - KPI OLX.BASE.XXX (Revenue (-1) - Total / VAS / Listings)
create temp table tmp_pt_olx_calc_revenue as
	select
		base_contact.cod_contact,
		base_contact.cod_source_system,
		scai.dat_processing dat_snap,
		inner_core.*,
		case when cod_month = to_char(sysdate, 'yyyymm') then 0 else -1 end revenue_month
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
						a.user_id atlas_user_id, --remove
						f.dsc_index_type, --remove
						f.cod_index_type,
						sum(a.price) price,
						sum(a.from_bonus_credits) from_bonus_credits,
						sum(a.from_refund_credits) from_refund_credits,
						sum(a.from_account) from_account
					from
						db_atlas.olxpt_payment_basket a,
						db_atlas.olxpt_payment_session b,
						crm_integration_anlt.t_lkp_paidad_index c,
						crm_integration_anlt.t_lkp_paidad_index_type d,
						crm_integration_anlt.v_lkp_paidad_index e,
						crm_integration_anlt.v_lkp_paidad_index_type f,
						crm_integration_anlt.t_lkp_atlas_user g
					where
						a.session_id = b.id
						and b.provider != 'admin'
						and a.price > 0
						and a.index_id = c.opr_paidad_index
						and c.cod_source_system = 8
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
						and date_trunc('month',b.last_status_date) in ( date_trunc('month', sysdate), date_trunc('month',add_months(sysdate,-1)))
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
	and base_contact.cod_source_system = 16
	and scai.cod_integration = 50000
	and scai.cod_country = 1;
	 

	 
-- CREATE TMP - KPI OLX.BASE.099 (Revenue (0) - Total)
-- CREATE TMP - KPI OLX.BASE.101 (Revenue (0) - VAS)
-- CREATE TMP - KPI OLX.BASE.100 (Revenue (0) - Listings)
create temp table tmp_pt_olx_calc_revenue_0_kpi as
select
	core.cod_contact,
	core.cod_custom_field,
	core.dat_snap,
	core.cod_source_system,
	(case when core.dsc_kpi = 'revenue (0) - vas' then
			custom_field_value_total
		when core.dsc_kpi = 'revenue (0) - total' then
			custom_field_value_vas
		when core.dsc_kpi = 'revenue (0) - listings' then
			custom_field_value_listings   end) as custom_field_value
from
	(
		select
			cod_contact,
			cod_custom_field,
			dat_snap,
			cod_source_system,
			cast(round(nvl(val_revenue_listings_net,0) + nvl(val_revenue_vas_net,0),2) as varchar) custom_field_value_total,
			cast(round(nvl(val_revenue_vas_net,0),2) as varchar) custom_field_value_vas,
			cast(round(nvl(val_revenue_listings_net,0),2) as varchar) custom_field_value_listings,
			dsc_kpi
		from
			(
				select
					rev_olx.cod_contact,
					kpi_custom_field.cod_custom_field,
					rev_olx.dat_snap,
					rev_olx.cod_source_system,
					rev_olx.val_revenue_listings_net,
					rev_olx.val_revenue_vas_net,
					dsc_kpi
				from
					tmp_pt_olx_calc_revenue rev_olx,
					(
						select
							rel.cod_custom_field,
							rel.flg_active,
							lower(kpi.dsc_kpi) dsc_kpi
						from
							crm_integration_anlt.t_lkp_kpi kpi,
							crm_integration_anlt.t_rel_kpi_custom_field rel
						where
							kpi.cod_kpi = rel.cod_kpi
							and lower(kpi.dsc_kpi) in ( 'revenue (0) - total','revenue (0) - vas','revenue (0) - listings')
							and rel.cod_source_system = 16
					) kpi_custom_field
				where
					kpi_custom_field.flg_active = 1
					and rev_olx.revenue_month = 0
					
			) core
	) core,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where
	core.cod_source_system = fac_snap.cod_source_system (+)
	and core.cod_custom_field = fac_snap.cod_custom_field (+)
	and core.cod_contact = fac_snap.cod_contact (+)
	and ( (case when core.dsc_kpi = 'revenue (0) - total' then
			custom_field_value_total != fac_snap.custom_field_value
				when core.dsc_kpi = 'revenue (0) - vas' then
			custom_field_value_vas != fac_snap.custom_field_value
				when core.dsc_kpi = 'revenue (0) - listings' then
			custom_field_value_listings != fac_snap.custom_field_value end)
		or fac_snap.cod_contact is null);



-- HST INSERT - KPI OLX.BASE.099 (Revenue (0) - Total)
-- HST INSERT - KPI OLX.BASE.101 (Revenue (0) - VAS)
-- HST INSERT - KPI OLX.BASE.100 (Revenue (0) - Listings)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from tmp_pt_olx_calc_revenue_0_kpi);



-- SNAP DELETE - KPI OLX.BASE.099 (Revenue (0) - Total)
-- SNAP DELETE - KPI OLX.BASE.101 (Revenue (0) - VAS)
-- SNAP DELETE - KPI OLX.BASE.100 (Revenue (0) - Listings)
DELETE FROM crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from tmp_pt_olx_calc_revenue_0_kpi);



--KPI OLX.BASE.099 (Revenue (0) - Total)
--KPI OLX.BASE.101 (Revenue (0) - VAS)
--KPI OLX.BASE.100 (Revenue (0) - Listings)
insert into crm_integration_anlt.t_fac_base_integration_snap
	select
		*
	from
		tmp_pt_olx_calc_revenue_0_kpi;





-- CREATE TMP - KPI OLX.BASE.099 (Revenue (-1) - Total)
-- CREATE TMP - KPI OLX.BASE.101 (Revenue (-1) - VAS)
-- CREATE TMP - KPI OLX.BASE.103 (Revenue (-1) - Listings)
create temp table tmp_pt_olx_calc_revenue_1_kpi as
select
	core.cod_contact,
	core.cod_custom_field,
	core.dat_snap,
	core.cod_source_system,
	(case when core.dsc_kpi = 'revenue (-1) - vas' then
			custom_field_value_total
		when core.dsc_kpi = 'revenue (-1) - total' then
			custom_field_value_vas
		when core.dsc_kpi = 'revenue (-1) - listings' then
			custom_field_value_listings   end) as custom_field_value
from
	(
		select
			cod_contact,
			cod_custom_field,
			dat_snap,
			cod_source_system,
			cast(round(nvl(val_revenue_listings_net,0) + nvl(val_revenue_vas_net,0),2) as varchar) custom_field_value_total,
			cast(round(nvl(val_revenue_vas_net,0),2) as varchar) custom_field_value_vas,
			cast(round(nvl(val_revenue_listings_net,0),2) as varchar) custom_field_value_listings,
			dsc_kpi
		from
			(
				select
					rev_olx.cod_contact,
					kpi_custom_field.cod_custom_field,
					rev_olx.dat_snap,
					rev_olx.cod_source_system,
					rev_olx.val_revenue_listings_net,
					rev_olx.val_revenue_vas_net,
					dsc_kpi
				from
					tmp_pt_olx_calc_revenue rev_olx,
					(
						select
							rel.cod_custom_field,
							rel.flg_active,
							lower(kpi.dsc_kpi) dsc_kpi
						from
							crm_integration_anlt.t_lkp_kpi kpi,
							crm_integration_anlt.t_rel_kpi_custom_field rel
						where
							kpi.cod_kpi = rel.cod_kpi
							and lower(kpi.dsc_kpi)in ( 'revenue (-1) - total','revenue (-1) - vas','revenue (-1) - listings')
							and rel.cod_source_system = 16
					) kpi_custom_field
				where
					kpi_custom_field.flg_active = 1
					and rev_olx.revenue_month = -1
			) core
	) core,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where
	core.cod_source_system = fac_snap.cod_source_system (+)
	and core.cod_custom_field = fac_snap.cod_custom_field (+)
	and core.cod_contact = fac_snap.cod_contact (+)
	and ( (case when core.dsc_kpi = 'revenue (-1) - total' then
			custom_field_value_total != fac_snap.custom_field_value
				when core.dsc_kpi = 'revenue (-1) - vas' then
			custom_field_value_vas != fac_snap.custom_field_value
				when core.dsc_kpi = 'revenue (-1) - listings' then
			custom_field_value_listings != fac_snap.custom_field_value end)
		or fac_snap.cod_contact is null);



-- HST INSERT - KPI OLX.BASE.102 (Revenue (-1) - Total)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from tmp_pt_olx_calc_revenue_1_kpi);



-- SNAP DELETE - KPI OLX.BASE.102 (Revenue (-1) - Total)
DELETE FROM crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from tmp_pt_olx_calc_revenue_1_kpi);



--KPI OLX.BASE.102 (Revenue (-1) - Total)
insert into crm_integration_anlt.t_fac_base_integration_snap
	select
		*
	from
		tmp_pt_olx_calc_revenue_1_kpi;



--$$$

-- CREATE TMP - KPI OLX.BASE.105 (User_ID)
create temp table tmp_pt_olx_calc_user_id as
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
	  isnull(a.cod_source_system,16) cod_source_system,
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
		atlas_user.cod_source_system = 8
		AND base_contact.cod_source_system = 16
		AND lower(base_contact.email) = lower(atlas_user.dsc_atlas_user)
		AND atlas_user.valid_to = 20991231
		AND base_contact.valid_to = 20991231
		AND scai.cod_integration = 50000
		and scai.cod_country = 1
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
			  and rel.cod_source_system = 16
		) kpi_custom_field
	WHERE
	  B.cod_contact = A.cod_contact (+)
	  and b.valid_to = 20991231
	  and b.cod_source_system = 16
	  and scai.cod_integration = 50000
	  and kpi_custom_field.flg_active = 1
		and scai.cod_country = 1
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
    where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from tmp_pt_olx_calc_user_id);



-- SNAP DELETE - KPI OLX.BASE.105 (User_ID)
DELETE FROM crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from tmp_pt_olx_calc_user_id);



--KPI OLX.BASE.105 (User_ID)
insert into crm_integration_anlt.t_fac_base_integration_snap
	select
		*
	from
		tmp_pt_olx_calc_user_id;



--$$$

--(# Ads consumed)
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
		  and rel.cod_source_system = 16
	) kpi_custom_field
  WHERE
    base_contact.cod_source_system = 16
    AND base_contact.valid_to = 20991231
    AND scai.cod_integration = 50000
		and scai.cod_country = 1
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
create temp table tmp_pt_olx_calc_chat_response as
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
      isnull(a.cod_source_system,16) cod_source_system,
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
              crm_integration_anlt.v_agg_olxpt_answer answer,
              crm_integration_anlt.t_lkp_atlas_user lkp_user,
              crm_integration_anlt.t_lkp_contact lkp_contact,
              crm_integration_anlt.t_rel_scai_country_integration scai
            where
              lkp_user.cod_source_system = 8
              and lkp_contact.cod_source_system = 16
              and answer.seller_id = lkp_user.opr_atlas_user
              and lkp_user.valid_to = 20991231
              and lower(lkp_contact.email) = lower(lkp_user.dsc_atlas_user)
              and lkp_contact.valid_to = 20991231
              and scai.cod_integration = 50000
			  and scai.cod_country = 1
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
			  and rel.cod_source_system = 16
		) kpi_custom_field
    where
      b.cod_contact = a.cod_contact (+)
      and b.valid_to = 20991231
      and b.cod_source_system = 16
      and scai.cod_integration = 50000
	  and kpi_custom_field.flg_active = 1
		and scai.cod_country = 1
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
    where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from tmp_pt_olx_calc_chat_response);



-- SNAP DELETE - KPI OLX.BASE.087 ( Chat response % )
DELETE FROM crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from tmp_pt_olx_calc_chat_response);



--KPI OLX.BASE.087 ( Chat response % )
insert into crm_integration_anlt.t_fac_base_integration_snap
	select
		*
	from
		tmp_pt_olx_calc_chat_response;
		
		
--$$$

--(Scheduled package)
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
		  and rel.cod_source_system = 16
	) kpi_custom_field
  WHERE
    base_contact.cod_source_system = 16
    AND base_contact.valid_to = 20991231
    AND scai.cod_integration = 50000
		and kpi_custom_field.flg_active = 1
		and scai.cod_country = 1
) source,
    crm_integration_anlt.t_fac_base_integration_snap fac_snap
where source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and source.custom_field_value = fac_snap.custom_field_value (+)
  and fac_snap.cod_contact is null
);

--$$$

--(AMI License)
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
		  and rel.cod_source_system = 16
	) kpi_custom_field
  WHERE
    base_contact.cod_source_system = 16
    AND base_contact.valid_to = 20991231
    AND scai.cod_integration = 50000
		and kpi_custom_field.flg_active = 1
		and scai.cod_country = 1
) source,
    crm_integration_anlt.t_fac_base_integration_snap fac_snap
where source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and source.custom_field_value = fac_snap.custom_field_value (+)
  and fac_snap.cod_contact is null
);

--$$$

--(NIF)
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
		  and rel.cod_source_system = 16
	) kpi_custom_field
  WHERE
    base_contact.cod_source_system = 16
    AND base_contact.valid_to = 20991231
    AND scai.cod_integration = 50000
	and kpi_custom_field.flg_active = 1
    and scai.cod_country = 1
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
    and rel_country_integr.cod_country = 1 -- Portugal
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_fac_base_integration_snap_pthorizontal';

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
and t_rel_scai_integration_process.cod_country = 1
and proc.dsc_process_short = 't_fac_base_integration_snap_pthorizontal'
and t_rel_scai_integration_process.ind_active = 1;