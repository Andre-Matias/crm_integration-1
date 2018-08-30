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
    and rel_integr_proc.cod_country = 5
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
    and rel_country_integr.cod_country = 5 -- Bulgaria
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
    8490 cod_custom_field,
    scai.dat_processing dat_snap,
    base_contact.cod_source_system,
    ' ' as custom_field_value
  from
    crm_integration_anlt.t_lkp_contact base_contact,
    crm_integration_anlt.t_rel_scai_country_integration scai
  where
    base_contact.cod_source_system = 22
    and base_contact.valid_to = 20991231
    and scai.cod_integration = 50000
    and scai.cod_country = 5
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
    8491 cod_custom_field,
    scai.dat_processing dat_snap,
    base_contact.cod_source_system,
    ' ' as custom_field_value
  FROM
    crm_integration_anlt.t_lkp_contact base_contact,
    crm_integration_anlt.t_rel_scai_country_integration scai
  WHERE
    base_contact.cod_source_system = 22
    AND base_contact.valid_to = 20991231
    AND scai.cod_integration = 50000
    and scai.cod_country = 5
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
    8492 cod_custom_field,
    scai.dat_processing dat_snap,
    base_contact.cod_source_system,
    ' ' as custom_field_value
  FROM
    crm_integration_anlt.t_lkp_contact base_contact,
    crm_integration_anlt.t_rel_scai_country_integration scai
  WHERE
    base_contact.cod_source_system = 22
    AND base_contact.valid_to = 20991231
    AND scai.cod_integration = 50000
    and scai.cod_country = 5
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
    8489 cod_custom_field,
    scai.dat_processing dat_snap,
    base_contact.cod_source_system,
    ' ' as custom_field_value
  FROM
    crm_integration_anlt.t_lkp_contact base_contact,
    crm_integration_anlt.t_rel_scai_country_integration scai
  WHERE
    base_contact.cod_source_system = 22
    AND base_contact.valid_to = 20991231
    AND scai.cod_integration = 50000
    and scai.cod_country = 5
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
    8488 cod_custom_field,
    scai.dat_processing dat_snap,
    base_contact.cod_source_system,
    ' ' as custom_field_value
  FROM
    crm_integration_anlt.t_lkp_contact base_contact,
    crm_integration_anlt.t_rel_scai_country_integration scai
  WHERE
    base_contact.cod_source_system = 22
    AND base_contact.valid_to = 20991231
    AND scai.cod_integration = 50000
    and scai.cod_country = 5
) source,
    crm_integration_anlt.t_fac_base_integration_snap fac_snap
where source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and fac_snap.cod_contact is null
);



--(--------NEW FIELDS--------)
insert into crm_integration_anlt.t_fac_base_integration_snap (
select source.* from (
  SELECT
    base_contact.cod_contact,
    8493 cod_custom_field,
    scai.dat_processing dat_snap,
    base_contact.cod_source_system,
    ' ' as custom_field_value
  FROM
    crm_integration_anlt.t_lkp_contact base_contact,
    crm_integration_anlt.t_rel_scai_country_integration scai
  WHERE
    base_contact.cod_source_system = 22
    AND base_contact.valid_to = 20991231
    AND scai.cod_integration = 50000
    and scai.cod_country = 5
) source,
    crm_integration_anlt.t_fac_base_integration_snap fac_snap
where source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and fac_snap.cod_contact is null
);


--(--------DELIMITER1--------)
insert into crm_integration_anlt.t_fac_base_integration_snap (
select source.* from (
  select
    base_contact.cod_contact,
    8401 cod_custom_field,
    scai.dat_processing dat_snap,
    base_contact.cod_source_system,
    ' ' as custom_field_value
  from
    crm_integration_anlt.t_lkp_contact base_contact,
    crm_integration_anlt.t_rel_scai_country_integration scai
  where
    base_contact.cod_source_system = 22
    and base_contact.valid_to = 20991231
    and scai.cod_integration = 50000
    and scai.cod_country = 5
) source,
    crm_integration_anlt.t_fac_base_integration_snap fac_snap
where source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and fac_snap.cod_contact is null
);

--(--------DELIMITER2--------)
insert into crm_integration_anlt.t_fac_base_integration_snap (
select source.* from (
  select
    base_contact.cod_contact,
    8402 cod_custom_field,
    scai.dat_processing dat_snap,
    base_contact.cod_source_system,
    ' ' as custom_field_value
  from
    crm_integration_anlt.t_lkp_contact base_contact,
    crm_integration_anlt.t_rel_scai_country_integration scai
  where
    base_contact.cod_source_system = 22
    and base_contact.valid_to = 20991231
    and scai.cod_integration = 50000
    and scai.cod_country = 5
) source,
    crm_integration_anlt.t_fac_base_integration_snap fac_snap
where source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and fac_snap.cod_contact is null
);

--(--------DELIMITER3--------)
insert into crm_integration_anlt.t_fac_base_integration_snap (
select source.* from (
  select
    base_contact.cod_contact,
    8403 cod_custom_field,
    scai.dat_processing dat_snap,
    base_contact.cod_source_system,
    ' ' as custom_field_value
  from
    crm_integration_anlt.t_lkp_contact base_contact,
    crm_integration_anlt.t_rel_scai_country_integration scai
  where
    base_contact.cod_source_system = 22
    and base_contact.valid_to = 20991231
    and scai.cod_integration = 50000
    and scai.cod_country = 5
) source,
    crm_integration_anlt.t_fac_base_integration_snap fac_snap
where source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and fac_snap.cod_contact is null
);

--(--------DELIMITER4--------)
insert into crm_integration_anlt.t_fac_base_integration_snap (
select source.* from (
  select
    base_contact.cod_contact,
    8404 cod_custom_field,
    scai.dat_processing dat_snap,
    base_contact.cod_source_system,
    ' ' as custom_field_value
  from
    crm_integration_anlt.t_lkp_contact base_contact,
    crm_integration_anlt.t_rel_scai_country_integration scai
  where
    base_contact.cod_source_system = 22
    and base_contact.valid_to = 20991231
    and scai.cod_integration = 50000
    and scai.cod_country = 5
) source,
    crm_integration_anlt.t_fac_base_integration_snap fac_snap
where source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and fac_snap.cod_contact is null
);

--$$$

-- CREATE TMP - KPI OLX.BASE.084 (Last login)
create temp table tmp_bg_olx_calc_last_login as
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
		  isnull(a.cod_source_system,22) cod_source_system,
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
				  atlas_user.cod_source_system = 21
				  AND base_contact.cod_source_system = 22
				  AND lower(base_contact.email) = lower(atlas_user.dsc_atlas_user)
				  AND atlas_user.valid_to = 20991231
				  AND base_contact.valid_to = 20991231
				  AND scai.cod_integration = 50000
				  and scai.cod_country = 5
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
				  and rel.cod_source_system = 22
			) kpi_custom_field
		WHERE
		  B.cod_contact = A.cod_contact (+)
		  and b.valid_to = 20991231
		  and b.cod_source_system = 22
		  and scai.cod_integration = 50000
		  and scai.cod_country = 5
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
    where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from tmp_bg_olx_calc_last_login);



-- SNAP DELETE - KPI OLX.BASE.085 (Last login)
DELETE FROM crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from tmp_bg_olx_calc_last_login);



--KPI OLX.BASE.085 (Last login)
insert into crm_integration_anlt.t_fac_base_integration_snap
	select
		*
	from
		tmp_bg_olx_calc_last_login;


--$$$

-- CREATE TMP - KPI OLX.BASE.031 (Created date)
create temp table tmp_bg_olx_calc_created_date as
select
	  a.cod_contact,
	  kpi_custom_field.cod_custom_field,
	  scai.dat_processing dat_snap,
	  isnull(a.cod_source_system,22) cod_source_system,
	  isnull(a.custom_field_value, ' ') custom_field_value
	from
	  (
			select
				*
			from
				(
					select
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
						atlas_user.cod_source_system = 21
						and base_contact.cod_source_system = 22
						and lower(base_contact.email) = lower(atlas_user.dsc_atlas_user)
						and atlas_user.valid_to = 20991231
						and base_contact.valid_to = 20991231
						and scai.cod_integration = 50000
						and scai.cod_country = 5
					) a
				where
					rn = 1
	  ) a,
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
		  and lower(kpi.dsc_kpi) = 'created date'
		  and rel.cod_source_system = ~22
	) kpi_custom_field
where
  base_contact.cod_contact = a.cod_contact (+)
  and base_contact.valid_to = 20991231
  and base_contact.cod_source_system = 22
  and scai.cod_integration = 50000
  and kpi_custom_field.flg_active = 1
  and scai.cod_country = 5;



-- HST INSERT - KPI OLX.BASE.031 (Created date)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from tmp_bg_olx_calc_created_date);



-- SNAP DELETE - KPI OLX.BASE.031 (Created date)
DELETE FROM crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from tmp_bg_olx_calc_created_date);



--KPI OLX.BASE.031 (Created date)
insert into crm_integration_anlt.t_fac_base_integration_snap
	select
		*
	from
		tmp_bg_olx_calc_created_date;



--$$$

-- CREATE TMP - KPI OLX.BASE.086 (# Logins last 30 days)
create temp table tmp_bg_olx_calc_logins_last_30_days as
select
    base_contact.cod_contact,
    kpi_custom_field.cod_custom_field,
    scai.dat_processing dat_snap,
    isnull(a.cod_source_system,22) cod_source_system,
    isnull(a.custom_field_value, '0') custom_field_value
  from
    (
      select
        cod_contact,
        dat_snap,
        cod_source_system,
        count(distinct server_date_day) custom_field_value
      from
        (
          select
            base.cod_contact,
            web.server_date_day,
            dat_snap,
            cod_source_system,
            count(*) nbr_platform_interactions
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
                atlas_user.cod_source_system = 21
                and base_contact.cod_source_system = 22
                and base_contact.cod_atlas_user = atlas_user.cod_atlas_user
                and atlas_user.valid_to = 20991231
                and base_contact.valid_to = 20991231
                and scai.cod_integration = 50000
                and scai.cod_country = 5
            ) base
          where
            web.server_date_day >= dateadd(day,-30,sysdate)
            and lower(web.country_code) = 'bg'
            and lower(web.host) like '%olx.bg%'
            and web.user_id = base.opr_atlas_user
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
                atlas_user.cod_source_system = 21
                and base_contact.cod_source_system = 22
                and base_contact.cod_atlas_user = atlas_user.cod_atlas_user
                and atlas_user.valid_to = 20991231
                and base_contact.valid_to = 20991231
                and scai.cod_integration = 50000
                and scai.cod_country = 5
            ) base
          where
            ios.server_date_day >= dateadd(day,-30,sysdate)
            and lower(ios.country_code) = 'bg'
            and ios.user_id = base.opr_atlas_user
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
                atlas_user.cod_source_system = 21
                and base_contact.cod_source_system = 22
                and base_contact.cod_atlas_user = atlas_user.cod_atlas_user
                and atlas_user.valid_to = 20991231
                and base_contact.valid_to = 20991231
                and scai.cod_integration = 50000
                and scai.cod_country = 5
            ) base
          where
            android.server_date_day >= dateadd(day,-30,sysdate)
            and lower(android.country_code) = 'bg'
            and android.user_id = base.opr_atlas_user
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
        and lower(kpi.dsc_kpi) = '# logins last 30 days'
        and rel.cod_source_system = 22
    ) kpi_custom_field
  where
    base_contact.cod_contact = a.cod_contact (+)
    and base_contact.valid_to = 20991231
    and base_contact.cod_source_system = 22
    and scai.cod_integration = 50000
    and kpi_custom_field.flg_active = 1
    and scai.cod_country = 5;



-- HST INSERT - KPI OLX.BASE.086 (# Logins last 30 days)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from tmp_bg_olx_calc_logins_last_30_days);



-- SNAP DELETE - KPI OLX.BASE.086 (# Logins last 30 days)
DELETE FROM crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from tmp_bg_olx_calc_logins_last_30_days);



--KPI OLX.BASE.086 (# Logins last 30 days)
insert into crm_integration_anlt.t_fac_base_integration_snap
	select
		*
	from
		tmp_bg_olx_calc_logins_last_30_days;



--$$$

-- CREATE TMP - KPI OLX.BASE.012 (Last package purchased)
create temp table tmp_bg_olx_calc_last_package_purchased as
select
	source.cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	source.custom_field_value
from
	(
    select
      base_contact.cod_contact,
      kpi_custom_field.cod_custom_field,
      scai.dat_processing dat_snap,
      base_contact.cod_source_system cod_source_system,
      coalesce(core.custom_field_value, ' ') custom_field_value
    from
      (
        select
          dsc_atlas_user,
          custom_field_value
        from
          (
            select
              dsc_atlas_user,
              custom_field_value,
              date,
              row_number() over (partition by dsc_atlas_user order by date desc) rn
            from
              (
                select
                  atlas_user.dsc_atlas_user,
                  packets.name ||' '|| variants.name custom_field_value,
                  max(bought) date
                from
                  crm_integration_anlt.t_lkp_atlas_user atlas_user,
                  db_atlas.olxbg_nnl_userpackets userpackets,
                  db_atlas.olxbg_nnl_variants variants,
                  db_atlas.olxbg_nnl_packets packets
                where
                  userpackets.variant_id=variants.variant_id
                  and packets.packet_id=variants.packet_id
                  and atlas_user.opr_atlas_user = userpackets.user_id
                  and atlas_user.valid_to = 20991231
                  and atlas_user.cod_source_system = 21
                group by
                  atlas_user.dsc_atlas_user,
                  packets.name ||' '|| variants.name
              ) a
            ) inner_core
           where
              inner_core.rn = 1
        ) core,
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
            and lower(kpi.dsc_kpi) = 'last package purchased'
            and rel.cod_source_system = 22
       ) kpi_custom_field
    where
      scai.cod_integration = 50000
      and scai.cod_country = 5
      and kpi_custom_field.flg_active = 1
      and lower(base_contact.email) = lower(dsc_atlas_user (+))
      and base_contact.valid_to = 20991231
      and base_contact.cod_source_system = 22
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
    where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from tmp_bg_olx_calc_last_package_purchased);



-- SNAP DELETE - KPI OLX.BASE.012 (Last package purchased)
DELETE FROM crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from tmp_bg_olx_calc_last_package_purchased);



--KPI OLX.BASE.012 (Last package purchased)
/*XXXXX: Como identificar um package?*/
insert into crm_integration_anlt.t_fac_base_integration_snap
	SELECT
		*
	from
		tmp_bg_olx_calc_last_package_purchased;


--$$$


-- CREATE TMP - KPI OLX.BASE.023 (# Replies)
create temp table tmp_bg_olx_calc_replies as
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
      isnull(a.cod_source_system,22) cod_source_system,
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
              eu_bi.fact_replies_unique fac,
              db_atlas.olxbg_ads ads,
              crm_integration_anlt.t_lkp_atlas_user lkp_user,
              crm_integration_anlt.t_lkp_contact lkp_contact,
              crm_integration_anlt.t_rel_scai_country_integration scai
            where
              lkp_user.cod_source_system = 21
              and lkp_contact.cod_source_system = 22
              and fac.listing_nk = ads.id
              and ads.user_id = lkp_user.opr_atlas_user
              and lkp_user.valid_to = 20991231
              and lower(lkp_contact.email) = lower(lkp_user.dsc_atlas_user)
              and lkp_contact.valid_to = 20991231
              and scai.cod_integration = 50000
              and trunc(fac.date_sent_nk) between trunc(sysdate) - 30 and trunc(sysdate)
			  and scai.cod_country = 5
              --and fac.action_sk != 'reply|sms'
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
			  and rel.cod_source_system = 22
		) kpi_custom_field
    where
      b.cod_contact = a.cod_contact (+)
      and b.valid_to = 20991231
      and b.cod_source_system = 22
      and scai.cod_integration = 50000
	  and kpi_custom_field.flg_active = 1
	  and scai.cod_country = 5
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
    where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from tmp_bg_olx_calc_replies);



-- SNAP DELETE - KPI OLX.BASE.023 (# Replies)
delete from crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from tmp_bg_olx_calc_replies);



-- OLX.BASE.023 (# Replies)
insert into crm_integration_anlt.t_fac_base_integration_snap
  select
    *
  from
    tmp_bg_olx_calc_replies;



--$$$

-- CREATE TMP - KPI OLX.BASE.081 (# Replies per Ad)
create temp table tmp_bg_olx_calc_replies_per_ad as
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
      isnull(a.cod_source_system,22) cod_source_system,
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
              eu_bi.fact_replies_unique fac,
              db_atlas.olxbg_ads ads,
              crm_integration_anlt.t_lkp_atlas_user lkp_user,
              crm_integration_anlt.t_lkp_contact lkp_contact,
              crm_integration_anlt.t_rel_scai_country_integration scai
            where
              lkp_user.cod_source_system = 21
              and lkp_contact.cod_source_system = 22
              and fac.listing_nk = ads.id
              and ads.status = 'active'
              and ads.user_id = lkp_user.opr_atlas_user
              and lkp_user.valid_to = 20991231
              and lower(lkp_contact.email) = lower(lkp_user.dsc_atlas_user)
              and lkp_contact.valid_to = 20991231
              and scai.cod_integration = 50000
      		  and scai.cod_country = 5
              --and fac.action_sk != 'reply|sms'
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
			  and rel.cod_source_system = 22
		) kpi_custom_field
    where
      b.cod_contact = a.cod_contact (+)
      and b.valid_to = 20991231
      and b.cod_source_system = 22
      and scai.cod_integration = 50000
	  and kpi_custom_field.flg_active = 1
	  and scai.cod_country = 5
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
    where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from tmp_bg_olx_calc_replies_per_ad);



-- SNAP DELETE - KPI OLX.BASE.081 (# Replies per Ad)
DELETE FROM crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from tmp_bg_olx_calc_replies_per_ad);



-- OLX.BASE.081 (# Replies per Ad)
insert into crm_integration_anlt.t_fac_base_integration_snap
  select
    *
  from
    tmp_bg_olx_calc_replies_per_ad;


--$$$

-- CREATE TMP - KPI OLX.BASE.084 (# Views)
create temp table tmp_bg_olx_calc_views as
select
  source.cod_contact,
  source.cod_custom_field,
  source.dat_snap,
  source.cod_source_system,
  source.custom_field_value
from (
    select
      base_contact.cod_contact,
      kpi_custom_field.cod_custom_field,
      scai.dat_processing dat_snap,
      isnull(a.cod_source_system,22) cod_source_system,
      isnull(a.custom_field_value,'-') custom_field_value
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
									atlas_user.cod_source_system = 21
									and base_contact.cod_source_system = 22
									and base_contact.cod_atlas_user = atlas_user.cod_atlas_user
									and atlas_user.valid_to = 20991231
									and base_contact.valid_to = 20991231
									and scai.cod_integration = 50000
									and scai.cod_country = 5
							) base,
						db_atlas.olxbg_ads ads
						where
							web.server_date_day >= dateadd(day,-30,sysdate)
							and lower(web.country_code) = 'bg'
							and lower(web.host) like '%olx.bg%'
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
									atlas_user.cod_source_system = 21
									and base_contact.cod_source_system = 22
									and base_contact.cod_atlas_user = atlas_user.cod_atlas_user
									and atlas_user.valid_to = 20991231
									and base_contact.valid_to = 20991231
									and scai.cod_integration = 50000
									and scai.cod_country = 5
							) base,
						db_atlas.olxbg_ads ads
						where
							ios.server_date_day >= dateadd(day,-30,sysdate)
							and lower(ios.country_code) = 'bg'
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
									atlas_user.cod_source_system = 21
									and base_contact.cod_source_system = 22
									and base_contact.cod_atlas_user = atlas_user.cod_atlas_user
									and atlas_user.valid_to = 20991231
									and base_contact.valid_to = 20991231
									and scai.cod_integration = 50000
									and scai.cod_country = 5
							) base,
						db_atlas.olxbg_ads ads
						where
							android.server_date_day >= dateadd(day,-30,sysdate)
							and lower(android.country_code) = 'bg'
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
      crm_integration_anlt.t_lkp_contact base_contact,
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
					and rel.cod_source_system = 22
			) kpi_custom_field
    where
      1=1
      and scai.cod_integration = 50000
      and kpi_custom_field.flg_active = 1
      and scai.cod_country = 5
      and base_contact.cod_contact = a.cod_contact (+)
      and base_contact.valid_to = 20991231
      and base_contact.cod_source_system = 22
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
    where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from tmp_bg_olx_calc_views);



-- SNAP DELETE - KPI OLX.BASE.084 (# Views)
delete from crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from tmp_bg_olx_calc_views);


-- OLX.BASE.084 (# Views)
insert into crm_integration_anlt.t_fac_base_integration_snap
  select
    *
  from
    tmp_bg_olx_calc_views;



--$$$

-- CREATE TEMPORARY TABLE - KPI OLX.BASE.088 (Active package expiry date)
create temp table tmp_bg_olx_calc_active_package_expiry_date as
select
	source.cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	source.custom_field_value
from
	(
		select
		  base_contact.cod_contact,
		  kpi_custom_field.cod_custom_field,
		  scai.dat_processing dat_snap,
		  base_contact.cod_source_system cod_source_system,
		  coalesce(core.custom_field_value, '1900-01-01 00:00:00') custom_field_value
		from
		  (
        select
          coalesce(dsc_atlas_user,'unknown') dsc_atlas_user,
          cast(max(fac.expire) as varchar) custom_field_value
        from
          crm_integration_anlt.t_lkp_atlas_user atlas_user,
          db_atlas.olxbg_nnl_userpackets fac
        where
          atlas_user.cod_source_system = 21
          and atlas_user.valid_to = 20991231
          and atlas_user.opr_atlas_user = fac.user_id
        group by
          atlas_user.dsc_atlas_user
		  ) core,
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
				  and lower(kpi.dsc_kpi) = 'active package expiry date'
				  and rel.cod_source_system = 22
			) kpi_custom_field
	where
	  scai.cod_integration = 50000
		and scai.cod_country = 5
	  and kpi_custom_field.flg_active = 1
		and lower(base_contact.email) = lower(dsc_atlas_user (+))
		and valid_to = 20991231
    and cod_source_system = 22
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
    where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from tmp_bg_olx_calc_active_package_expiry_date);



-- SNAP DELETE - KPI OLX.BASE.088 (Active package expiry date)
delete from crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in
  (select cod_contact, cod_custom_field from tmp_bg_olx_calc_active_package_expiry_date);



--KPI OLX.BASE.088 (Active package expiry date)
insert into crm_integration_anlt.t_fac_base_integration_snap
  select
    *
  from
    tmp_bg_olx_calc_active_package_expiry_date;
-- 8106 Active package expiry date

	

--$$$	

-- CREATE TMP - KPI OLX.BASE.014 (Max days since last call)
create temp table tmp_bg_olx_calc_max_days_since_last_call as
select
  source.cod_contact,
  source.cod_custom_field,
  source.dat_snap,
  source.cod_source_system,
  source.custom_field_value
from
  (
    select
      base_contact.cod_contact,
      kpi_custom_field.cod_custom_field,
      scai.dat_processing dat_snap,
      isnull(a.cod_source_system,22) cod_source_system,
      coalesce(a.custom_field_value,'-') custom_field_value
    from
      (
        select
          cod_contact,
          dat_snap,
          cod_source_system,
          cast(min(custom_field_value) as varchar) custom_field_value
        from
          (
            select
              lkp_contact.cod_contact,
              scai.dat_processing dat_snap,
              lkp_contact.cod_source_system,
              min(datediff(days, trunc(fac.updated_at), trunc(sysdate))) custom_field_value
            from
              crm_integration_anlt.t_fac_call fac,
              crm_integration_anlt.t_lkp_contact lkp_contact,
              crm_integration_anlt.t_rel_scai_country_integration scai
            where
              lkp_contact.cod_source_system = 22
              and lkp_contact.cod_contact = fac.cod_contact
              and lkp_contact.valid_to = 20991231
              and scai.cod_integration = 50000
              and fac.flg_missed = 0
              and scai.cod_country = 5
            group by
              lkp_contact.cod_source_system,
              lkp_contact.cod_contact,
              scai.dat_processing
          ) core
        group by
          cod_contact,
          dat_snap,
          cod_source_system
      ) a,
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
        and lower(kpi.dsc_kpi) = 'max days since last call'
        and rel.cod_source_system = 22
    ) kpi_custom_field
    where
      base_contact.cod_contact = a.cod_contact (+)
      and base_contact.valid_to = 20991231
      and base_contact.cod_source_system = 22
      and scai.cod_integration = 50000
      and scai.cod_country = 5
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
    where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from tmp_bg_olx_calc_max_days_since_last_call);



-- SNAP DELETE - KPI OLX.BASE.014 (Max days since last call)
delete from crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from tmp_bg_olx_calc_max_days_since_last_call);



--KPI OLX.BASE.014 (Max days since last call)
insert into crm_integration_anlt.t_fac_base_integration_snap
  select
    *
  from
    tmp_bg_olx_calc_max_days_since_last_call;	



--$$$
-- CREATE TMP - KPI OLX.BASE.XXX (Revenue (0) - Total / VAS / Listings / VAS Single / VAS Bundle) 
create temp table tmp_bg_olx_calc_revenue_0 as
select
  base_contact.cod_contact,
  kpi_custom_field.cod_custom_field,
  kpi_custom_field.dsc_kpi,
  scai.dat_processing dat_snap,
  base_contact.cod_source_system,
  a.total_revenue_0,
  a.vas_revenue_0,
  a.listings_revenue_0,
  a.single_vas_revenue_0,
  a.bundle_vas_revenue_0
from
  (
    select
      atlas_user.dsc_atlas_user,
      user_payments.id_user,
      -sum(case when paidads_indexes.type in ('topads','pushup','ad_homepage','bundle','logo','nnl') then price else 0 end) total_revenue_0,
      -sum(case when paidads_indexes.type in ('topads','pushup','ad_homepage','bundle','logo') then price else 0 end) vas_revenue_0,
      -sum(case when paidads_indexes.type in ('nnl') then price else 0 end) listings_revenue_0,
      -sum(case when paidads_indexes.type in ('topads','pushup','ad_homepage','logo') then price else 0 end) single_vas_revenue_0,
      -sum(case when paidads_indexes.type in ('bundle') then price else 0 end) bundle_vas_revenue_0
    from
      db_atlas.olxbg_paidads_user_payments user_payments,
      db_atlas.olxbg_paidads_indexes paidads_indexes,
      crm_integration_anlt.t_lkp_atlas_user atlas_user
    where
      atlas_user.opr_atlas_user = user_payments.id_user
      and atlas_user.cod_source_system = 21
      and atlas_user.valid_to = 20991231
      and user_payments.id_index = paidads_indexes.id
      and date >= date_trunc('month', current_date)
    group by
      atlas_user.dsc_atlas_user,
      user_payments.id_user
  ) a,
  crm_integration_anlt.t_lkp_contact base_contact,
  crm_integration_anlt.t_rel_scai_country_integration scai,
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
      and lower(kpi.dsc_kpi) in ( 'revenue (0) - total','revenue (0) - vas','revenue (0) - listings', 'revenue (0) - vas single', 'revenue (0) - vas bundle')
      and rel.cod_source_system = 22
  ) kpi_custom_field
where
  lower(base_contact.email) = lower(dsc_atlas_user (+))
  and base_contact.valid_to = 20991231
  and base_contact.cod_source_system = 22
  and scai.cod_integration = 50000
  and scai.cod_country = 5
  and kpi_custom_field.flg_active = 1;
	 

create temp table tmp_bg_olx_calc_revenue_0_kpi as
select
  core.cod_contact,
	core.cod_custom_field,
	core.dat_snap,
	core.cod_source_system,
  (case
    when core.dsc_kpi = 'revenue (0) - total' then coalesce(total_revenue_0,0)
    when core.dsc_kpi = 'revenue (0) - vas' then coalesce(vas_revenue_0,0)
    when core.dsc_kpi = 'revenue (0) - listings' then coalesce(listings_revenue_0,0)
    when core.dsc_kpi = 'revenue (0) - vas single' then coalesce(single_vas_revenue_0,0)
    when core.dsc_kpi = 'revenue (0) - vas bundle' then coalesce(bundle_vas_revenue_0,0)
   end) as custom_field_value
from
  tmp_bg_olx_calc_revenue_0 core,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where
	core.cod_source_system = fac_snap.cod_source_system (+)
	and core.cod_custom_field = fac_snap.cod_custom_field (+)
	and core.cod_contact = fac_snap.cod_contact (+)
	and ((case
          when core.dsc_kpi = 'revenue (0) - total' then total_revenue_0 != fac_snap.custom_field_value
          when core.dsc_kpi = 'revenue (0) - vas' then vas_revenue_0 != fac_snap.custom_field_value
          when core.dsc_kpi = 'revenue (0) - listings' then listings_revenue_0 != fac_snap.custom_field_value
          when core.dsc_kpi = 'revenue (0) - vas single' then single_vas_revenue_0 != fac_snap.custom_field_value
          when core.dsc_kpi = 'revenue (0) - vas bundle' then bundle_vas_revenue_0 != fac_snap.custom_field_value
        end)
		  or fac_snap.cod_contact is null);



-- HST INSERT - KPI OLX.BASE.099 (Revenue (0) - Total)
-- HST INSERT - KPI OLX.BASE.101 (Revenue (0) - VAS)
-- HST INSERT - KPI OLX.BASE.100 (Revenue (0) - Listings)
-- HST INSERT - KPI OLX.BASE.124 (Revenue (0) - VAS Single)
-- HST INSERT - KPI OLX.BASE.125 (Revenue (0) - VAS Bundle)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from tmp_bg_olx_calc_revenue_0_kpi);



-- SNAP DELETE - KPI OLX.BASE.099 (Revenue (0) - Total)
-- SNAP DELETE - KPI OLX.BASE.101 (Revenue (0) - VAS)
-- SNAP DELETE - KPI OLX.BASE.100 (Revenue (0) - Listings)
-- SNAP DELETE - KPI OLX.BASE.124 (Revenue (0) - VAS Single)
-- SNAP DELETE - KPI OLX.BASE.125 (Revenue (0) - VAS Bundle)
DELETE FROM crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from tmp_bg_olx_calc_revenue_0_kpi);



--KPI OLX.BASE.099 (Revenue (0) - Total)
--KPI OLX.BASE.101 (Revenue (0) - VAS)
--KPI OLX.BASE.100 (Revenue (0) - Listings)
--KPI OLX.BASE.124 (Revenue (0) - VAS Single)
--KPI OLX.BASE.125 (Revenue (0) - VAS Bundle)
insert into crm_integration_anlt.t_fac_base_integration_snap
	select
		*
	from
		tmp_bg_olx_calc_revenue_0_kpi;


--$$$


-- CREATE TMP - KPI OLX.BASE.XXX (Revenue (-1) - Total / VAS / Listings / VAS Single / VAS Bundle) 
create temp table tmp_bg_olx_calc_revenue_1 as
select
  base_contact.cod_contact,
  kpi_custom_field.cod_custom_field,
  kpi_custom_field.dsc_kpi,
  scai.dat_processing dat_snap,
  base_contact.cod_source_system,
  a.total_revenue_1,
  a.vas_revenue_1,
  a.listings_revenue_1,
  a.single_vas_revenue_1,
  a.bundle_vas_revenue_1
from
  (
    select
      atlas_user.dsc_atlas_user,
      user_payments.id_user,
      -sum(case when paidads_indexes.type in ('topads','pushup','ad_homepage','bundle','logo','nnl') then price else 0 end) total_revenue_1,
      -sum(case when paidads_indexes.type in ('topads','pushup','ad_homepage','bundle','logo') then price else 0 end) vas_revenue_1,
      -sum(case when paidads_indexes.type in ('nnl') then price else 0 end) listings_revenue_1,
      -sum(case when paidads_indexes.type in ('topads','pushup','ad_homepage','logo') then price else 0 end) single_vas_revenue_1,
      -sum(case when paidads_indexes.type in ('bundle') then price else 0 end) bundle_vas_revenue_1
    from
      db_atlas.olxbg_paidads_user_payments user_payments,
      db_atlas.olxbg_paidads_indexes paidads_indexes,
      crm_integration_anlt.t_lkp_atlas_user atlas_user
    where
      atlas_user.opr_atlas_user = user_payments.id_user
      and atlas_user.cod_source_system = 21
      and atlas_user.valid_to = 20991231
      and user_payments.id_index = paidads_indexes.id
      and date >= date_trunc('month', current_date) - interval '1 month' and date < date_trunc('month', current_date)
    group by
      atlas_user.dsc_atlas_user,
      user_payments.id_user
  ) a,
  crm_integration_anlt.t_lkp_contact base_contact,
  crm_integration_anlt.t_rel_scai_country_integration scai,
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
      and lower(kpi.dsc_kpi) in ( 'revenue (-1) - total','revenue (-1) - vas','revenue (-1) - listings', 'revenue (-1) - vas single', 'revenue (-1) - vas bundle')
      and rel.cod_source_system = 22
  ) kpi_custom_field
where
  lower(base_contact.email) = lower(dsc_atlas_user (+))
  and base_contact.valid_to = 20991231
  and base_contact.cod_source_system = 22
  and scai.cod_integration = 50000
  and scai.cod_country = 5
  and kpi_custom_field.flg_active = 1;

  
create temp table tmp_bg_olx_calc_revenue_1_kpi as
select
  core.cod_contact,
	core.cod_custom_field,
	core.dat_snap,
	core.cod_source_system,
  (case
    when core.dsc_kpi = 'revenue (-1) - total' then coalesce(total_revenue_1,0)
    when core.dsc_kpi = 'revenue (-1) - vas' then coalesce(vas_revenue_1,0)
    when core.dsc_kpi = 'revenue (-1) - listings' then coalesce(listings_revenue_1,0)
    when core.dsc_kpi = 'revenue (-1) - vas single' then coalesce(single_vas_revenue_1,0)
    when core.dsc_kpi = 'revenue (-1) - vas bundle' then coalesce(bundle_vas_revenue_1,0)
   end) as custom_field_value
from
  tmp_bg_olx_calc_revenue_1 core,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where
	core.cod_source_system = fac_snap.cod_source_system (+)
	and core.cod_custom_field = fac_snap.cod_custom_field (+)
	and core.cod_contact = fac_snap.cod_contact (+)
	and ((case
          when core.dsc_kpi = 'revenue (-1) - total' then total_revenue_1 != fac_snap.custom_field_value
          when core.dsc_kpi = 'revenue (-1) - vas' then vas_revenue_1 != fac_snap.custom_field_value
          when core.dsc_kpi = 'revenue (-1) - listings' then listings_revenue_1 != fac_snap.custom_field_value
          when core.dsc_kpi = 'revenue (-1) - vas single' then single_vas_revenue_1 != fac_snap.custom_field_value
          when core.dsc_kpi = 'revenue (-1) - vas bundle' then bundle_vas_revenue_1 != fac_snap.custom_field_value
        end)
		  or fac_snap.cod_contact is null);


-- HST INSERT - KPI OLX.BASE.102 (Revenue (-1) - Total)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from tmp_bg_olx_calc_revenue_1_kpi);



-- SNAP DELETE - KPI OLX.BASE.102 (Revenue (-1) - Total)
DELETE FROM crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from tmp_bg_olx_calc_revenue_1_kpi);



--KPI OLX.BASE.102 (Revenue (-1) - Total)
insert into crm_integration_anlt.t_fac_base_integration_snap
	select
		*
	from
		tmp_bg_olx_calc_revenue_1_kpi;

--$$$
		
-- CREATE TMP - KPI OLX.BASE.XXX (Revenue (-2) - Total / VAS / Listings / VAS Single / VAS Bundle) 
create temp table tmp_bg_olx_calc_revenue_2 as
select
  base_contact.cod_contact,
  kpi_custom_field.cod_custom_field,
  kpi_custom_field.dsc_kpi,
  scai.dat_processing dat_snap,
  base_contact.cod_source_system,
  a.total_revenue_2,
  a.vas_revenue_2,
  a.listings_revenue_2,
  a.single_vas_revenue_2,
  a.bundle_vas_revenue_2
from
  (
    select
      atlas_user.dsc_atlas_user,
      user_payments.id_user,
      -sum(case when paidads_indexes.type in ('topads','pushup','ad_homepage','bundle','logo','nnl') then price else 0 end) total_revenue_2,
      -sum(case when paidads_indexes.type in ('topads','pushup','ad_homepage','bundle','logo') then price else 0 end) vas_revenue_2,
      -sum(case when paidads_indexes.type in ('nnl') then price else 0 end) listings_revenue_2,
      -sum(case when paidads_indexes.type in ('topads','pushup','ad_homepage','logo') then price else 0 end) single_vas_revenue_2,
      -sum(case when paidads_indexes.type in ('bundle') then price else 0 end) bundle_vas_revenue_2
    from
      db_atlas.olxbg_paidads_user_payments user_payments,
      db_atlas.olxbg_paidads_indexes paidads_indexes,
      crm_integration_anlt.t_lkp_atlas_user atlas_user
    where
      atlas_user.opr_atlas_user = user_payments.id_user
      and atlas_user.cod_source_system = 21
      and atlas_user.valid_to = 20991231
      and user_payments.id_index = paidads_indexes.id
      and date >= date_trunc('month', current_date) - interval '2 month' and date < date_trunc('month', current_date) - interval '1 month'
    group by
      atlas_user.dsc_atlas_user,
      user_payments.id_user
  ) a,
  crm_integration_anlt.t_lkp_contact base_contact,
  crm_integration_anlt.t_rel_scai_country_integration scai,
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
      and lower(kpi.dsc_kpi) in ( 'revenue (-2) - total','revenue (-2) - vas','revenue (-2) - listings', 'revenue (-2) - vas single', 'revenue (-2) - vas bundle')
      and rel.cod_source_system = 22
  ) kpi_custom_field
where
  lower(base_contact.email) = lower(dsc_atlas_user (+))
  and base_contact.valid_to = 20991231
  and base_contact.cod_source_system = 22
  and scai.cod_integration = 50000
  and scai.cod_country = 5
  and kpi_custom_field.flg_active = 1;

  
create temp table tmp_bg_olx_calc_revenue_2_kpi as
select
  core.cod_contact,
	core.cod_custom_field,
	core.dat_snap,
	core.cod_source_system,
  (case
    when core.dsc_kpi = 'revenue (-2) - total' then coalesce(total_revenue_2,0)
    when core.dsc_kpi = 'revenue (-2) - vas' then coalesce(vas_revenue_2,0)
    when core.dsc_kpi = 'revenue (-2) - listings' then coalesce(listings_revenue_2,0)
    when core.dsc_kpi = 'revenue (-2) - vas single' then coalesce(single_vas_revenue_2,0)
    when core.dsc_kpi = 'revenue (-2) - vas bundle' then coalesce(bundle_vas_revenue_2,0)
   end) as custom_field_value
from
  tmp_bg_olx_calc_revenue_2 core,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where
	core.cod_source_system = fac_snap.cod_source_system (+)
	and core.cod_custom_field = fac_snap.cod_custom_field (+)
	and core.cod_contact = fac_snap.cod_contact (+)
	and ((case
          when core.dsc_kpi = 'revenue (-2) - total' then total_revenue_2 != fac_snap.custom_field_value
          when core.dsc_kpi = 'revenue (-2) - vas' then vas_revenue_2 != fac_snap.custom_field_value
          when core.dsc_kpi = 'revenue (-2) - listings' then listings_revenue_2 != fac_snap.custom_field_value
          when core.dsc_kpi = 'revenue (-2) - vas single' then single_vas_revenue_2 != fac_snap.custom_field_value
          when core.dsc_kpi = 'revenue (-2) - vas bundle' then bundle_vas_revenue_2 != fac_snap.custom_field_value
        end)
		  or fac_snap.cod_contact is null);


-- HST INSERT - KPI OLX.BASE.102 (Revenue (-2) - Total)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from tmp_bg_olx_calc_revenue_2_kpi);



-- SNAP DELETE - KPI OLX.BASE.102 (Revenue (-2) - Total)
DELETE FROM crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from tmp_bg_olx_calc_revenue_2_kpi);



--KPI OLX.BASE.102 (Revenue (-2) - Total)
insert into crm_integration_anlt.t_fac_base_integration_snap
	select
		*
	from
		tmp_bg_olx_calc_revenue_2_kpi;		


--$$$


-- CREATE TMP - KPI OLX.BASE.XXX (Revenue (-3) - Total / VAS / Listings / VAS Single / VAS Bundle) 
create temp table tmp_bg_olx_calc_revenue_3 as
select
  base_contact.cod_contact,
  kpi_custom_field.cod_custom_field,
  kpi_custom_field.dsc_kpi,
  scai.dat_processing dat_snap,
  base_contact.cod_source_system,
  a.total_revenue_3,
  a.vas_revenue_3,
  a.listings_revenue_3,
  a.single_vas_revenue_3,
  a.bundle_vas_revenue_3
from
  (
    select
      atlas_user.dsc_atlas_user,
      user_payments.id_user,
      -sum(case when paidads_indexes.type in ('topads','pushup','ad_homepage','bundle','logo','nnl') then price else 0 end) total_revenue_3,
      -sum(case when paidads_indexes.type in ('topads','pushup','ad_homepage','bundle','logo') then price else 0 end) vas_revenue_3,
      -sum(case when paidads_indexes.type in ('nnl') then price else 0 end) listings_revenue_3,
      -sum(case when paidads_indexes.type in ('topads','pushup','ad_homepage','logo') then price else 0 end) single_vas_revenue_3,
      -sum(case when paidads_indexes.type in ('bundle') then price else 0 end) bundle_vas_revenue_3
    from
      db_atlas.olxbg_paidads_user_payments user_payments,
      db_atlas.olxbg_paidads_indexes paidads_indexes,
      crm_integration_anlt.t_lkp_atlas_user atlas_user
    where
      atlas_user.opr_atlas_user = user_payments.id_user
      and atlas_user.cod_source_system = 21
      and atlas_user.valid_to = 20991231
      and user_payments.id_index = paidads_indexes.id
      and date >= date_trunc('month', current_date) - interval '3 month' and date<date_trunc('month', current_date) - interval '2 month'
    group by
      atlas_user.dsc_atlas_user,
      user_payments.id_user
  ) a,
  crm_integration_anlt.t_lkp_contact base_contact,
  crm_integration_anlt.t_rel_scai_country_integration scai,
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
      and lower(kpi.dsc_kpi) in ( 'revenue (-3) - total','revenue (-3) - vas','revenue (-3) - listings', 'revenue (-3) - vas single', 'revenue (-3) - vas bundle')
      and rel.cod_source_system = 22
  ) kpi_custom_field
where
  lower(base_contact.email) = lower(dsc_atlas_user (+))
  and base_contact.valid_to = 20991231
  and base_contact.cod_source_system = 22
  and scai.cod_integration = 50000
  and scai.cod_country = 5
  and kpi_custom_field.flg_active = 1;

  
create temp table tmp_bg_olx_calc_revenue_3_kpi as
select
  core.cod_contact,
	core.cod_custom_field,
	core.dat_snap,
	core.cod_source_system,
  (case
    when core.dsc_kpi = 'revenue (-3) - total' then coalesce(total_revenue_3,0)
    when core.dsc_kpi = 'revenue (-3) - vas' then coalesce(vas_revenue_3,0)
    when core.dsc_kpi = 'revenue (-3) - listings' then coalesce(listings_revenue_3,0)
    when core.dsc_kpi = 'revenue (-3) - vas single' then coalesce(single_vas_revenue_3,0)
    when core.dsc_kpi = 'revenue (-3) - vas bundle' then coalesce(bundle_vas_revenue_3,0)
   end) as custom_field_value
from
  tmp_bg_olx_calc_revenue_3 core,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where
	core.cod_source_system = fac_snap.cod_source_system (+)
	and core.cod_custom_field = fac_snap.cod_custom_field (+)
	and core.cod_contact = fac_snap.cod_contact (+)
	and ((case
          when core.dsc_kpi = 'revenue (-3) - total' then total_revenue_3 != fac_snap.custom_field_value
          when core.dsc_kpi = 'revenue (-3) - vas' then vas_revenue_3 != fac_snap.custom_field_value
          when core.dsc_kpi = 'revenue (-3) - listings' then listings_revenue_3 != fac_snap.custom_field_value
          when core.dsc_kpi = 'revenue (-3) - vas single' then single_vas_revenue_3 != fac_snap.custom_field_value
          when core.dsc_kpi = 'revenue (-3) - vas bundle' then bundle_vas_revenue_3 != fac_snap.custom_field_value
        end)
		  or fac_snap.cod_contact is null);


-- HST INSERT - KPI OLX.BASE.102 (Revenue (-3) - Total)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from tmp_bg_olx_calc_revenue_3_kpi);



-- SNAP DELETE - KPI OLX.BASE.102 (Revenue (-3) - Total)
DELETE FROM crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from tmp_bg_olx_calc_revenue_3_kpi);



--KPI OLX.BASE.102 (Revenue (-3) - Total)
insert into crm_integration_anlt.t_fac_base_integration_snap
	select
		*
	from
		tmp_bg_olx_calc_revenue_3_kpi;				

--$$$

-- CREATE TMP - KPI OLX.BASE.105 (User_ID)
create temp table tmp_bg_olx_calc_user_id as
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
		atlas_user.cod_source_system = 21
		AND base_contact.cod_source_system = 22
		AND lower(base_contact.email) = lower(atlas_user.dsc_atlas_user)
		AND atlas_user.valid_to = 20991231
		AND base_contact.valid_to = 20991231
		AND scai.cod_integration = 50000
		and scai.cod_country = 5
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
			  and rel.cod_source_system = 22
		) kpi_custom_field
	WHERE
	  B.cod_contact = A.cod_contact (+)
	  and b.valid_to = 20991231
	  and b.cod_source_system = 22
	  and scai.cod_integration = 50000
	  and kpi_custom_field.flg_active = 1
		and scai.cod_country = 5
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
    where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from tmp_bg_olx_calc_user_id);



-- SNAP DELETE - KPI OLX.BASE.105 (User_ID)
DELETE FROM crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from tmp_bg_olx_calc_user_id);



--KPI OLX.BASE.105 (User_ID)
insert into crm_integration_anlt.t_fac_base_integration_snap
	select
		*
	from
		tmp_bg_olx_calc_user_id;

--$$$
		
-- CREATE TMP - KPI OLX.BASE.137 (# of usable packages)
create temp table tmp_bg_olx_calc_usable_packages as
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
	  isnull(a.cod_source_system,22) cod_source_system,
	  isnull(a.custom_field_value,'0') custom_field_value
	FROM
	  (
	  SELECT
		base_contact.cod_contact,
		base_contact.cod_source_system,
		cast(count(distinct(userpacket_id)) as varchar) custom_field_value
	  FROM
		db_atlas.olxbg_nnl_userpackets userpackets,
		crm_integration_anlt.t_lkp_atlas_user atlas_user,
		crm_integration_anlt.t_lkp_contact base_contact
	  WHERE
		atlas_user.cod_source_system = 21
		AND base_contact.cod_source_system = 22
    AND userpackets.user_id = atlas_user.opr_atlas_user
    AND userpackets.expire >= sysdate
    AND userpackets.left > 0
		AND lower(base_contact.email) = lower(atlas_user.dsc_atlas_user)
		AND atlas_user.valid_to = 20991231
		AND base_contact.valid_to = 20991231
    group by
    		base_contact.cod_contact,
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
			  and lower(kpi.dsc_kpi) = '# of usable packages'
			  and rel.cod_source_system = 22
		) kpi_custom_field
	WHERE
	  B.cod_contact = A.cod_contact (+)
	  and b.valid_to = 20991231
	  and b.cod_source_system = 22
	  and scai.cod_integration = 50000
	  and kpi_custom_field.flg_active = 1
	  and scai.cod_country = 5
	) source,
		crm_integration_anlt.t_fac_base_integration_snap fac_snap
where source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);



-- HST INSERT - KPI OLX.BASE.137 (# of usable packages)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from tmp_bg_olx_calc_usable_packages);



-- SNAP DELETE - KPI OLX.BASE.137 (# of usable packages)
DELETE FROM crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from tmp_bg_olx_calc_usable_packages);



--KPI OLX.BASE.137 (# of usable packages)
insert into crm_integration_anlt.t_fac_base_integration_snap
	select
		*
	from
		tmp_bg_olx_calc_usable_packages;
		
--$$$

-- CREATE TMP - KPI OLX.BASE.113 (# of ads expiring in next 5 DAYS)
create temp table tmp_bg_olx_calc_ads_expiring as
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
	  isnull(a.cod_source_system,22) cod_source_system,
	  isnull(a.custom_field_value,'0') custom_field_value
	FROM
	  (
	  SELECT
		base_contact.cod_contact,
		base_contact.cod_source_system,
		cast(count(id) as varchar) custom_field_value
	  FROM
		db_atlas.olxbg_ads ads,
		crm_integration_anlt.t_lkp_atlas_user atlas_user,
		crm_integration_anlt.t_lkp_contact base_contact
	  WHERE
		atlas_user.cod_source_system = 21
		AND base_contact.cod_source_system = 22
		AND ads.user_id = atlas_user.opr_atlas_user
		AND ads.status='active'
		AND ads.valid_to <= dateadd(day,5,sysdate)
		AND lower(base_contact.email) = lower(atlas_user.dsc_atlas_user)
		AND atlas_user.valid_to = 20991231
		AND base_contact.valid_to = 20991231
	  GROUP BY
    	base_contact.cod_contact,
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
			  and lower(kpi.dsc_kpi) = '# of ads expiring in next 5 days'
			  and rel.cod_source_system = 22
		) kpi_custom_field
	WHERE
	  B.cod_contact = A.cod_contact (+)
	  and b.valid_to = 20991231
	  and b.cod_source_system = 22
	  and scai.cod_integration = 50000
	  and kpi_custom_field.flg_active = 1
	  and scai.cod_country = 5
	) source,
		crm_integration_anlt.t_fac_base_integration_snap fac_snap
where source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);



-- HST INSERT - KPI OLX.BASE.113 (# of ads expiring in next 5 DAYS)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from tmp_bg_olx_calc_ads_expiring);



-- SNAP DELETE - KPI OLX.BASE.113 (# of ads expiring in next 5 DAYS)
DELETE FROM crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from tmp_bg_olx_calc_ads_expiring);



--KPI OLX.BASE.113 (# of ads expiring in next 5 DAYS)
insert into crm_integration_anlt.t_fac_base_integration_snap
	select
		*
	from
		tmp_bg_olx_calc_ads_expiring;
		
--$$$

-- CREATE TMP - KPI OLX.BASE.112 (Account Status)
create temp table tmp_bg_olx_calc_account_status as
select source.cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	source.custom_field_value
from
	(
	select
		  base_contact.cod_contact,
		  kpi_custom_field.cod_custom_field,
		  scai.dat_processing dat_snap,
		  isnull(base_contact.cod_source_system,22) cod_source_system,
		  isnull(a.custom_field_value, ' ') custom_field_value
		from
		  (
			select
					*
				from
					(
						select
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
							atlas_user.cod_source_system = 21
							and base_contact.cod_source_system = 22
							and lower(base_contact.email) = lower(atlas_user.dsc_atlas_user)
							and atlas_user.valid_to = 20991231
							and base_contact.valid_to = 20991231
							and scai.cod_integration = 50000
							and scai.cod_country = 5
						) a
					where
						rn = 1
		  ) a,
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
				  and lower(kpi.dsc_kpi) = 'account status'
				  and rel.cod_source_system = 22
			) kpi_custom_field
		where
		  1 = 1
		  and base_contact.cod_contact = A.cod_contact (+)
		  and base_contact.valid_to = 20991231
		  and base_contact.cod_source_system = 22
		  and scai.cod_integration = 50000
		  and kpi_custom_field.flg_active = 1
		  and scai.cod_country = 5
		) source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where source.cod_source_system = fac_snap.cod_source_system (+)
	and source.cod_custom_field = fac_snap.cod_custom_field (+)
	and source.cod_contact = fac_snap.cod_contact (+)
	and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);


-- HST INSERT - KPI OLX.BASE.112 (Account Status)
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from tmp_bg_olx_calc_account_status);



-- SNAP DELETE - KPI OLX.BASE.112 (Account Status)
DELETE FROM crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from tmp_bg_olx_calc_account_status);



--KPI OLX.BASE.112 (Account Status)
insert into crm_integration_anlt.t_fac_base_integration_snap
	select
		*
	from
		tmp_bg_olx_calc_account_status;


		
--$$$

-- CREATE TEMPORARY TABLE - KPI OLX.BASE.140 (# of MODERATED ads (30d))
create temp table tmp_bg_olx_calc_moderated_ads as
select
	source.cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	source.custom_field_value
from
	(
		select
		  base_contact.cod_contact,
		  kpi_custom_field.cod_custom_field,
		  scai.dat_processing dat_snap,
		  base_contact.cod_source_system cod_source_system,
		  coalesce(core.custom_field_value, '0') custom_field_value
		from
		  (
          select
            coalesce(dsc_atlas_user,'unknown') dsc_atlas_user,
            cast(count(id) as varchar) custom_field_value
          from
            crm_integration_anlt.t_lkp_atlas_user atlas_user,
            db_atlas.olxbg_ads ads
          where
            atlas_user.cod_source_system = 21
            and atlas_user.valid_to = 20991231
            and atlas_user.opr_atlas_user = ads.user_id
            and status = 'moderated'
            and rmoderation_removed_at >= current_date - interval '30 days'
          group by
            atlas_user.dsc_atlas_user
			) core,
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
				  and lower(kpi.dsc_kpi) = '# of moderated ads (30d)'
				  and rel.cod_source_system = 22
			) kpi_custom_field
	where
	  scai.cod_integration = 50000
		and scai.cod_country = 5
	  and kpi_custom_field.flg_active = 1
		and lower(base_contact.email) = lower(dsc_atlas_user (+))
		and valid_to = 20991231
    and cod_source_system = 22
	) source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);



-- HST INSERT - KPI OLX.BASE.140 (# of MODERATED ads (30d))
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from tmp_bg_olx_calc_moderated_ads);



-- SNAP DELETE - KPI OLX.BASE.140 (# of MODERATED ads (30d))
delete from crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in
  (select cod_contact, cod_custom_field from tmp_bg_olx_calc_moderated_ads);



--KPI OLX.BASE.140 (# of MODERATED ads (30d))
insert into crm_integration_anlt.t_fac_base_integration_snap
  select
    *
  from
    tmp_bg_olx_calc_moderated_ads;
	
--$$$

-- CREATE TEMPORARY TABLE - KPI OLX.BASE.139 (# of OUTDATED ads (30d))
create temp table tmp_bg_olx_calc_outdated_ads as
select
	source.cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	source.custom_field_value
from
	(
		select
		  base_contact.cod_contact,
		  kpi_custom_field.cod_custom_field,
		  scai.dat_processing dat_snap,
		  base_contact.cod_source_system cod_source_system,
		  coalesce(core.custom_field_value, '0') custom_field_value
		from
		  (
          select
            coalesce(dsc_atlas_user,'unknown') dsc_atlas_user,
            cast(count(id) as varchar) custom_field_value
          from
            crm_integration_anlt.t_lkp_atlas_user atlas_user,
            db_atlas.olxbg_ads ads
          where
            atlas_user.cod_source_system = 21
            and atlas_user.valid_to = 20991231
            and atlas_user.opr_atlas_user = ads.user_id
            and status = 'outdated'
			      and ads.valid_to >= current_date - interval '30 days'
          group by
            atlas_user.dsc_atlas_user
			) core,
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
				  and lower(kpi.dsc_kpi) = '# of outdated ads (30d)'
				  and rel.cod_source_system = 22
			) kpi_custom_field
	where
	  scai.cod_integration = 50000
		and scai.cod_country = 5
	  and kpi_custom_field.flg_active = 1
		and lower(base_contact.email) = lower(dsc_atlas_user (+))
		and valid_to = 20991231
    and cod_source_system = 22
	) source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);



-- HST INSERT - KPI OLX.BASE.139 (# of OUTDATED ads (30d))
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from tmp_bg_olx_calc_outdated_ads);



-- SNAP DELETE - KPI OLX.BASE.139 (# of OUTDATED ads (30d))
delete from crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in
  (select cod_contact, cod_custom_field from tmp_bg_olx_calc_outdated_ads);



--KPI OLX.BASE.139 (# of OUTDATED ads (30d))
insert into crm_integration_anlt.t_fac_base_integration_snap
  select
    *
  from
    tmp_bg_olx_calc_outdated_ads;
	
--$$$

-- CREATE TEMPORARY TABLE - KPI OLX.BASE.132 (Delivery (0))
create temp table tmp_bg_olx_calc_delivery_0 as
select
	source.cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	source.custom_field_value
from
	(
		select
		  base_contact.cod_contact,
		  kpi_custom_field.cod_custom_field,
		  scai.dat_processing dat_snap,
		  base_contact.cod_source_system cod_source_system,
		  coalesce(core.custom_field_value, '0') custom_field_value
		from
		  (
          select
            coalesce(dsc_atlas_user,'unknown') dsc_atlas_user,
            cast(count(id) as varchar) custom_field_value
          from
            crm_integration_anlt.t_lkp_atlas_user atlas_user,
            db_atlas.olxbg_econt_shipping_bills shipping_bills
          where
            atlas_user.cod_source_system = 21
            and atlas_user.valid_to = 20991231
            and atlas_user.opr_atlas_user = shipping_bills.user_id
            and shipment_status = 'delivered'
            and delivery_date >= date_trunc('month', current_date)
          group by
            atlas_user.dsc_atlas_user
			) core,
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
				  and lower(kpi.dsc_kpi) = 'delivery (0)'
				  and rel.cod_source_system = 22
			) kpi_custom_field
	where
	  scai.cod_integration = 50000
		and scai.cod_country = 5
		and kpi_custom_field.flg_active = 1
		and lower(base_contact.email) = lower(dsc_atlas_user (+))
		and valid_to = 20991231
    and cod_source_system = 22
	) source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);



-- HST INSERT - KPI OLX.BASE.132 (Delivery (0))
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from tmp_bg_olx_calc_delivery_0);



-- SNAP DELETE - KPI OLX.BASE.132 (Delivery (0))
delete from crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in
  (select cod_contact, cod_custom_field from tmp_bg_olx_calc_delivery_0);



--KPI OLX.BASE.132 (Delivery (0))
insert into crm_integration_anlt.t_fac_base_integration_snap
  select
    *
  from
    tmp_bg_olx_calc_delivery_0;
	
--$$$


-- CREATE TEMPORARY TABLE - KPI OLX.BASE.133 (Delivery (-1))
create temp table tmp_bg_olx_calc_delivery_1 as
select
	source.cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	source.custom_field_value
from
	(
		select
		  base_contact.cod_contact,
		  kpi_custom_field.cod_custom_field,
		  scai.dat_processing dat_snap,
		  base_contact.cod_source_system cod_source_system,
		  coalesce(core.custom_field_value, '0') custom_field_value
		from
		  (
          select
            coalesce(dsc_atlas_user,'unknown') dsc_atlas_user,
            cast(count(id) as varchar) custom_field_value
          from
            crm_integration_anlt.t_lkp_atlas_user atlas_user,
            db_atlas.olxbg_econt_shipping_bills shipping_bills
          where
            atlas_user.cod_source_system = 21
            and atlas_user.valid_to = 20991231
            and atlas_user.opr_atlas_user = shipping_bills.user_id
            and shipment_status = 'delivered'
			and delivery_date >= date_trunc('month', current_date) - interval '1 month' and delivery_date < date_trunc('month', current_date)
          group by
            atlas_user.dsc_atlas_user
			) core,
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
				  and lower(kpi.dsc_kpi) = 'delivery (-1)'
				  and rel.cod_source_system = 22
			) kpi_custom_field
	where
	  scai.cod_integration = 50000
		and scai.cod_country = 5
		and kpi_custom_field.flg_active = 1
		and lower(base_contact.email) = lower(dsc_atlas_user (+))
		and valid_to = 20991231
    and cod_source_system = 22
	) source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);



-- HST INSERT - KPI OLX.BASE.133 (Delivery (-1))
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from tmp_bg_olx_calc_delivery_1);



-- SNAP DELETE - KPI OLX.BASE.133 (Delivery (-1))
delete from crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in
  (select cod_contact, cod_custom_field from tmp_bg_olx_calc_delivery_1);



--KPI OLX.BASE.133 (Delivery (-1))
insert into crm_integration_anlt.t_fac_base_integration_snap
  select
    *
  from
    tmp_bg_olx_calc_delivery_1;
	
--$$$


-- CREATE TEMPORARY TABLE - KPI OLX.BASE.134 (Delivery (-2))
create temp table tmp_bg_olx_calc_delivery_2 as
select
	source.cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	source.custom_field_value
from
	(
		select
		  base_contact.cod_contact,
		  kpi_custom_field.cod_custom_field,
		  scai.dat_processing dat_snap,
		  base_contact.cod_source_system cod_source_system,
		  coalesce(core.custom_field_value, '0') custom_field_value
		from
		  (
          select
            coalesce(dsc_atlas_user,'unknown') dsc_atlas_user,
            cast(count(id) as varchar) custom_field_value
          from
            crm_integration_anlt.t_lkp_atlas_user atlas_user,
            db_atlas.olxbg_econt_shipping_bills shipping_bills
          where
            atlas_user.cod_source_system = 21
            and atlas_user.valid_to = 20991231
            and atlas_user.opr_atlas_user = shipping_bills.user_id
            and shipment_status = 'delivered'
			and delivery_date >= date_trunc('month', current_date) - interval '2 month' and delivery_date < date_trunc('month', current_date) - interval '1 month'
          group by
            atlas_user.dsc_atlas_user
			) core,
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
				  and lower(kpi.dsc_kpi) = 'delivery (-2)'
				  and rel.cod_source_system = 22
			) kpi_custom_field
	where
	  scai.cod_integration = 50000
		and scai.cod_country = 5
		and kpi_custom_field.flg_active = 1
		and lower(base_contact.email) = lower(dsc_atlas_user (+))
		and valid_to = 20991231
    and cod_source_system = 22
	) source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);



-- HST INSERT - KPI OLX.BASE.134 (Delivery (-2))
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from tmp_bg_olx_calc_delivery_2);



-- SNAP DELETE - KPI OLX.BASE.134 (Delivery (-2))
delete from crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in
  (select cod_contact, cod_custom_field from tmp_bg_olx_calc_delivery_2);



--KPI OLX.BASE.134 (Delivery (-2))
insert into crm_integration_anlt.t_fac_base_integration_snap
  select
    *
  from
    tmp_bg_olx_calc_delivery_2;
	
--$$$


-- CREATE TEMPORARY TABLE - KPI OLX.BASE.135 (Delivery (-3))
create temp table tmp_bg_olx_calc_delivery_3 as
select
	source.cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	source.custom_field_value
from
	(
		select
		  base_contact.cod_contact,
		  kpi_custom_field.cod_custom_field,
		  scai.dat_processing dat_snap,
		  base_contact.cod_source_system cod_source_system,
		  coalesce(core.custom_field_value, '0') custom_field_value
		from
		  (
          select
            coalesce(dsc_atlas_user,'unknown') dsc_atlas_user,
            cast(count(id) as varchar) custom_field_value
          from
            crm_integration_anlt.t_lkp_atlas_user atlas_user,
            db_atlas.olxbg_econt_shipping_bills shipping_bills
          where
            atlas_user.cod_source_system = 21
            and atlas_user.valid_to = 20991231
            and atlas_user.opr_atlas_user = shipping_bills.user_id
            and shipment_status = 'delivered'
			and delivery_date >= date_trunc('month', current_date) - interval '3 month' and delivery_date < date_trunc('month', current_date) - interval '2 month'
          group by
            atlas_user.dsc_atlas_user
			) core,
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
				  and lower(kpi.dsc_kpi) = 'delivery (-3)'
				  and rel.cod_source_system = 22
			) kpi_custom_field
	where
	  scai.cod_integration = 50000
		and scai.cod_country = 5
		and kpi_custom_field.flg_active = 1
		and lower(base_contact.email) = lower(dsc_atlas_user (+))
		and valid_to = 20991231
    and cod_source_system = 22
	) source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);



-- HST INSERT - KPI OLX.BASE.135 (Delivery (-3))
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from tmp_bg_olx_calc_delivery_3);



-- SNAP DELETE - KPI OLX.BASE.135 (Delivery (-3))
delete from crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in
  (select cod_contact, cod_custom_field from tmp_bg_olx_calc_delivery_3);



--KPI OLX.BASE.135 (Delivery (-3))
insert into crm_integration_anlt.t_fac_base_integration_snap
  select
    *
  from
    tmp_bg_olx_calc_delivery_3;
	
--$$$


-- CREATE TEMPORARY TABLE - KPI OLX.BASE.136 (Customer Ads Source (API, Manual))
create temp table tmp_bg_olx_calc_ads_source as
select
	source.cod_contact,
	source.cod_custom_field,
	source.dat_snap,
	source.cod_source_system,
	source.custom_field_value
from
	(
	select
	  base_contact.cod_contact,
	  kpi_custom_field.cod_custom_field,
	  scai.dat_processing dat_snap,
	  base_contact.cod_source_system,
	  nvl(core.custom_field_value,'-') custom_field_value
	from
	  (
		select
		  inner_core.cod_source_system,
		  inner_core.cod_contact,
		  inner_core.cod_atlas_user,
		  listagg(inner_core.ad_source, ', ') within group (order by inner_core.ad_source) custom_field_value
		from
		  (
			select distinct
			  lkp_contact.cod_source_system,
			  lkp_contact.cod_contact,
			  lkp_user.cod_atlas_user,
			  case
				when ads.external_partner_code is null then 'Manual'
				when ads.external_partner_code in ('Maciej Wagner','api','api_test','android','apple','i') then 'Manual'
				else 'API'
			  end ad_source
			from
			  db_atlas.olxbg_ads ads,
			  crm_integration_anlt.t_lkp_atlas_user lkp_user,
			  crm_integration_anlt.t_lkp_contact lkp_contact
			where
			  lkp_user.cod_source_system = 21
			  and lkp_contact.cod_source_system = 22
			  and ads.status = 'active'
			  and ads.user_id = lkp_user.opr_atlas_user
			  and lkp_user.valid_to = 20991231
			  and lower(lkp_contact.email) = lower(lkp_user.dsc_atlas_user)
			  and lkp_contact.valid_to = 20991231
		  ) inner_core
		group by
		  inner_core.cod_source_system,
		  inner_core.cod_contact,
		  inner_core.cod_atlas_user
	  ) core,
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
			  and lower(kpi.dsc_kpi) = 'ads source (api, manual)'
			  and rel.cod_source_system = 22
		) kpi_custom_field
	where
	  base_contact.cod_contact = core.cod_contact (+)
	  and kpi_custom_field.flg_active = 1
	  and base_contact.valid_to = 20991231
	  and base_contact.cod_source_system = 22
	  and scai.cod_integration = 50000
	  and scai.cod_country = 5
	) source,
	crm_integration_anlt.t_fac_base_integration_snap fac_snap
where source.cod_source_system = fac_snap.cod_source_system (+)
  and source.cod_custom_field = fac_snap.cod_custom_field (+)
  and source.cod_contact = fac_snap.cod_contact (+)
  and (source.custom_field_value != fac_snap.custom_field_value or fac_snap.cod_contact is null);



-- HST INSERT - KPI OLX.BASE.136 (Customer Ads Source (API, Manual))
insert into crm_integration_anlt.t_hst_base_integration_snap
    select
      target.*
    from
      crm_integration_anlt.t_fac_base_integration_snap target
    where (cod_contact, cod_custom_field) in (select cod_contact, cod_custom_field from tmp_bg_olx_calc_ads_source);



-- SNAP DELETE - KPI OLX.BASE.136 (Customer Ads Source (API, Manual))
delete from crm_integration_anlt.t_fac_base_integration_snap
where (cod_contact, cod_custom_field) in
  (select cod_contact, cod_custom_field from tmp_bg_olx_calc_ads_source);



--KPI OLX.BASE.136 (Customer Ads Source (API, Manual))
insert into crm_integration_anlt.t_fac_base_integration_snap
  select
    *
  from
    tmp_bg_olx_calc_ads_source;
	
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
    and rel_country_integr.cod_country = 5 -- Bulgaria
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
and t_rel_scai_integration_process.cod_country = 5
and proc.dsc_process_short = 't_fac_base_integration_snap_pthorizontal'
and t_rel_scai_integration_process.ind_active = 1;




delete from crm_integration_anlt.t_fac_base_integration_snap
where cod_source_system = 22
and cod_contact not in (
1305357
,1373263
,1342033
,1313158
,1352687
,1352985
,1390716
,1299890
,1360819
,1322389
,1364802
,1362310
,1335100
,1295267
,1314070
,1343908
,1302239
,1475767
,1314831
,1372962
,1389624
,1318178
,1330064
,1342688
,1327818
,1337985
,1297896
,1332884
,1301772
,1360722
,1288867
,1309500
,1291877
,1320450
,1294362
,1392700
,1304068
,1322313
,1389480
,1394902
);