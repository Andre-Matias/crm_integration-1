-- #######################
-- ####    PASSO 3    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set dat_processing = source.dat_processing, execution_nbr = source.execution_nbr, cod_status = 2 -- Running
from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc, crm_integration_anlt.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_resource_type'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 4
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
    (select coalesce(max(cod_execution),0) max_cod_exec from crm_integration_anlt.t_fac_scai_execution),
    crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
    crm_integration_anlt.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 4 -- Romania
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_lkp_resource_type';

--$$$

-- #############################################
-- # 			 BASE - Romania                #
-- #		LOADING t_lkp_resource_type (SCD1) #
-- #############################################
	

create temp table tmp_ro_load_resource_type
as
select
  row_number() over (order by source_table.opr_resource_type) new_cod,
  source_table.opr_resource_type,
  source_table.opr_resource_type dsc_resource_type,
  -1 cod_source_system,
  source_table.cod_execution,
  source_table.dat_processing,
  max_cod_resource_type.max_cod,
  case
    when target.opr_resource_type is null then 'I'
    else 'X'
  end dml_type
  from
    (
      select
        resource_type opr_resource_type,
        scai_execution.cod_execution,
        scai_execution.dat_processing
      from
        (
          select distinct resource_type from crm_integration_stg.stg_ro_d_base_calls where resource_type is not null
          union
          select distinct resource_type from crm_integration_stg.stg_ro_d_base_tags where resource_type is not null
          union
          select distinct resource_type from crm_integration_stg.stg_ro_d_base_sources where resource_type is not null
          union
          select distinct resource_type from crm_integration_stg.stg_ro_d_base_tasks where resource_type is not null
        ) a,
        (
          select
            rel_integr_proc.dat_processing,
            max(fac.cod_execution) cod_execution
          from
            crm_integration_anlt.t_lkp_scai_process proc,
            crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
            crm_integration_anlt.t_fac_scai_execution fac
          where
            rel_integr_proc.cod_process = proc.cod_process
            and rel_integr_proc.cod_country = 4
            and rel_integr_proc.cod_integration = 30000
            and rel_integr_proc.ind_active = 1
            and proc.dsc_process_short = 't_lkp_resource_type'
            and fac.cod_process = rel_integr_proc.cod_process
            and fac.cod_integration = rel_integr_proc.cod_integration
            and rel_integr_proc.dat_processing = fac.dat_processing
            and fac.cod_status = 2
          group by
            rel_integr_proc.dat_processing
        ) scai_execution
  ) source_table,
  (select coalesce(max(cod_resource_type),0) max_cod from crm_integration_anlt.t_lkp_resource_type) max_cod_resource_type,
  crm_integration_anlt.t_lkp_resource_type target
where
  source_table.opr_resource_type = target.opr_resource_type (+);

analyze tmp_ro_load_resource_type;



insert into crm_integration_anlt.t_lkp_resource_type
    select
      (max_cod + new_cod) cod_resource_type,
      opr_resource_type,
      dsc_resource_type,
      (select rel_integr_proc.dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_resource_type') valid_from, 
      20991231 valid_to,
      cod_source_system,
	  cod_execution
    from
      tmp_ro_load_resource_type
    where
      dml_type = 'I';



analyze crm_integration_anlt.t_lkp_resource_type;

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
    (select coalesce(max(cod_execution),0) max_cod_exec from crm_integration_anlt.t_fac_scai_execution),
    crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
    crm_integration_anlt.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 4 -- Romania
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_lkp_resource_type';

--$$$
	
-- #######################
-- ####    PASSO 6    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set cod_status = 1, -- Ok
last_processing_datetime = coalesce(sysdate,last_processing_datetime)
/*from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc, crm_integration_anlt.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_resource_type'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 4
  ) source*/
from crm_integration_anlt.t_lkp_scai_process proc 
where t_rel_scai_integration_process.cod_process = proc.cod_process
and t_rel_scai_integration_process.cod_status = 2
and t_rel_scai_integration_process.cod_country = 4
and proc.dsc_process_short = 't_lkp_resource_type'
and t_rel_scai_integration_process.ind_active = 1
/*crm_integration_anlt.t_rel_scai_integration_process.cod_process = source.cod_process
and crm_integration_anlt.t_rel_scai_integration_process.cod_country = source.cod_country
and crm_integration_anlt.t_rel_scai_integration_process.cod_integration = source.cod_integration*/;


--$$$

-- #######################
-- ####    PASSO 3    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set dat_processing = source.dat_processing, execution_nbr = source.execution_nbr, cod_status = 2 -- Running
from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc, crm_integration_anlt.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_industry'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 4
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
    (select coalesce(max(cod_execution),0) max_cod_exec from crm_integration_anlt.t_fac_scai_execution),
    crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
    crm_integration_anlt.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 4 -- Romania
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_lkp_industry';

--$$$	

-- #############################################
-- # 			 BASE - Romania                #
-- #		LOADING t_lkp_industry (SCD1)      #
-- #############################################



create temp table tmp_ro_load_industry
as
select
  row_number() over (order by source_table.opr_industry) new_cod,
  source_table.opr_industry,
  source_table.opr_industry dsc_industry,
  source_table.cod_source_system,
  source_table.cod_execution,
  source_table.dat_processing,
  max_cod_industry.max_cod,
  case
    when target.opr_industry is null then 'I'
    else 'X'
  end dml_type
from
    (
      select
        b.cod_source_system,
        a.industry opr_industry,
        scai_execution.cod_execution,
        scai_execution.dat_processing
      from
        (
          select distinct base_account_country+base_account_category opr_source_system,industry from crm_integration_stg.stg_ro_d_base_contacts where industry is not null
          union
          select distinct base_account_country+base_account_category opr_source_system,industry from crm_integration_stg.stg_ro_d_base_leads where industry is not null
        ) a,
        crm_integration_anlt.t_lkp_source_system b,
        (
          select
            rel_integr_proc.dat_processing,
            max(fac.cod_execution) cod_execution
          from
            crm_integration_anlt.t_lkp_scai_process proc,
            crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
            crm_integration_anlt.t_fac_scai_execution fac
          where
            rel_integr_proc.cod_process = proc.cod_process
            and rel_integr_proc.cod_country = 4
            and rel_integr_proc.cod_integration = 30000
            and rel_integr_proc.ind_active = 1
            and proc.dsc_process_short = 't_lkp_industry'
            and fac.cod_process = rel_integr_proc.cod_process
            and fac.cod_integration = rel_integr_proc.cod_integration
            and rel_integr_proc.dat_processing = fac.dat_processing
            and fac.cod_status = 2
          group by
            rel_integr_proc.dat_processing
        ) scai_execution
      where
        a.opr_source_system = b.opr_source_system
        and cod_country = 4
  ) source_table,
  (select coalesce(max(cod_industry),0) max_cod from crm_integration_anlt.t_lkp_industry) max_cod_industry,
  crm_integration_anlt.t_lkp_industry target
where
  source_table.opr_industry = target.opr_industry (+)
  and source_table.cod_source_system = target.cod_source_system (+);

analyze tmp_ro_load_industry;



insert into crm_integration_anlt.t_lkp_industry
    select
      (max_cod + new_cod) cod_industry,
      opr_industry,
      dsc_industry,
      (select rel_integr_proc.dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_industry') valid_from, 
      20991231 valid_to,
      cod_source_system,
	  cod_execution
    from
      tmp_ro_load_industry
    where
      dml_type = 'I';



analyze crm_integration_anlt.t_lkp_industry;

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
    (select coalesce(max(cod_execution),0) max_cod_exec from crm_integration_anlt.t_fac_scai_execution),
    crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
    crm_integration_anlt.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 4 -- Romania
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_lkp_industry';

--$$$
	
-- #######################
-- ####    PASSO 6    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set cod_status = 1, -- Ok
last_processing_datetime = coalesce(sysdate,last_processing_datetime)
/*from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc, crm_integration_anlt.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_industry'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 4
  ) source*/
from crm_integration_anlt.t_lkp_scai_process proc 
where t_rel_scai_integration_process.cod_process = proc.cod_process
and t_rel_scai_integration_process.cod_status = 2
and t_rel_scai_integration_process.cod_country = 4
and proc.dsc_process_short = 't_lkp_industry'
and t_rel_scai_integration_process.ind_active = 1
/*crm_integration_anlt.t_rel_scai_integration_process.cod_process = source.cod_process
and crm_integration_anlt.t_rel_scai_integration_process.cod_country = source.cod_country
and crm_integration_anlt.t_rel_scai_integration_process.cod_integration = source.cod_integration*/;



--$$$

-- #######################
-- ####    PASSO 3    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set dat_processing = source.dat_processing, execution_nbr = source.execution_nbr, cod_status = 2 -- Running
from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc, crm_integration_anlt.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_lead_status'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 4
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
    (select coalesce(max(cod_execution),0) max_cod_exec from crm_integration_anlt.t_fac_scai_execution),
    crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
    crm_integration_anlt.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 4 -- Romania
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_lkp_lead_status';

--$$$
	
-- #############################################
-- # 			 BASE - Romania                #
-- #		LOADING t_lkp_lead_status (SCD1)   #
-- #############################################	



create temp table tmp_ro_load_lead_status
as
select
  row_number() over (order by source_table.opr_lead_status) new_cod,
  source_table.opr_lead_status,
  source_table.opr_lead_status dsc_lead_status,
  source_table.cod_source_system,
  source_table.cod_execution,
  source_table.dat_processing,
  max_cod_lead_status.max_cod,
  case
    when target.opr_lead_status is null then 'I'
    else 'X'
  end dml_type
from
    (
      select
        b.cod_source_system,
        a.status opr_lead_status,
        scai_execution.cod_execution,
        scai_execution.dat_processing
      from
        (
          select distinct base_account_country+base_account_category opr_source_system,status from crm_integration_stg.stg_ro_d_base_leads where status is not null
        ) a,
        crm_integration_anlt.t_lkp_source_system b,
        (
          select
            rel_integr_proc.dat_processing,
            max(fac.cod_execution) cod_execution
          from
            crm_integration_anlt.t_lkp_scai_process proc,
            crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
            crm_integration_anlt.t_fac_scai_execution fac
          where
            rel_integr_proc.cod_process = proc.cod_process
            and rel_integr_proc.cod_country = 4
            and rel_integr_proc.cod_integration = 30000
            and rel_integr_proc.ind_active = 1
            and proc.dsc_process_short = 't_lkp_lead_status'
            and fac.cod_process = rel_integr_proc.cod_process
            and fac.cod_integration = rel_integr_proc.cod_integration
            and rel_integr_proc.dat_processing = fac.dat_processing
            and fac.cod_status = 2
          group by
            rel_integr_proc.dat_processing
        ) scai_execution
      where
        a.opr_source_system = b.opr_source_system
        and cod_country = 4
  ) source_table,
  (select coalesce(max(cod_lead_status),0) max_cod from crm_integration_anlt.t_lkp_lead_status) max_cod_lead_status,
  crm_integration_anlt.t_lkp_lead_status target
where
  source_table.opr_lead_status = target.opr_lead_status (+)
  and source_table.cod_source_system = target.cod_source_system (+);	

analyze tmp_ro_load_lead_status;



insert into crm_integration_anlt.t_lkp_lead_status
    select
      (max_cod + new_cod) cod_lead_status,
      opr_lead_status,
      dsc_lead_status,
      (select rel_integr_proc.dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_lead_status') valid_from, 
      20991231 valid_to,
      cod_source_system,
	  cod_execution
    from
      tmp_ro_load_lead_status
    where
      dml_type = 'I';



analyze crm_integration_anlt.t_lkp_lead_status;

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
    (select coalesce(max(cod_execution),0) max_cod_exec from crm_integration_anlt.t_fac_scai_execution),
    crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
    crm_integration_anlt.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 4 -- Romania
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_lkp_lead_status';

--$$$
	
-- #######################
-- ####    PASSO 6    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set cod_status = 1, -- Ok
last_processing_datetime = coalesce(sysdate,last_processing_datetime)
/*from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc, crm_integration_anlt.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_lead_status'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 4
  ) source*/
from crm_integration_anlt.t_lkp_scai_process proc 
where t_rel_scai_integration_process.cod_process = proc.cod_process
and t_rel_scai_integration_process.cod_status = 2
and t_rel_scai_integration_process.cod_country = 4
and proc.dsc_process_short = 't_lkp_lead_status'
and t_rel_scai_integration_process.ind_active = 1
/*crm_integration_anlt.t_rel_scai_integration_process.cod_process = source.cod_process
and crm_integration_anlt.t_rel_scai_integration_process.cod_country = source.cod_country
and crm_integration_anlt.t_rel_scai_integration_process.cod_integration = source.cod_integration*/;



--$$$

-- #######################
-- ####    PASSO 3    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set dat_processing = source.dat_processing, execution_nbr = source.execution_nbr, cod_status = 2 -- Running
from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc, crm_integration_anlt.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_product'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 4
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
    (select coalesce(max(cod_execution),0) max_cod_exec from crm_integration_anlt.t_fac_scai_execution),
    crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
    crm_integration_anlt.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 4 -- Romania
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_lkp_product';

--$$$

-- #############################################
-- # 			 BASE - Romania                #
-- #		LOADING t_lkp_product (SCD1)   	   #
-- #############################################



create temp table tmp_ro_load_product
as
select
  row_number() over (order by source_table.opr_sku) new_cod,
  source_table.opr_sku,
  source_table.opr_sku dsc_sku,
  source_table.cod_source_system,
  source_table.cod_execution,
  source_table.dat_processing,
  max_cod_sku.max_cod,
  case
    when target.opr_sku is null then 'I'
    else 'X'
  end dml_type
from
    (
      select
        b.cod_source_system,
        a.sku opr_sku,
        scai_execution.cod_execution,
        scai_execution.dat_processing
      from
        (
          select distinct base_account_country+base_account_category opr_source_system,sku from crm_integration_stg.stg_ro_d_base_line_items where sku is not null
        ) a,
        crm_integration_anlt.t_lkp_source_system b,
        (
          select
            rel_integr_proc.dat_processing,
            max(fac.cod_execution) cod_execution
          from
            crm_integration_anlt.t_lkp_scai_process proc,
            crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
            crm_integration_anlt.t_fac_scai_execution fac
          where
            rel_integr_proc.cod_process = proc.cod_process
            and rel_integr_proc.cod_country = 4
            and rel_integr_proc.cod_integration = 30000
            and rel_integr_proc.ind_active = 1
            and proc.dsc_process_short = 't_lkp_product'
            and fac.cod_process = rel_integr_proc.cod_process
            and fac.cod_integration = rel_integr_proc.cod_integration
            and rel_integr_proc.dat_processing = fac.dat_processing
            and fac.cod_status = 2
          group by
            rel_integr_proc.dat_processing
        ) scai_execution
      where
        a.opr_source_system = b.opr_source_system
        and cod_country = 4
  ) source_table,
  (select coalesce(max(cod_sku),0) max_cod from crm_integration_anlt.t_lkp_product) max_cod_sku,
  crm_integration_anlt.t_lkp_product target
where
  source_table.opr_sku = target.opr_sku (+)
  and source_table.cod_source_system = target.cod_source_system (+);
  
analyze tmp_ro_load_product;



insert into crm_integration_anlt.t_lkp_product
    select
      (max_cod + new_cod) cod_sku,
      opr_sku,
      dsc_sku,
      (select rel_integr_proc.dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_product') valid_from, 
      20991231 valid_to,
      cod_source_system,
	  cod_execution
    from
      tmp_ro_load_product
    where
      dml_type = 'I';



analyze crm_integration_anlt.t_lkp_product;

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
    (select coalesce(max(cod_execution),0) max_cod_exec from crm_integration_anlt.t_fac_scai_execution),
    crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
    crm_integration_anlt.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 4 -- Romania
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_lkp_product';

--$$$

-- #######################
-- ####    PASSO 6    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set cod_status = 1, -- Ok
last_processing_datetime = coalesce(sysdate,last_processing_datetime)
/*from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc, crm_integration_anlt.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_product'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 4
  ) source*/
from crm_integration_anlt.t_lkp_scai_process proc 
where t_rel_scai_integration_process.cod_process = proc.cod_process
and t_rel_scai_integration_process.cod_status = 2
and t_rel_scai_integration_process.cod_country = 4
and proc.dsc_process_short = 't_lkp_product'
and t_rel_scai_integration_process.ind_active = 1
/*crm_integration_anlt.t_rel_scai_integration_process.cod_process = source.cod_process
and crm_integration_anlt.t_rel_scai_integration_process.cod_country = source.cod_country
and crm_integration_anlt.t_rel_scai_integration_process.cod_integration = source.cod_integration*/;



--$$$

-- #######################
-- ####    PASSO 3    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set dat_processing = source.dat_processing, execution_nbr = source.execution_nbr, cod_status = 2 -- Running
from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc, crm_integration_anlt.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_currency'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 4
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
    (select coalesce(max(cod_execution),0) max_cod_exec from crm_integration_anlt.t_fac_scai_execution),
    crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
    crm_integration_anlt.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 4 -- Romania
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_lkp_currency';

--$$$
	
-- #############################################
-- # 			 BASE - Romania                #
-- #		LOADING t_lkp_currency (SCD1)      #
-- #############################################



create temp table tmp_ro_load_currency
as
select
  row_number() over (order by source_table.opr_currency) new_cod,
  source_table.opr_currency,
  source_table.opr_currency dsc_currency,
  -1 cod_source_system,
  source_table.cod_execution,
  source_table.dat_processing,
  max_cod_currency.max_cod,  
  case
    when target.opr_currency is null then 'I'
    else 'X'
  end dml_type
  from
    (
      select
        currency opr_currency,
        scai_execution.cod_execution,
        scai_execution.dat_processing
      from
        (
          select distinct currency from crm_integration_stg.stg_ro_d_base_line_items where currency is not null
        ) a,
        (
          select
            rel_integr_proc.dat_processing,
            max(fac.cod_execution) cod_execution
          from
            crm_integration_anlt.t_lkp_scai_process proc,
            crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
            crm_integration_anlt.t_fac_scai_execution fac
          where
            rel_integr_proc.cod_process = proc.cod_process
            and rel_integr_proc.cod_country = 4
            and rel_integr_proc.cod_integration = 30000
            and rel_integr_proc.ind_active = 1
            and proc.dsc_process_short = 't_lkp_currency'
            and fac.cod_process = rel_integr_proc.cod_process
            and fac.cod_integration = rel_integr_proc.cod_integration
            and rel_integr_proc.dat_processing = fac.dat_processing
            and fac.cod_status = 2
          group by
            rel_integr_proc.dat_processing
        ) scai_execution
  ) source_table,
  (select coalesce(max(cod_currency),0) max_cod from crm_integration_anlt.t_lkp_currency) max_cod_currency,
  crm_integration_anlt.t_lkp_currency target
where
  source_table.opr_currency = target.opr_currency (+);

analyze tmp_ro_load_currency;



insert into crm_integration_anlt.t_lkp_currency
    select
      (max_cod + new_cod) cod_currency,
      opr_currency,
      dsc_currency,
      (select rel_integr_proc.dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_currency') valid_from, 
      20991231 valid_to,
      cod_source_system,
	  cod_execution
    from
      tmp_ro_load_currency
    where
      dml_type = 'I';


	  
analyze crm_integration_anlt.t_lkp_currency;

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
    (select coalesce(max(cod_execution),0) max_cod_exec from crm_integration_anlt.t_fac_scai_execution),
    crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
    crm_integration_anlt.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 4 -- Romania
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_lkp_currency';

--$$$
	
-- #######################
-- ####    PASSO 6    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set cod_status = 1, -- Ok
last_processing_datetime = coalesce(sysdate,last_processing_datetime)
/*from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc, crm_integration_anlt.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_currency'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 4
  ) source*/
from crm_integration_anlt.t_lkp_scai_process proc 
where t_rel_scai_integration_process.cod_process = proc.cod_process
and t_rel_scai_integration_process.cod_status = 2
and t_rel_scai_integration_process.cod_country = 4
and proc.dsc_process_short = 't_lkp_currency'
and t_rel_scai_integration_process.ind_active = 1
/*crm_integration_anlt.t_rel_scai_integration_process.cod_process = source.cod_process
and crm_integration_anlt.t_rel_scai_integration_process.cod_country = source.cod_country
and crm_integration_anlt.t_rel_scai_integration_process.cod_integration = source.cod_integration*/;



--$$$

-- #######################
-- ####    PASSO 3    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set dat_processing = source.dat_processing, execution_nbr = source.execution_nbr, cod_status = 2 -- Running
from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc, crm_integration_anlt.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_source'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 4
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
    (select coalesce(max(cod_execution),0) max_cod_exec from crm_integration_anlt.t_fac_scai_execution),
    crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
    crm_integration_anlt.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 4 -- Romania
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_lkp_source';

--$$$
	
-- #############################################
-- # 			 ATLAS - Romania               #
-- #		LOADING t_lkp_source (SCD1)        #
-- #############################################


create temp table tmp_ro_load_source
as
select
  row_number() over (order by source_table.opr_source) new_cod,
  source_table.opr_source,
  source_table.opr_source dsc_source,
  -1 cod_source_system,
  source_table.cod_execution,
  source_table.dat_processing,
  max_cod_source.max_cod,
  case
    when target.opr_source is null then 'I'
    else 'X'
  end dml_type
  from
    (
      select
        source opr_source,
        scai_execution.cod_execution,
        scai_execution.dat_processing
      from
        (
          select distinct source from db_atlas_verticals.ads, crm_integration_anlt.t_lkp_source_system where source is not null and livesync_dbname = opr_source_system and cod_country = 4
          union
          select distinct source from db_atlas.olxro_ads where source is not null
          union
          select distinct source from db_atlas_verticals.payment_session, crm_integration_anlt.t_lkp_source_system where source is not null and livesync_dbname = opr_source_system and cod_country = 4
          union
          select distinct source from db_atlas.olxro_payment_session where source is not null
          union
          select distinct source from db_atlas_verticals.answers, crm_integration_anlt.t_lkp_source_system where source is not null and livesync_dbname = opr_source_system and cod_country = 4
          union
          select distinct source from db_atlas.olxro_answers where source is not null
        ) a,
        (
          select
            rel_integr_proc.dat_processing,
            max(fac.cod_execution) cod_execution
          from
            crm_integration_anlt.t_lkp_scai_process proc,
            crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
            crm_integration_anlt.t_fac_scai_execution fac
          where
            rel_integr_proc.cod_process = proc.cod_process
            and rel_integr_proc.cod_country = 4
            and rel_integr_proc.cod_integration = 30000
            and rel_integr_proc.ind_active = 1
            and proc.dsc_process_short = 't_lkp_source'
            and fac.cod_process = rel_integr_proc.cod_process
            and fac.cod_integration = rel_integr_proc.cod_integration
            and rel_integr_proc.dat_processing = fac.dat_processing
            and fac.cod_status = 2
          group by
            rel_integr_proc.dat_processing
        ) scai_execution
  ) source_table,
  (select coalesce(max(cod_source),0) max_cod from crm_integration_anlt.t_lkp_source) max_cod_source,
  crm_integration_anlt.t_lkp_source target
where
  source_table.opr_source = target.opr_source (+);
  
analyze tmp_ro_load_source;



insert into crm_integration_anlt.t_lkp_source
    select
      (max_cod + new_cod) cod_source,
      opr_source,
      dsc_source,
      (select rel_integr_proc.dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_source') valid_from, 
      20991231 valid_to,
      cod_source_system,
	  cod_execution
    from
      tmp_ro_load_source
    where
      dml_type = 'I';



analyze crm_integration_anlt.t_lkp_source;

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
    (select coalesce(max(cod_execution),0) max_cod_exec from crm_integration_anlt.t_fac_scai_execution),
    crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
    crm_integration_anlt.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 4 -- Romania
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_lkp_source';

--$$$
	
-- #######################
-- ####    PASSO 6    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set cod_status = 1, -- Ok
last_processing_datetime = coalesce(sysdate,last_processing_datetime)
/*from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc, crm_integration_anlt.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_source'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 4
  ) source*/
from crm_integration_anlt.t_lkp_scai_process proc 
where t_rel_scai_integration_process.cod_process = proc.cod_process
and t_rel_scai_integration_process.cod_status = 2
and t_rel_scai_integration_process.cod_country = 4
and proc.dsc_process_short = 't_lkp_source'
and t_rel_scai_integration_process.ind_active = 1
/*crm_integration_anlt.t_rel_scai_integration_process.cod_process = source.cod_process
and crm_integration_anlt.t_rel_scai_integration_process.cod_country = source.cod_country
and crm_integration_anlt.t_rel_scai_integration_process.cod_integration = source.cod_integration*/;



--$$$

-- #######################
-- ####    PASSO 3    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set dat_processing = source.dat_processing, execution_nbr = source.execution_nbr, cod_status = 2 -- Running
from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc, crm_integration_anlt.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_event'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 4
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
    (select coalesce(max(cod_execution),0) max_cod_exec from crm_integration_anlt.t_fac_scai_execution),
    crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
    crm_integration_anlt.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 4 -- Romania
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_lkp_event';

--$$$

-- #############################################
-- # 			 ATLAS - Romania               #
-- #		LOADING t_lkp_event (SCD1)         #
-- #############################################



create temp table tmp_ro_load_event
as
select
  row_number() over (order by source_table.opr_event) new_cod,
  source_table.opr_event,
  source_table.opr_event dsc_event,
  -1 cod_source_system,
  source_table.cod_execution,
  source_table.dat_processing,
  max_cod_event.max_cod,
  case
    when target.opr_event is null then 'I'
    else 'X'
  end dml_type
  from
    (
      select
        event_type opr_event,
        scai_execution.cod_execution,
        scai_execution.dat_processing
      from
        (
          select distinct action_type event_type from crm_integration_stg.stg_ro_hydra_web where action_type is not null
          union
          select distinct trackname event_type from crm_integration_stg.stg_ro_hydra_verticals_web where trackname is not null
        ) a,
        (
          select
            rel_integr_proc.dat_processing,
            max(fac.cod_execution) cod_execution
          from
            crm_integration_anlt.t_lkp_scai_process proc,
            crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
            crm_integration_anlt.t_fac_scai_execution fac
          where
            rel_integr_proc.cod_process = proc.cod_process
            and rel_integr_proc.cod_country = 4
            and rel_integr_proc.cod_integration = 30000
            and rel_integr_proc.ind_active = 1
            and proc.dsc_process_short = 't_lkp_event'
            and fac.cod_process = rel_integr_proc.cod_process
            and fac.cod_integration = rel_integr_proc.cod_integration
            and rel_integr_proc.dat_processing = fac.dat_processing
            and fac.cod_status = 2
          group by
            rel_integr_proc.dat_processing
        ) scai_execution
  ) source_table,
  (select coalesce(max(cod_event),0) max_cod from crm_integration_anlt.t_lkp_event) max_cod_event,
  crm_integration_anlt.t_lkp_event target
where
  source_table.opr_event = target.opr_event (+);

analyze tmp_ro_load_event;



insert into crm_integration_anlt.t_lkp_event
    select
      (max_cod + new_cod) cod_event,
      opr_event,
      dsc_event,
      (select rel_integr_proc.dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_event') valid_from, 
      20991231 valid_to,
      cod_source_system,
	  cod_execution
    from
      tmp_ro_load_event
    where
      dml_type = 'I';


	  
analyze crm_integration_anlt.t_lkp_event;
  
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
    (select coalesce(max(cod_execution),0) max_cod_exec from crm_integration_anlt.t_fac_scai_execution),
    crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
    crm_integration_anlt.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 4 -- Romania
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_lkp_event';

--$$$
	
-- #######################
-- ####    PASSO 6    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set cod_status = 1, -- Ok
last_processing_datetime = coalesce(sysdate,last_processing_datetime)
/*from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc, crm_integration_anlt.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_event'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 4
  ) source*/
from crm_integration_anlt.t_lkp_scai_process proc 
where t_rel_scai_integration_process.cod_process = proc.cod_process
and t_rel_scai_integration_process.cod_status = 2
and t_rel_scai_integration_process.cod_country = 4
and proc.dsc_process_short = 't_lkp_event'
and t_rel_scai_integration_process.ind_active = 1
/*crm_integration_anlt.t_rel_scai_integration_process.cod_process = source.cod_process
and crm_integration_anlt.t_rel_scai_integration_process.cod_country = source.cod_country
and crm_integration_anlt.t_rel_scai_integration_process.cod_integration = source.cod_integration*/;



--$$$

-- #######################
-- ####    PASSO 3    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set dat_processing = source.dat_processing, execution_nbr = source.execution_nbr, cod_status = 2 -- Running
from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc, crm_integration_anlt.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_base_source'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 4
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
    (select coalesce(max(cod_execution),0) max_cod_exec from crm_integration_anlt.t_fac_scai_execution),
    crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
    crm_integration_anlt.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 4 -- Romania
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_lkp_base_source';		

--$$$	

-- #############################################
-- # 			 BASE - Romania                #
-- #		 LOADING t_lkp_base_source         #
-- #############################################


create temp table tmp_ro_load_base_source 
distkey(cod_source_system)
sortkey(cod_base_source, opr_base_source)
as
  select
    source_table.opr_base_source,
    source_table.dsc_base_source,
    coalesce(lkp_resource_type.cod_resource_type,-2) cod_resource_type,
    source_table.cod_source_system,
	source_table.meta_event_type,
	source_table.meta_event_time,
    source_table.created_at,
    source_table.updated_at,
    source_table.hash_base_source,
    source_table.cod_execution,
    max_cod_base_source.max_cod,
    row_number() over (order by source_table.opr_base_source desc) new_cod,
    target.cod_base_source,
	target.valid_from,
    case
      --when target.cod_base_source is null then 'I'
	  when target.cod_base_source is null or (source_table.hash_base_source != target.hash_base_source and target.valid_from = source_table.dat_processing) then 'I'
	  when source_table.meta_event_type = 'deleted' then 'D'
      when source_table.hash_base_source != target.hash_base_source then 'U'
        else 'X'
    end dml_type
  from
    (
	select
		source.*,
		lkp_source_system.cod_source_system,
		md5
		(coalesce(dsc_base_source,'') + coalesce(opr_resource_type,'')) hash_base_source
	from
	(
      SELECT
        id opr_base_source,
        name dsc_base_source,
        resource_type opr_resource_type,
        base_account_country + base_account_category opr_source_system,
		meta_event_type,
		meta_event_time,
        created_at,
        updated_at,
        scai_execution.cod_execution,
        scai_execution.dat_processing
      FROM
        crm_integration_stg.stg_ro_d_base_sources,
        (
          select
            rel_integr_proc.dat_processing,
            max(fac.cod_execution) cod_execution
          from
            crm_integration_anlt.t_lkp_scai_process proc,
            crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
            crm_integration_anlt.t_fac_scai_execution fac
          where
            rel_integr_proc.cod_process = proc.cod_process
            and rel_integr_proc.cod_country = 4
            and rel_integr_proc.cod_country = fac.cod_country
            and rel_integr_proc.cod_integration = 30000
            and rel_integr_proc.ind_active = 1
            and proc.dsc_process_short = 't_lkp_base_source'
            and fac.cod_process = rel_integr_proc.cod_process
            and fac.cod_integration = rel_integr_proc.cod_integration
            and rel_integr_proc.dat_processing = fac.dat_processing
            and fac.cod_status = 2
          group by
            rel_integr_proc.dat_processing
        ) scai_execution
	) source,
    crm_integration_anlt.t_lkp_source_system lkp_source_system
	where source.opr_source_system = lkp_source_system.opr_source_system
	and lkp_source_system.cod_country = 4 -- Romania
	) source_table,
    crm_integration_anlt.t_lkp_resource_type lkp_resource_type,
    (select coalesce(max(cod_base_source),0) max_cod from crm_integration_anlt.t_lkp_base_source) max_cod_base_source,
    (
			select
				*
			from
				(
					SELECT
						a.*,
						row_number()
						OVER (
							PARTITION BY opr_base_source, cod_source_system
							ORDER BY valid_to DESC ) rn
					FROM
						crm_integration_anlt.t_lkp_base_source a
				)
			where rn = 1
	) target
  where
    coalesce(source_table.opr_base_source,-1) = target.opr_base_source(+)
	and source_table.cod_source_system = target.cod_source_system (+)
    and coalesce(source_table.opr_resource_type,'Unknown') = lkp_resource_type.opr_resource_type (+)
	and lkp_resource_type.valid_to (+) = 20991231;

analyze tmp_ro_load_base_source;
	

	
delete from crm_integration_anlt.t_lkp_base_source
using tmp_ro_load_base_source
where 
	tmp_ro_load_base_source.dml_type = 'I' 
	and t_lkp_base_source.opr_base_source = tmp_ro_load_base_source.opr_base_source 
	and t_lkp_base_source.valid_from = (select dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_base_source');


	
update crm_integration_anlt.t_lkp_base_source
set valid_to = (select rel_integr_proc.dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_base_source')
from tmp_ro_load_base_source source
where source.cod_base_source = crm_integration_anlt.t_lkp_base_source.cod_base_source
and crm_integration_anlt.t_lkp_base_source.valid_to = 20991231
and source.dml_type in('U','D');



insert into crm_integration_anlt.t_lkp_base_source
    select
      case
        when dml_type = 'I' then case when valid_from = (select dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc
														where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_base_source')
									then cod_base_source else max_cod + new_cod end
        when dml_type = 'U' then cod_base_source
      end cod_base_source,
      opr_base_source,
      dsc_base_source,
      cod_source_system,
      (select rel_integr_proc.dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_base_source') valid_from, 
      20991231 valid_to,
      created_at,
      updated_at,
      cod_resource_type,
      hash_base_source,
	  cod_execution
    from
      tmp_ro_load_base_source
    where
      dml_type in ('U','I');



analyze crm_integration_anlt.t_lkp_base_source;
	  
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
    (select coalesce(max(cod_execution),0) max_cod_exec from crm_integration_anlt.t_fac_scai_execution),
    crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
    crm_integration_anlt.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 4 -- Romania
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_lkp_base_source';

--$$$

-- #######################
-- ####    PASSO 6    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set cod_status = 1, -- Ok
last_processing_datetime = coalesce((select max(updated_at) from tmp_ro_load_base_source),last_processing_datetime)
/*from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc, crm_integration_anlt.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_base_source'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 4
  ) source*/
from crm_integration_anlt.t_lkp_scai_process proc 
where t_rel_scai_integration_process.cod_process = proc.cod_process
and t_rel_scai_integration_process.cod_status = 2
and t_rel_scai_integration_process.cod_country = 4
and proc.dsc_process_short = 't_lkp_base_source'
and t_rel_scai_integration_process.ind_active = 1
/*crm_integration_anlt.t_rel_scai_integration_process.cod_process = source.cod_process
and crm_integration_anlt.t_rel_scai_integration_process.cod_country = source.cod_country
and crm_integration_anlt.t_rel_scai_integration_process.cod_integration = source.cod_integration*/;



--$$$

-- #######################
-- ####    PASSO 3    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set dat_processing = source.dat_processing, execution_nbr = source.execution_nbr, cod_status = 2 -- Running
from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc, crm_integration_anlt.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_base_user'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 4
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
    (select coalesce(max(cod_execution),0) max_cod_exec from crm_integration_anlt.t_fac_scai_execution),
    crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
    crm_integration_anlt.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 4 -- Romania
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_lkp_base_user';	

--$$$
	
-- #############################################
-- # 			 BASE - Romania                #
-- #		 LOADING t_lkp_base_user           #
-- #############################################


create temp table tmp_ro_load_base_user 
distkey(cod_source_system)
sortkey(cod_base_user, opr_base_user)
as
  select
    source_table.opr_base_user,
    source_table.dsc_base_user,
    source_table.cod_source_system,
    source_table.email,
    source_table.role,
    source_table.status,
    source_table.flg_confirmed,
	source_table.flg_invited,
	source_table.phone_number,
	source_table.roles,
	source_table.team_name,
	case
		when source_table."group" = '' then -1
		else cast(substring(split_part(source_table."group",'"',3),2,len(split_part(source_table."group",'"',3))-2) as bigint)
	end opr_group,
	split_part(source_table."group",'"',6) dsc_group,
	target_base_user_responsible.cod_base_user cod_base_user_responsible,
	source_table.timezone,
	source_table.meta_event_type,
	source_table.meta_event_time,
    source_table.created_at,
    source_table.updated_at,
    source_table.deleted_at,
    source_table.hash_base_user,
    source_table.cod_execution,
    max_cod_base_user.max_cod,
    row_number() over (order by source_table.opr_base_user desc) new_cod,
    target.cod_base_user,
	target.valid_from,
    case
      --when target.cod_base_user is null then 'I'
	  when target.cod_base_user is null or (source_table.hash_base_user != target.hash_base_user and target.valid_from = source_table.dat_processing) then 'I'
	  when source_table.meta_event_type = 'deleted' then 'D'
      when source_table.hash_base_user != target.hash_base_user then 'U'
        else 'X'
    end dml_type
  from
    (
	select
		 source.*,
		lkp_source_system.cod_source_system,
		md5(coalesce(dsc_base_user,'') + coalesce(email,'') + coalesce(role,'') + coalesce(status,'') + decode(flg_confirmed, 1, 1, 0) +
			decode(flg_invited, 1, 1, 0) + coalesce(phone_number,'') + coalesce(roles,'') + coalesce(team_name,'') + coalesce("group",'') + coalesce(reports_to,'-1') + coalesce(timezone,'')) hash_base_user
	from
	(
      SELECT
        id opr_base_user,
        name dsc_base_user,
        base_account_country + base_account_category opr_source_system,
		meta_event_type,
		meta_event_time,
        email,
        role,
        status,
        confirmed flg_confirmed,
		invited flg_invited,
		phone_number,
		roles,
		team_name,
		"group",
		reports_to,
		timezone,
        created_at,
        updated_at,
        deleted_at,
        scai_execution.cod_execution,
        scai_execution.dat_processing
      FROM
        crm_integration_stg.stg_ro_d_base_users,
        (
          select
            rel_integr_proc.dat_processing,
            max(fac.cod_execution) cod_execution
          from
            crm_integration_anlt.t_lkp_scai_process proc,
            crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
            crm_integration_anlt.t_fac_scai_execution fac
          where
            rel_integr_proc.cod_process = proc.cod_process
            and rel_integr_proc.cod_country = 4
            and rel_integr_proc.cod_country = fac.cod_country
            and rel_integr_proc.cod_integration = 30000
            and rel_integr_proc.ind_active = 1
            and proc.dsc_process_short = 't_lkp_base_user'
            and fac.cod_process = rel_integr_proc.cod_process
            and fac.cod_integration = rel_integr_proc.cod_integration
            and rel_integr_proc.dat_processing = fac.dat_processing
            and fac.cod_status = 2
          group by
            rel_integr_proc.dat_processing
        ) scai_execution
	) source,
    crm_integration_anlt.t_lkp_source_system lkp_source_system
	where source.opr_source_system = lkp_source_system.opr_source_system
	and lkp_source_system.cod_country = 4
	) source_table,
    (select coalesce(max(cod_base_user),0) max_cod from crm_integration_anlt.t_lkp_base_user) max_cod_base_user,
    (
			select
				*
			from
				(
					SELECT
						a.*,
						row_number()
						OVER (
							PARTITION BY opr_base_user, cod_source_system
							ORDER BY valid_to DESC ) rn
					FROM
						crm_integration_anlt.t_lkp_base_user a
				)
			where rn = 1
	) target,
    (
			select
				*
			from
				(
					SELECT
						a.*,
						row_number()
						OVER (
							PARTITION BY opr_base_user, cod_source_system
							ORDER BY valid_to DESC ) rn
					FROM
						crm_integration_anlt.t_lkp_base_user a
				)
			where rn = 1
	) target_base_user_responsible
  where
    coalesce(source_table.opr_base_user,-1) = target.opr_base_user(+)
	and source_table.cod_source_system = target.cod_source_system (+)
	and coalesce(source_table.reports_to,-1) = target_base_user_responsible.opr_base_user(+)
	and source_table.cod_source_system = target_base_user_responsible.cod_source_system (+); -- Romania

analyze tmp_ro_load_base_user;


	
delete from crm_integration_anlt.t_lkp_base_user
using tmp_ro_load_base_user
where 
	tmp_ro_load_base_user.dml_type = 'I' 
	and t_lkp_base_user.opr_base_user = tmp_ro_load_base_user.opr_base_user 
	and t_lkp_base_user.valid_from = (select dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_base_user');



-- update valid_to in the updated/deleted records on source	
update crm_integration_anlt.t_lkp_base_user
set valid_to = (select rel_integr_proc.dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_base_user') 
from tmp_ro_load_base_user source
where source.cod_base_user = crm_integration_anlt.t_lkp_base_user.cod_base_user
and crm_integration_anlt.t_lkp_base_user.valid_to = 20991231
and source.dml_type in('U','D');



insert into crm_integration_anlt.t_lkp_base_user
    select
      case
        when dml_type = 'I' then case when valid_from = (select dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc
														where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_base_user')
									then cod_base_user else max_cod + new_cod end
        when dml_type = 'U' then cod_base_user
      end cod_base_user,
      opr_base_user,
      dsc_base_user,
      cod_source_system,
      (select rel_integr_proc.dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_base_user') valid_from, 
      20991231 valid_to,
      email,
      role,
      status,
      decode(flg_confirmed,1,1,0) flg_confirmed,
	  decode(flg_invited,1,1,0) flg_invited,
	  phone_number,
	  roles,
	  team_name,
	  opr_group,
	  dsc_group,
	  cod_base_user_responsible,
	  timezone,
      created_at, 
      updated_at,
      deleted_at,
      hash_base_user,
	  cod_execution
    from
      tmp_ro_load_base_user
    where
      dml_type in ('U','I');



analyze crm_integration_anlt.t_lkp_base_user;
	  
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
    (select coalesce(max(cod_execution),0) max_cod_exec from crm_integration_anlt.t_fac_scai_execution),
    crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
    crm_integration_anlt.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 4 -- Romania
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_lkp_base_user';

--$$$

-- #######################
-- ####    PASSO 6    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set cod_status = 1, -- Ok
last_processing_datetime = coalesce((select max(updated_at) from tmp_ro_load_base_user),last_processing_datetime)
/*from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc, crm_integration_anlt.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_base_user'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 4
  ) source*/
from crm_integration_anlt.t_lkp_scai_process proc 
where t_rel_scai_integration_process.cod_process = proc.cod_process
and t_rel_scai_integration_process.cod_status = 2
and t_rel_scai_integration_process.cod_country = 4
and proc.dsc_process_short = 't_lkp_base_user'
and t_rel_scai_integration_process.ind_active = 1
/*crm_integration_anlt.t_rel_scai_integration_process.cod_process = source.cod_process
and crm_integration_anlt.t_rel_scai_integration_process.cod_country = source.cod_country
and crm_integration_anlt.t_rel_scai_integration_process.cod_integration = source.cod_integration*/;



--$$$

-- #######################
-- ####    PASSO 3    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set dat_processing = source.dat_processing, execution_nbr = source.execution_nbr, cod_status = 2 -- Running
from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc, crm_integration_anlt.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_task'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 4
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
    (select coalesce(max(cod_execution),0) max_cod_exec from crm_integration_anlt.t_fac_scai_execution),
    crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
    crm_integration_anlt.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 4 -- Romania
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_lkp_task';		

--$$$
	
-- #############################################
-- # 			  BASE - Romania               #
-- #		    LOADING t_lkp_task             #
-- #############################################


create temp table tmp_ro_load_task 
distkey(cod_source_system)
sortkey(cod_task, opr_task)
as
  select
    source_table.opr_task,
    coalesce(lkp_base_user_owner.cod_base_user,-2) cod_base_user_owner,
    coalesce(lkp_base_user_creator.cod_base_user,-2) cod_base_user_creator,
    coalesce(lkp_resource_type.cod_resource_type,-2) cod_resource_type,
    source_table.cod_source_system,
	source_table.resource_id,
	source_table.flg_completed,
	source_table.completed_at,
	source_table.due_date,
	source_table.flg_overdue,
	source_table.remind_at,
	source_table.content,
	source_table.reminder_offset,
	source_table.meta_event_type,
	source_table.meta_event_time,
    source_table.created_at,
    source_table.updated_at,
    source_table.hash_task,
    source_table.cod_execution,
    max_cod_task.max_cod,
    row_number() over (order by source_table.opr_task desc) new_cod,
    target.cod_task,
	target.valid_from,
    case
      --when target.cod_task is null then 'I'
	  when target.cod_task is null or (source_table.hash_task != target.hash_task and target.valid_from = source_table.dat_processing) then 'I'
	  when source_table.meta_event_type = 'deleted' then 'D'
      when source_table.hash_task != target.hash_task then 'U'
        else 'X'
    end dml_type
  from
    (
	select
		 source.*,
		lkp_source_system.cod_source_system,
		md5(coalesce(opr_base_user_creator,-1) + coalesce(opr_base_user_owner,-1) + coalesce(opr_resource_type,'') + coalesce(resource_id,-1) + decode(flg_completed, 1, 1, 0) + decode(flg_overdue, 1, 1, 0) + coalesce(content,'')) hash_task
    FROM
	(
      SELECT
        id opr_task,
        base_account_country + base_account_category opr_source_system,
        creator_id opr_base_user_creator,
		owner_id opr_base_user_owner,
        resource_type opr_resource_type,
		resource_id,
		completed flg_completed,
		completed_at,
		due_date,
		overdue flg_overdue,
		remind_at,
		content,
		reminder_offset,
		meta_event_type,
		meta_event_time,
        created_at,
        updated_at,
        scai_execution.cod_execution,
        scai_execution.dat_processing
      FROM
        crm_integration_stg.stg_ro_d_base_tasks,
        (
          select
            rel_integr_proc.dat_processing,
            max(fac.cod_execution) cod_execution
          from
            crm_integration_anlt.t_lkp_scai_process proc,
            crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
            crm_integration_anlt.t_fac_scai_execution fac
          where
            rel_integr_proc.cod_process = proc.cod_process
            and rel_integr_proc.cod_country = 4
            and rel_integr_proc.cod_country = fac.cod_country
            and rel_integr_proc.cod_integration = 30000
            and rel_integr_proc.ind_active = 1
            and proc.dsc_process_short = 't_lkp_task'
            and fac.cod_process = rel_integr_proc.cod_process
            and fac.cod_integration = rel_integr_proc.cod_integration
            and rel_integr_proc.dat_processing = fac.dat_processing
            and fac.cod_status = 2
          group by
            rel_integr_proc.dat_processing
        ) scai_execution
	) source,
    crm_integration_anlt.t_lkp_source_system lkp_source_system
	where source.opr_source_system = lkp_source_system.opr_source_system
	and lkp_source_system.cod_country = 4 -- Romania
	) source_table,
	crm_integration_anlt.t_lkp_base_user lkp_base_user_creator,
	crm_integration_anlt.t_lkp_base_user lkp_base_user_owner,
    crm_integration_anlt.t_lkp_resource_type lkp_resource_type,
    (select coalesce(max(cod_task),0) max_cod from crm_integration_anlt.t_lkp_task) max_cod_task,
    (
			select
				*
			from
				(
					SELECT
						a.*,
						row_number()
						OVER (
							PARTITION BY opr_task, cod_source_system
							ORDER BY valid_to DESC ) rn
					FROM
						crm_integration_anlt.t_lkp_task a
				)
			where rn = 1
	) target
  where
    coalesce(source_table.opr_task,-1) = target.opr_task(+)
	and source_table.cod_source_system = target.cod_source_system (+)
	and coalesce(source_table.opr_base_user_owner,'-1') = lkp_base_user_owner.opr_base_user (+)
	and source_table.cod_source_system = lkp_base_user_owner.cod_source_system (+) -- new
	and lkp_base_user_owner.valid_to (+) = 20991231
    and coalesce(source_table.opr_base_user_creator,-1) = lkp_base_user_creator.opr_base_user (+)
	and source_table.cod_source_system = lkp_base_user_creator.cod_source_system (+) -- new
	and lkp_base_user_creator.valid_to (+) = 20991231
    and coalesce(source_table.opr_resource_type,'Unknown') = lkp_resource_type.opr_resource_type (+)
	and lkp_resource_type.valid_to (+) = 20991231;

analyze tmp_ro_load_task;
	

	
delete from crm_integration_anlt.t_lkp_task
using tmp_ro_load_task
where 
	tmp_ro_load_task.dml_type = 'I' 
	and t_lkp_task.opr_task = tmp_ro_load_task.opr_task 
	and t_lkp_task.valid_from = (select dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_task');

	
update crm_integration_anlt.t_lkp_task
set valid_to = (select rel_integr_proc.dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_task')
from tmp_ro_load_task source
where source.cod_task = crm_integration_anlt.t_lkp_task.cod_task
and crm_integration_anlt.t_lkp_task.valid_to = 20991231
and source.dml_type in('U','D');


insert into crm_integration_anlt.t_lkp_task
    select
      case
        when dml_type = 'I' then case when valid_from = (select dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc
														where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_task')
									then cod_task else max_cod + new_cod end
        when dml_type = 'U' then cod_task
      end cod_task,
      opr_task,
      (select rel_integr_proc.dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_task') valid_from, 
      20991231 valid_to,
	  cod_base_user_creator,
	  cod_base_user_owner,
	  cod_resource_type,
	  resource_id,
	  flg_completed,
	  completed_at,
	  due_date,
	  flg_overdue,
	  remind_at,
	  content,
	  reminder_offset,
	  cod_source_system,
	  hash_task,
	  cod_execution
    from
      tmp_ro_load_task
    where
      dml_type in ('U','I');



analyze crm_integration_anlt.t_lkp_task;
	 
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
    (select coalesce(max(cod_execution),0) max_cod_exec from crm_integration_anlt.t_fac_scai_execution),
    crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
    crm_integration_anlt.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 4 -- Romania
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_lkp_task';

--$$$

-- #######################
-- ####    PASSO 6    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set cod_status = 1, -- Ok
last_processing_datetime = coalesce((select max(updated_at) from tmp_ro_load_task),last_processing_datetime)
/*from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc, crm_integration_anlt.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_task'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 4
  ) source*/
from crm_integration_anlt.t_lkp_scai_process proc 
where t_rel_scai_integration_process.cod_process = proc.cod_process
and t_rel_scai_integration_process.cod_status = 2
and t_rel_scai_integration_process.cod_country = 4
and proc.dsc_process_short = 't_lkp_task'
and t_rel_scai_integration_process.ind_active = 1
/*crm_integration_anlt.t_rel_scai_integration_process.cod_process = source.cod_process
and crm_integration_anlt.t_rel_scai_integration_process.cod_country = source.cod_country
and crm_integration_anlt.t_rel_scai_integration_process.cod_integration = source.cod_integration*/;



--$$$

-- #######################
-- ####    PASSO 3    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set dat_processing = source.dat_processing, execution_nbr = source.execution_nbr, cod_status = 2 -- Running
from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc, crm_integration_anlt.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_call_outcome'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 4
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
    (select coalesce(max(cod_execution),0) max_cod_exec from crm_integration_anlt.t_fac_scai_execution),
    crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
    crm_integration_anlt.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 4 -- Romania
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_lkp_call_outcome';

--$$$
	
-- #############################################
-- # 	          BASE - Romania               #
-- #        LOADING t_lkp_call_outcome         #
-- #############################################



create temp table tmp_ro_load_call_outcome 
distkey(cod_source_system)
sortkey(cod_call_outcome, opr_call_outcome)
as
  select source.*, coalesce(lkp_user_creator.cod_base_user,-2) cod_base_user_creator from(
select
    source_table.opr_call_outcome,
    source_table.dsc_call_outcome,
    source_table.cod_source_system,
	source_table.meta_event_type,
	source_table.meta_event_time,
    source_table.opr_base_user,
    source_table.created_at,
    source_table.updated_at,
    source_table.hash_call_outcome,
    source_table.cod_execution,
    max_cod_call_outcome.max_cod,
    row_number() over (order by source_table.opr_call_outcome desc) new_cod,
    target.cod_call_outcome,
	target.valid_from,
    case
      --when target.cod_call_outcome is null then 'I'
	  when target.cod_call_outcome is null or (source_table.hash_call_outcome != target.hash_call_outcome and target.valid_from = source_table.dat_processing) then 'I'
	  when source_table.meta_event_type = 'deleted' then 'D'
      when source_table.hash_call_outcome != target.hash_call_outcome then 'U'
        else 'X'
    end dml_type
  from
    (
	select
		source.*,
		lkp_source_system.cod_source_system,
		md5(coalesce(dsc_call_outcome,'') + coalesce(opr_base_user,0)) hash_call_outcome
	from
	(
      SELECT
        id opr_call_outcome,
        name dsc_call_outcome,
        base_account_country + base_account_category opr_source_system,
		meta_event_type,
		meta_event_time,
        creator_id opr_base_user,
        created_at,
        updated_at,
        scai_execution.cod_execution,
        scai_execution.dat_processing
      FROM
        crm_integration_stg.stg_ro_d_base_call_outcomes,
        (
          select
            rel_integr_proc.dat_processing,
            max(fac.cod_execution) cod_execution
          from
            crm_integration_anlt.t_lkp_scai_process proc,
            crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
            crm_integration_anlt.t_fac_scai_execution fac
          where
            rel_integr_proc.cod_process = proc.cod_process
            and rel_integr_proc.cod_country = 4
            and rel_integr_proc.cod_country = fac.cod_country
            and rel_integr_proc.cod_integration = 30000
            and rel_integr_proc.ind_active = 1
            and proc.dsc_process_short = 't_lkp_call_outcome'
            and fac.cod_process = rel_integr_proc.cod_process
            and fac.cod_integration = rel_integr_proc.cod_integration
            and rel_integr_proc.dat_processing = fac.dat_processing
            and fac.cod_status = 2
          group by
            rel_integr_proc.dat_processing
        ) scai_execution
	) source,
    crm_integration_anlt.t_lkp_source_system lkp_source_system
	where source.opr_source_system = lkp_source_system.opr_source_system
	and lkp_source_system.cod_country = 4 -- Romania
	) source_table,
    (select coalesce(max(cod_call_outcome),0) max_cod from crm_integration_anlt.t_lkp_call_outcome) max_cod_call_outcome,
    (
			select
				*
			from
				(
					SELECT
						a.*,
						row_number()
						OVER (
							PARTITION BY opr_call_outcome, cod_source_system
							ORDER BY valid_to DESC ) rn
					FROM
						crm_integration_anlt.t_lkp_call_outcome a
				)
			where rn = 1
	) target
  where
    coalesce(source_table.opr_call_outcome,-1) = target.opr_call_outcome(+)
	and source_table.cod_source_system = target.cod_source_system (+)
    ) source, crm_integration_anlt.t_lkp_base_user lkp_user_creator
    where coalesce(source.opr_base_user,-1) = lkp_user_creator.opr_base_user (+)
	and source.cod_source_system = lkp_user_creator.cod_source_system (+) -- new
	and lkp_user_creator.valid_to (+) = 20991231;

analyze tmp_ro_load_call_outcome;


	
delete from crm_integration_anlt.t_lkp_call_outcome
using tmp_ro_load_call_outcome
where 
	tmp_ro_load_call_outcome.dml_type = 'I' 
	and t_lkp_call_outcome.opr_call_outcome = tmp_ro_load_call_outcome.opr_call_outcome 
	and t_lkp_call_outcome.valid_from = (select dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_call_outcome');


	
update crm_integration_anlt.t_lkp_call_outcome
set valid_to = (select rel_integr_proc.dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_call_outcome') 
from tmp_ro_load_call_outcome source
where source.cod_call_outcome = crm_integration_anlt.t_lkp_call_outcome.cod_call_outcome
and crm_integration_anlt.t_lkp_call_outcome.valid_to = 20991231
and source.dml_type in('U','D');


insert into crm_integration_anlt.t_lkp_call_outcome
    select
      case
        when dml_type = 'I' then case when valid_from = (select dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc
														where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_call_outcome')
									then cod_call_outcome else max_cod + new_cod end
        when dml_type = 'U' then cod_call_outcome
      end cod_call_outcome,
      opr_call_outcome,
      dsc_call_outcome,
      cod_source_system,
      (select rel_integr_proc.dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_call_outcome') valid_from, 
      20991231 valid_to,
      cod_base_user_creator,
      created_at,
      updated_at,
      hash_call_outcome,
	  cod_execution
    from
      tmp_ro_load_call_outcome
    where
      dml_type in ('U','I');



analyze crm_integration_anlt.t_lkp_call_outcome;
	  
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
    (select coalesce(max(cod_execution),0) max_cod_exec from crm_integration_anlt.t_fac_scai_execution),
    crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
    crm_integration_anlt.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 4 -- Romania
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_lkp_call_outcome';

--$$$

-- #######################
-- ####    PASSO 6    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set cod_status = 1, -- Ok
last_processing_datetime = coalesce((select max(updated_at) from tmp_ro_load_call_outcome),last_processing_datetime)
/*from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc, crm_integration_anlt.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_call_outcome'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 4
  ) source*/
from crm_integration_anlt.t_lkp_scai_process proc 
where t_rel_scai_integration_process.cod_process = proc.cod_process
and t_rel_scai_integration_process.cod_status = 2
and t_rel_scai_integration_process.cod_country = 4
and proc.dsc_process_short = 't_lkp_call_outcome'
and t_rel_scai_integration_process.ind_active = 1
/*crm_integration_anlt.t_rel_scai_integration_process.cod_process = source.cod_process
and crm_integration_anlt.t_rel_scai_integration_process.cod_country = source.cod_country
and crm_integration_anlt.t_rel_scai_integration_process.cod_integration = source.cod_integration*/;



--$$$

-- #######################
-- ####    PASSO 3    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set dat_processing = source.dat_processing, execution_nbr = source.execution_nbr, cod_status = 2 -- Running
from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc, crm_integration_anlt.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_contact'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 4
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
    (select coalesce(max(cod_execution),0) max_cod_exec from crm_integration_anlt.t_fac_scai_execution),
    crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
    crm_integration_anlt.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 4 -- Romania
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_lkp_contact';	

--$$$
	
-- #############################################
-- # 		     BASE - ROMANIA                #
-- #           LOADING t_lkp_contact           #
-- #############################################

drop table if exists crm_integration_anlt.tmp_ro_load_contact;

create table crm_integration_anlt.tmp_ro_load_contact 
distkey(cod_source_system)
sortkey(cod_contact, opr_contact)
as
select
    source_table.opr_contact,
    source_table.dsc_contact,
    source_table.cod_source_system,
	source_table.meta_event_type,
	source_table.meta_event_time,
	coalesce(lkp_base_user_creator.cod_base_user,-2) cod_base_user_creator,
	source_table.contact_id,
	source_table.created_at,
	source_table.updated_at,
	source_table.title,
	source_table.first_name,
	source_table.last_name,
	source_table.description,
	coalesce(lkp_industry.cod_industry,-2) cod_industry,
	source_table.website,
	source_table.email,
	source_table.phone,
	source_table.mobile,
	source_table.fax,
	source_table.twitter,
	source_table.facebook,
	source_table.linkedin,
	source_table.skype,
	coalesce(lkp_base_user_owner.cod_base_user,-2) cod_base_user_owner,
	source_table.flg_organization,
	source_table.address,
	source_table.custom_fields,
	source_table.customer_status,
	source_table.prospect_status,
	source_table.tags,
    source_table.hash_contact,
    source_table.cod_execution,
    max_cod_contacts.max_cod,
    row_number() over (order by source_table.opr_contact desc) new_cod,
    target.cod_contact,
	target.valid_from,
    case
      --when target.cod_contact is null then 'I'
	  when target.cod_contact is null or (source_table.hash_contact != target.hash_contact and target.valid_from = source_table.dat_processing) then 'I'
	  when source_table.meta_event_type = 'deleted' then 'D'
      when source_table.hash_contact != target.hash_contact then 'U'
        else 'X'
    end dml_type
  from
    (
	select
		source.*,
		lkp_source_system.cod_source_system,
		        md5(
		coalesce(dsc_contact                                                       ,'') +
		coalesce(meta_event_type                                                   ,'') +
		--coalesce(meta_event_time                                                   ,'2099-12-31 00:00:00.000000') +
		coalesce(opr_base_user_creator                                             ,-1) +
		coalesce(contact_id                                                        ,0) +
		--coalesce(created_at                                                        ,'2099-12-31') +
		--coalesce(updated_at                                                        ,'2099-12-31') +
		coalesce(title                                                             ,'') +
		coalesce(first_name                                                        ,'') +
		coalesce(last_name                                                         ,'') +
		coalesce(description                                                       ,'') +
		coalesce(opr_industry                                                      ,'') +
		coalesce(website                                                           ,'') +
		coalesce(email                                                             ,'') +
		coalesce(phone                                                             ,'') +
		coalesce(mobile                                                            ,'') +
		coalesce(fax                                                               ,'') +
		coalesce(twitter                                                           ,'') +
		coalesce(facebook                                                          ,'') +
		coalesce(linkedin                                                          ,'') +
		coalesce(skype                                                             ,'') +
		coalesce(opr_base_user_owner                                               ,'-1') +
		decode(flg_organization,1,1,0)                                           +
		coalesce(address                                                           ,'') +
		coalesce(custom_fields                                                     ,'') +
		coalesce(customer_status                                                   ,'') +
		coalesce(prospect_status                                                   ,'') +
		coalesce(tags                                                              ,'')
		) hash_contact
	from
	(
      SELECT DISTINCT
		id opr_contact,
		name dsc_contact,
		base_account_country + base_account_category opr_source_system,
		meta_event_type,
		meta_event_time,
		creator_id opr_base_user_creator,
		contact_id,
		created_at,
		updated_at,
		title,
		first_name,
		last_name,
		description,
		industry opr_industry,
		website,
		email,
		phone,
		mobile,
		fax,
		twitter,
		facebook,
		linkedin,
		skype,
		owner_id opr_base_user_owner,
		is_organization flg_organization,
		address,
		custom_fields,
		customer_status,
		prospect_status,
		tags,
		scai_execution.cod_execution,
		scai_execution.dat_processing
      FROM
        crm_integration_stg.stg_ro_d_base_contacts,
        (
          select
            rel_integr_proc.dat_processing,
            max(fac.cod_execution) cod_execution
          from
            crm_integration_anlt.t_lkp_scai_process proc,
            crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
            crm_integration_anlt.t_fac_scai_execution fac
          where
            rel_integr_proc.cod_process = proc.cod_process
            and rel_integr_proc.cod_country = 4
            and rel_integr_proc.cod_country = fac.cod_country
            and rel_integr_proc.cod_integration = 30000
            and rel_integr_proc.ind_active = 1
            and proc.dsc_process_short = 't_lkp_contact'
            and fac.cod_process = rel_integr_proc.cod_process
            and fac.cod_integration = rel_integr_proc.cod_integration
            and rel_integr_proc.dat_processing = fac.dat_processing
            and fac.cod_status = 2
          group by 
            rel_integr_proc.dat_processing
        ) scai_execution
	) source,
    crm_integration_anlt.t_lkp_source_system lkp_source_system
	where source.opr_source_system = lkp_source_system.opr_source_system
	and lkp_source_system.cod_country = 4 -- Romania
	) source_table,
    (select coalesce(max(cod_contact),0) max_cod from crm_integration_anlt.t_lkp_contact) max_cod_contacts,
	crm_integration_anlt.t_lkp_base_user lkp_base_user_creator,
	crm_integration_anlt.t_lkp_base_user lkp_base_user_owner,
	crm_integration_anlt.t_lkp_industry lkp_industry,
    (
			select
				*
			from
				(
					SELECT
						a.*,
						row_number()
						OVER (
							PARTITION BY opr_contact, cod_source_system
							ORDER BY valid_to DESC ) rn
					FROM
						crm_integration_anlt.t_lkp_contact a
				)
			where rn = 1
	) target
  where
	source_table.opr_contact = target.opr_contact(+)
	and source_table.cod_source_system = target.cod_source_system (+)
	and coalesce(source_table.opr_base_user_owner,'-1') = lkp_base_user_owner.opr_base_user (+)
	and source_table.cod_source_system = lkp_base_user_owner.cod_source_system (+) -- new
	and lkp_base_user_owner.valid_to (+) = 20991231
    and coalesce(source_table.opr_base_user_creator,-1) = lkp_base_user_creator.opr_base_user (+)
	and source_table.cod_source_system = lkp_base_user_creator.cod_source_system (+) -- new
	and lkp_base_user_creator.valid_to (+) = 20991231
    and coalesce(source_table.opr_industry,'Unknown') = lkp_industry.opr_industry (+)
	and source_table.cod_source_system = lkp_industry.cod_source_system (+) -- new
	and lkp_industry.valid_to (+) = 20991231;

analyze crm_integration_anlt.tmp_ro_load_contact;
	

	
delete from crm_integration_anlt.t_lkp_contact
using crm_integration_anlt.tmp_ro_load_contact
where 
	crm_integration_anlt.tmp_ro_load_contact.dml_type = 'I' 
	and t_lkp_contact.opr_contact = crm_integration_anlt.tmp_ro_load_contact.opr_contact 
	and t_lkp_contact.valid_from = (select dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_contact');


	
-- update valid_to in the updated/deleted records on source	
update crm_integration_anlt.t_lkp_contact
set valid_to = (select rel_integr_proc.dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_contact') 
from crm_integration_anlt.tmp_ro_load_contact source
where source.cod_contact = crm_integration_anlt.t_lkp_contact.cod_contact
and crm_integration_anlt.t_lkp_contact.valid_to = 20991231
and source.dml_type in('U','D');


	
insert into crm_integration_anlt.t_lkp_contact
    select
      case
        when dml_type = 'I' then case when valid_from = (select dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc
														where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_contact')
									then cod_contact else max_cod + new_cod end
        when dml_type = 'U' then cod_contact
      end cod_contact,
      opr_contact,
      dsc_contact,
      cod_source_system,
      (select rel_integr_proc.dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_contact') valid_from, 
      20991231 valid_to,
	  cod_base_user_creator cod_base_user,
    null cod_atlas_user,
	  contact_id, -- cod_contact_parent
	  created_at,
	  updated_at,
	  title,
	  first_name,
	  last_name,
	  description,
	  cod_industry,
	  website,
	  email,
	  phone,
	  mobile,
	  fax,
	  twitter,
	  facebook,
	  linkedin,
	  skype,
	  cod_base_user_owner,
	  flg_organization,
	  address,
	  custom_fields,
	  customer_status,
	  prospect_status,
	  tags,
      hash_contact,
	  cod_execution
    from
      crm_integration_anlt.tmp_ro_load_contact
    where
      dml_type in ('U','I');



analyze crm_integration_anlt.t_lkp_contact;
	  
--$$$

-- update do contact_id/cod_contact_parent - CARS RO
update crm_integration_anlt.t_lkp_contact
set cod_contact_parent = contact_parent.cod_contact
from
(
select * from crm_integration_anlt.t_lkp_contact
where cod_source_system = 18
and cod_contact_parent is null
) contact_parent
where t_lkp_contact.cod_contact_parent = contact_parent.opr_contact
and t_lkp_contact.cod_source_system = contact_parent.cod_source_system;
	  
	--$$$

-- update do contact_id/cod_contact_parent - RE RO
update crm_integration_anlt.t_lkp_contact
set cod_contact_parent = contact_parent.cod_contact
from
(
select * from crm_integration_anlt.t_lkp_contact
where cod_source_system = 19
and cod_contact_parent is null
) contact_parent
where t_lkp_contact.cod_contact_parent = contact_parent.opr_contact
and t_lkp_contact.cod_source_system = contact_parent.cod_source_system;
	  
	--$$$

-- update do contact_id/cod_contact_parent - OLX RO
update crm_integration_anlt.t_lkp_contact
set cod_contact_parent = contact_parent.cod_contact
from
(
select * from crm_integration_anlt.t_lkp_contact
where cod_source_system = 20
and cod_contact_parent is null
) contact_parent
where t_lkp_contact.cod_contact_parent = contact_parent.opr_contact
and t_lkp_contact.cod_source_system = contact_parent.cod_source_system;
	  
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
    (select coalesce(max(cod_execution),0) max_cod_exec from crm_integration_anlt.t_fac_scai_execution),
    crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
    crm_integration_anlt.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 4 -- Romania
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_lkp_contact';

--$$$
	
-- #######################
-- ####    PASSO 6    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set cod_status = 1, -- Ok
last_processing_datetime = coalesce((select max(updated_at) from crm_integration_anlt.tmp_ro_load_contact),last_processing_datetime)
/*from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc, crm_integration_anlt.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_contact'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 4
  ) source*/
from crm_integration_anlt.t_lkp_scai_process proc 
where t_rel_scai_integration_process.cod_process = proc.cod_process
and t_rel_scai_integration_process.cod_status = 2
and t_rel_scai_integration_process.cod_country = 4
and proc.dsc_process_short = 't_lkp_contact'
and t_rel_scai_integration_process.ind_active = 1
/*crm_integration_anlt.t_rel_scai_integration_process.cod_process = source.cod_process
and crm_integration_anlt.t_rel_scai_integration_process.cod_country = source.cod_country
and crm_integration_anlt.t_rel_scai_integration_process.cod_integration = source.cod_integration*/;

	--$$$
	
-- #######################
-- ####    PASSO 3    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set dat_processing = source.dat_processing, execution_nbr = source.execution_nbr, cod_status = 2 -- Running
from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc, crm_integration_anlt.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_custom_field'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 4
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
    (select coalesce(max(cod_execution),0) max_cod_exec from crm_integration_anlt.t_fac_scai_execution),
    crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
    crm_integration_anlt.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 4 -- Romania
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_lkp_custom_field';	

	--$$$
	
-- #############################################
-- # 	     BASE - Romania                    #
-- #       LOADING t_lkp_custom_field          #
-- #############################################



create temp table tmp_ro_contact_custom_field_1 as
select
            *
          from
            (
              select (1000 * t1.num) + (100 * t2.num) + (10 * t3.num) + t4.num AS gen_num
              from
                (select 1 as num union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9 union select 0) t1,
                (select 1 as num union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9 union select 0) t2,
                (select 1 as num union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9 union select 0) t3,
                (select 1 as num union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9 union select 0) t4
            )
          where
            gen_num between 1 and (select max(regexp_count(custom_fields, '\\","') + 1) from crm_integration_anlt.tmp_ro_load_contact);



			
create temp table tmp_ro_contact_custom_field_2 as
select
        ts.opr_contact,
        ts.custom_fields,
        s.gen_num,
        ts.cod_source_system,
        split_part(replace(replace(replace(replace(custom_fields,':false,',':"false",'),':true,',':"true",'),':false}',':"false"}'),':true}',':"true"}'),'","', s.gen_num) AS segment
      from
        crm_integration_anlt.tmp_ro_load_contact ts,
        crm_integration_anlt.tmp_ro_contact_custom_field_teste1 s
      where
        split_part(custom_fields, '","', s.gen_num) != ''
        and custom_fields != '{}';
		


		
create temp table tmp_ro_contact_custom_field
distkey(cod_source_system)
sortkey(custom_field_name, cod_source_system)
as
  select
    opr_contact,
    custom_fields,
    cod_source_system,
    case when segment = '{}' then null else replace(replace(split_part(segment,'":"',1),'{"',''),'"}','') end custom_field_name,
    case when segment = '{}' then null else replace(replace(split_part(segment,'":"',2),'{"',''),'"}','') end custom_field_value
  from
    tmp_ro_contact_custom_field_2
;
/*		
drop table if exists tmp_ro_contact_custom_field;

create temp table tmp_ro_contact_custom_field 
distkey(cod_source_system)
sortkey(custom_field_name, cod_source_system)
as
  select
    opr_contact,
    custom_fields,
    cod_source_system,
    case when segment = '{}' then null else replace(replace(split_part(segment,'":"',1),'{"',''),'"}','') end custom_field_name,
    case when segment = '{}' then null else replace(replace(split_part(segment,'":"',2),'{"',''),'"}','') end custom_field_value
  from
    (
      select
        ts.opr_contact,
        ts.custom_fields,
        s.gen_num,
        ts.cod_source_system,
        split_part(replace(replace(replace(replace(custom_fields,':false,',':"false",'),':true,',':"true",'),':false}',':"false"}'),':true}',':"true"}'),'","', s.gen_num) AS segment
      from
        tmp_ro_load_contact ts,
        (
          select
            *
          from
            (
              select (1000 * t1.num) + (100 * t2.num) + (10 * t3.num) + t4.num AS gen_num
              from
                (select 1 as num union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9 union select 0) t1,
                (select 1 as num union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9 union select 0) t2,
                (select 1 as num union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9 union select 0) t3,
                (select 1 as num union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9 union select 0) t4
            )
          where
            gen_num between 1 and (select max(regexp_count(custom_fields, '\\","') + 1) from tmp_ro_load_contact)
        ) s
      where
        split_part(custom_fields, '","', s.gen_num) != ''
        and custom_fields != '{}'
    )
;
*/

analyze tmp_ro_contact_custom_field;




	
create temp table tmp_ro_load_custom_field as
   select
    source_table.opr_custom_field,
    source_table.opr_custom_field dsc_custom_field,
    source_table.cod_source_system,
    source_table.cod_execution,
    max_cod_custom_field.max_cod,
    row_number() over (order by source_table.opr_custom_field desc) new_cod,
    target.cod_custom_field,
    cf_context.cod_custom_field_context,
    case
      when target.cod_custom_field is null then 'I'
        else 'X'
    end dml_type
  from
    (
      select
        distinct custom_field_name opr_custom_field,
        cod_source_system,
        scai_execution.cod_execution
      from
        tmp_ro_contact_custom_field,
        (
          select
            rel_integr_proc.dat_processing,
            max(fac.cod_execution) cod_execution
          from
            crm_integration_anlt.t_lkp_scai_process proc,
            crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
            crm_integration_anlt.t_fac_scai_execution fac
          where
            rel_integr_proc.cod_process = proc.cod_process
            and rel_integr_proc.cod_country = 4
            and rel_integr_proc.cod_country = fac.cod_country
            and rel_integr_proc.cod_integration = 30000
            and rel_integr_proc.ind_active = 1
            and proc.dsc_process_short = 't_lkp_custom_field'
            and fac.cod_process = rel_integr_proc.cod_process
            and fac.cod_integration = rel_integr_proc.cod_integration
            and rel_integr_proc.dat_processing = fac.dat_processing
            and fac.cod_status = 2
          group by 
            rel_integr_proc.dat_processing
        ) scai_execution
    ) source_table,
    (select coalesce(max(cod_custom_field),0) max_cod from crm_integration_anlt.t_lkp_custom_field) max_cod_custom_field,
    (
			select
				*
			from
				(
					SELECT
						a.*,
						row_number()
						OVER (
							PARTITION BY opr_custom_field, cod_source_system
							ORDER BY valid_to DESC ) rn
					FROM
						crm_integration_anlt.t_lkp_custom_field a
				)
			where rn = 1
	) target,
    crm_integration_anlt.t_lkp_custom_field_context cf_context
  where
    coalesce(source_table.opr_custom_field,'-1') = target.opr_custom_field(+)
    and cf_context.opr_custom_field_context = 'Contacts'
	and source_table.cod_source_system = target.cod_source_system (+);

analyze tmp_ro_load_custom_field;
	

	
insert into crm_integration_anlt.t_lkp_custom_field
    select
      max_cod + new_cod cod_custom_field,
      opr_custom_field,
      dsc_custom_field,
      cod_source_system,
      cod_custom_field_context,
      (select rel_integr_proc.dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_custom_field') valid_from, 
      20991231 valid_to,
	  cod_execution
    from
      tmp_ro_load_custom_field
    where
      dml_type = 'I';



analyze crm_integration_anlt.t_lkp_custom_field;
	  
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
    (select coalesce(max(cod_execution),0) max_cod_exec from crm_integration_anlt.t_fac_scai_execution),
    crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
    crm_integration_anlt.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 4 -- Romania
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_lkp_custom_field';

--$$$

-- #######################
-- ####    PASSO 6    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set cod_status = 1, -- Ok
last_processing_datetime = coalesce((select max(updated_at) from crm_integration_anlt.tmp_ro_load_contact),last_processing_datetime)
/*from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc, crm_integration_anlt.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_stage'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 4
  ) source*/
from crm_integration_anlt.t_lkp_scai_process proc 
where t_rel_scai_integration_process.cod_process = proc.cod_process
and t_rel_scai_integration_process.cod_status = 2
and t_rel_scai_integration_process.cod_country = 4
and proc.dsc_process_short = 't_lkp_custom_field'
and t_rel_scai_integration_process.ind_active = 1
/*crm_integration_anlt.t_rel_scai_integration_process.cod_process = source.cod_process
and crm_integration_anlt.t_rel_scai_integration_process.cod_country = source.cod_country
and crm_integration_anlt.t_rel_scai_integration_process.cod_integration = source.cod_integration*/;



--$$$
	
-- #######################
-- ####    PASSO 3    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set dat_processing = source.dat_processing, execution_nbr = source.execution_nbr, cod_status = 2 -- Running
from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc, crm_integration_anlt.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_rel_contact_custom_field'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 4
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
    (select coalesce(max(cod_execution),0) max_cod_exec from crm_integration_anlt.t_fac_scai_execution),
    crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
    crm_integration_anlt.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 4 -- Romania
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_rel_contact_custom_field';	

--$$$
	
-- #############################################
-- # 	  BASE - Romania                       #
-- #    LOADING t_rel_contact_custom_field     #
-- #############################################


create temp table tmp_ro_rel_contact_custom_field as
  select
    source.cod_contact,
    source.cod_custom_field,
    source.cod_source_system,
    source.custom_field_value,
    source.cod_execution,
    case
      when target.cod_contact is null then 'I'
      when source.custom_field_value != target.custom_field_value then 'U'
        else 'X'
    end dml_type
  from
    (
      select
        contact.cod_contact,
        cf.cod_custom_field,
        tmp_cf.cod_source_system,
        tmp_cf.custom_field_value,
        scai_execution.cod_execution
      from
        tmp_ro_contact_custom_field tmp_cf,
        crm_integration_anlt.t_lkp_contact contact,
        crm_integration_anlt.t_lkp_custom_field cf,
        (
          select
            rel_integr_proc.dat_processing,
            max(fac.cod_execution) cod_execution
          from
            crm_integration_anlt.t_lkp_scai_process proc,
            crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
            crm_integration_anlt.t_fac_scai_execution fac
          where
            rel_integr_proc.cod_process = proc.cod_process
            and rel_integr_proc.cod_country = 4
            and rel_integr_proc.cod_country = fac.cod_country
            and rel_integr_proc.cod_integration = 30000
            and rel_integr_proc.ind_active = 1
            and proc.dsc_process_short = 't_rel_contact_custom_field'
            and fac.cod_process = rel_integr_proc.cod_process
            and fac.cod_integration = rel_integr_proc.cod_integration
            and rel_integr_proc.dat_processing = fac.dat_processing
            and fac.cod_status = 2
          group by 
            rel_integr_proc.dat_processing
        ) scai_execution
      where
        tmp_cf.opr_contact = contact.opr_contact
        and tmp_cf.custom_field_name = cf.opr_custom_field
		and tmp_cf.cod_source_system = cf.cod_source_system
        and tmp_cf.cod_source_system = contact.cod_source_system
        and contact.valid_to = 20991231
        and cf.valid_to = 20991231
    ) source,
    crm_integration_anlt.t_rel_contact_custom_field target
  where
    source.cod_contact = target.cod_contact(+)
    and source.cod_custom_field = target.cod_custom_field(+)
    and source.cod_source_system = target.cod_source_system(+)
    and target.valid_to(+) = 20991231;

analyze tmp_ro_rel_contact_custom_field;
	

	
update crm_integration_anlt.t_rel_contact_custom_field
set valid_to = (select rel_integr_proc.dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_rel_contact_custom_field') 
from tmp_ro_rel_contact_custom_field source
where source.cod_contact = crm_integration_anlt.t_rel_contact_custom_field.cod_contact
and source.cod_custom_field = crm_integration_anlt.t_rel_contact_custom_field.cod_custom_field
and source.cod_source_system = crm_integration_anlt.t_rel_contact_custom_field.cod_source_system
and crm_integration_anlt.t_rel_contact_custom_field.valid_to = 20991231
and source.dml_type = 'U';


	
insert into crm_integration_anlt.t_rel_contact_custom_field
  select
    cod_contact,
    cod_custom_field,
    cod_source_system,
    custom_field_value,
    (select rel_integr_proc.dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_rel_contact_custom_field') valid_from, 
    20991231 valid_to,
	cod_execution
  from
    tmp_ro_rel_contact_custom_field
  where
    dml_type in ('I','U');



analyze crm_integration_anlt.t_rel_contact_custom_field;
	
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
    (select coalesce(max(cod_execution),0) max_cod_exec from crm_integration_anlt.t_fac_scai_execution),
    crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
    crm_integration_anlt.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 4 -- Romania
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_rel_contact_custom_field';

--$$$

-- #######################
-- ####    PASSO 6    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set cod_status = 1, -- Ok
last_processing_datetime = coalesce((select max(updated_at) from crm_integration_anlt.tmp_ro_load_contact),last_processing_datetime)
/*from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc, crm_integration_anlt.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_stage'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 4
  ) source*/
from crm_integration_anlt.t_lkp_scai_process proc 
where t_rel_scai_integration_process.cod_process = proc.cod_process
and t_rel_scai_integration_process.cod_status = 2
and t_rel_scai_integration_process.cod_country = 4
and proc.dsc_process_short = 't_rel_contact_custom_field'
and t_rel_scai_integration_process.ind_active = 1
/*crm_integration_anlt.t_rel_scai_integration_process.cod_process = source.cod_process
and crm_integration_anlt.t_rel_scai_integration_process.cod_country = source.cod_country
and crm_integration_anlt.t_rel_scai_integration_process.cod_integration = source.cod_integration*/;


--$$$
	
-- #######################
-- ####    PASSO 3    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set dat_processing = source.dat_processing, execution_nbr = source.execution_nbr, cod_status = 2 -- Running
from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc, crm_integration_anlt.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_lead'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 4
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
    (select coalesce(max(cod_execution),0) max_cod_exec from crm_integration_anlt.t_fac_scai_execution),
    crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
    crm_integration_anlt.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 4 -- Romania
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_lkp_lead';	

--$$$
	
-- #############################################
-- # 	          BASE - Romania               #
-- #           LOADING t_lkp_lead              #
-- #############################################


create temp table tmp_ro_load_lead 
distkey(cod_source_system)
sortkey(cod_lead, opr_lead)
as
   select
    source_table.opr_lead,
    source_table.dsc_lead,
    source_table.cod_source_system,
    coalesce(lkp_base_user_owner.cod_base_user,-2) cod_base_user_owner,
    coalesce(lkp_base_user_creator.cod_base_user,-2) cod_base_user_creator,
    --lkp_base_source.cod_base_source,
    coalesce(lkp_industry.cod_industry,-2) cod_industry,
    coalesce(lkp_lead_status.cod_lead_status,-2) cod_lead_status,
    source_table.created_at,
    source_table.updated_at,
    source_table.hash_lead,
    max_cod_lead.max_cod,
    row_number() over (order by source_table.opr_lead desc) new_cod,
    target.cod_lead,
	source_table.meta_event_type,
	source_table.meta_event_time,
    source_table.first_name,
    source_table.last_name,
    source_table.twitter,
    source_table.phone,
    source_table.mobile,
    source_table.facebook,
    source_table.email,
    source_table.title,
    source_table.skype,
    source_table.linkedin,
    source_table.fax,
    source_table.website,
    source_table.address,
    source_table.organization_name,
    source_table.custom_fields,
    source_table.tags,
    source_table.cod_execution,
	target.valid_from,
    case
      --when target.cod_lead is null then 'I'
	  when target.cod_lead is null or (source_table.hash_lead != target.hash_lead and target.valid_from = source_table.dat_processing) then 'I'
	  when source_table.meta_event_type = 'deleted' then 'D'
      when source_table.hash_lead != target.hash_lead then 'U'
        else 'X'
    end dml_type
  from
    (
	select
		source.*,
		lkp_source_system.cod_source_system,
        md5(coalesce(dsc_lead,'') + coalesce(first_name,'') + coalesce(last_name,'') + coalesce(opr_base_user_owner,0) /*+ coalesce(source_id,0)*/ + coalesce(twitter,'') + coalesce(phone,'')
          + coalesce(mobile,'') + coalesce(facebook,'') + coalesce(email,'') + coalesce(title,'') + coalesce(skype,'') + coalesce(linkedin,'') + coalesce(opr_industry,'')
          + coalesce(fax,'') + coalesce(website,'') + coalesce(address,'') + coalesce(opr_lead_status,'') + coalesce(opr_base_user_creator,0) + coalesce(organization_name,'')
          + coalesce(custom_fields,'') + coalesce(tags,'')) hash_lead
	from
	(
      SELECT
        id opr_lead,
        description dsc_lead,
        base_account_country + base_account_category opr_source_system,
		meta_event_type,
		meta_event_time,
        first_name,
        last_name,
        owner_id opr_base_user_owner,
        source_id opr_base_source,
        twitter,
        phone,
        mobile,
        facebook,
        email,
        title,
        skype,
        linkedin,
        industry opr_industry,
        fax,
        website,
        address,
        status opr_lead_status,
        creator_id opr_base_user_creator,
        organization_name,
        custom_fields,
        tags,
        created_at,
        updated_at,
        scai_execution.cod_execution,
        scai_execution.dat_processing
      FROM
        crm_integration_stg.stg_ro_d_base_leads,
        (
          select
            rel_integr_proc.dat_processing,
            max(fac.cod_execution) cod_execution
          from
            crm_integration_anlt.t_lkp_scai_process proc,
            crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
            crm_integration_anlt.t_fac_scai_execution fac
          where
            rel_integr_proc.cod_process = proc.cod_process
            and rel_integr_proc.cod_country = 4
            and rel_integr_proc.cod_country = fac.cod_country
            and rel_integr_proc.cod_integration = 30000
            and rel_integr_proc.ind_active = 1
            and proc.dsc_process_short = 't_lkp_lead'
            and fac.cod_process = rel_integr_proc.cod_process
            and fac.cod_integration = rel_integr_proc.cod_integration
            and rel_integr_proc.dat_processing = fac.dat_processing
            and fac.cod_status = 2
          group by 
            rel_integr_proc.dat_processing
        ) scai_execution
	) source,
    crm_integration_anlt.t_lkp_source_system lkp_source_system
	where source.opr_source_system = lkp_source_system.opr_source_system
	and lkp_source_system.cod_country = 4 -- Romania
	) source_table,
    crm_integration_anlt.t_lkp_base_user lkp_base_user_owner,
    crm_integration_anlt.t_lkp_base_user lkp_base_user_creator,
    --crm_integration_anlt.t_lkp_base_source lkp_base_source,
    crm_integration_anlt.t_lkp_industry lkp_industry,
    crm_integration_anlt.t_lkp_lead_status lkp_lead_status,
    (select coalesce(max(cod_lead),0) max_cod from crm_integration_anlt.t_lkp_lead) max_cod_lead,
    (
			select
				*
			from
				(
					SELECT
						a.*,
						row_number()
						OVER (
							PARTITION BY opr_lead, cod_source_system
							ORDER BY valid_to DESC ) rn
					FROM
						crm_integration_anlt.t_lkp_lead a
				)
			where rn = 1
	) target
  where
    coalesce(source_table.opr_lead,-1) = target.opr_lead(+)
	and source_table.cod_source_system = target.cod_source_system (+)
    and coalesce(source_table.opr_base_user_owner,-1) = lkp_base_user_owner.opr_base_user (+)
	and source_table.cod_source_system = lkp_base_user_owner.cod_source_system (+) -- new
	and lkp_base_user_owner.valid_to (+) = 20991231
    and coalesce(source_table.opr_base_user_creator,-1) = lkp_base_user_creator.opr_base_user (+)
	and source_table.cod_source_system = lkp_base_user_creator.cod_source_system (+) -- new
	and lkp_base_user_creator.valid_to (+) = 20991231
    and coalesce(source_table.opr_industry,'Unknown') = lkp_industry.opr_industry (+) -- LOST DATA
	and source_table.cod_source_system = lkp_industry.cod_source_system (+) -- new
	and lkp_industry.valid_to (+) = 20991231
    and coalesce(source_table.opr_lead_status,'Unknown') = lkp_lead_status.opr_lead_status (+)
	and source_table.cod_source_system = lkp_lead_status.cod_source_system (+) -- new
	and lkp_lead_status.valid_to (+) = 20991231;

analyze tmp_ro_load_lead;
	

	
delete from crm_integration_anlt.t_lkp_lead
using tmp_ro_load_lead
where 
	tmp_ro_load_lead.dml_type = 'I' 
	and t_lkp_lead.opr_lead = tmp_ro_load_lead.opr_lead 
	and t_lkp_lead.valid_from = (select dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_lead');


	
-- update valid_to in the updated/deleted records on source	
update crm_integration_anlt.t_lkp_lead
set valid_to = (select rel_integr_proc.dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_lead') 
from tmp_ro_load_lead source
where source.cod_lead = crm_integration_anlt.t_lkp_lead.cod_lead
and crm_integration_anlt.t_lkp_lead.valid_to = 20991231
and source.dml_type in('U','D');

	
insert into crm_integration_anlt.t_lkp_lead
    select
      case
        when dml_type = 'I' then case when valid_from = (select dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc
														where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_lead')
									then cod_lead else max_cod + new_cod end
        when dml_type = 'U' then cod_lead
      end cod_lead,
      opr_lead,
      dsc_lead,
      cod_source_system,
      (select rel_integr_proc.dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_lead') valid_from, 
      20991231 valid_to,
      first_name,
      last_name,
      cod_base_user_owner,
      -1 cod_base_source,
      created_at,
      updated_at,
      twitter,
      phone,
      mobile,
      facebook,
      email,
      title,
      skype,
      linkedin,
      cod_industry,
      fax,
      website,
      address,
      cod_lead_status,
      cod_base_user_creator,
      organization_name,
      custom_fields,
      tags,
      hash_lead,
	  cod_execution
    from
      tmp_ro_load_lead
    where
      dml_type in ('U','I');



analyze crm_integration_anlt.t_lkp_lead;
	  
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
    (select coalesce(max(cod_execution),0) max_cod_exec from crm_integration_anlt.t_fac_scai_execution),
    crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
    crm_integration_anlt.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 4 -- Romania
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_lkp_lead';

--$$$

-- #######################
-- ####    PASSO 6    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set cod_status = 1, -- Ok
last_processing_datetime = coalesce((select max(updated_at) from tmp_ro_load_lead),last_processing_datetime)
/*from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc, crm_integration_anlt.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_lead'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 4
  ) source*/
from crm_integration_anlt.t_lkp_scai_process proc 
where t_rel_scai_integration_process.cod_process = proc.cod_process
and t_rel_scai_integration_process.cod_status = 2
and t_rel_scai_integration_process.cod_country = 4
and proc.dsc_process_short = 't_lkp_lead'
and t_rel_scai_integration_process.ind_active = 1
/*crm_integration_anlt.t_rel_scai_integration_process.cod_process = source.cod_process
and crm_integration_anlt.t_rel_scai_integration_process.cod_country = source.cod_country
and crm_integration_anlt.t_rel_scai_integration_process.cod_integration = source.cod_integration*/;


--$$$
	
-- #######################
-- ####    PASSO 3    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set dat_processing = source.dat_processing, execution_nbr = source.execution_nbr, cod_status = 2 -- Running
from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc, crm_integration_anlt.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_loss_reason'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 4
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
    (select coalesce(max(cod_execution),0) max_cod_exec from crm_integration_anlt.t_fac_scai_execution),
    crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
    crm_integration_anlt.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 4 -- Romania
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_lkp_loss_reason';	

--$$$
	
-- #############################################
-- # 	          BASE - Romania               #
-- #        LOADING t_lkp_loss_reason          #
-- #############################################



create temp table tmp_ro_load_loss_reason 
distkey(cod_source_system)
sortkey(cod_loss_reason, opr_loss_reason)
as
  select
    source_table.opr_loss_reason,
    source_table.dsc_loss_reason,
    source_table.opr_source_system,
	source_table.meta_event_type,
	source_table.meta_event_time,
    coalesce(lkp_user_creator.cod_base_user,-2) cod_base_user_creator, -- CORRIGIR
    source_table.hash_loss_reason,
    source_table.cod_source_system,
    max_cod_loss_reason.max_cod,
    row_number() over (order by source_table.opr_loss_reason desc) new_cod,
    target.cod_loss_reason,
    source_table.created_at,
    source_table.updated_at,
    source_table.cod_execution,
	target.valid_from,
    case
      --when target.cod_loss_reason is null then 'I'
	  when target.cod_loss_reason is null or (source_table.hash_loss_reason != target.hash_loss_reason and target.valid_from = source_table.dat_processing) then 'I'
	  when source_table.meta_event_type = 'deleted' then 'D'
      when source_table.hash_loss_reason != target.hash_loss_reason then 'U'
        else 'X'
    end dml_type
  from
    (
	select
		source.*,
		lkp_source_system.cod_source_system,
        md5(coalesce(dsc_loss_reason,'') + coalesce(opr_base_user_creator,0)) hash_loss_reason
    from
	(
      SELECT
        id opr_loss_reason,
        name dsc_loss_reason,
        base_account_country + base_account_category opr_source_system,
		meta_event_type,
		meta_event_time,
        creator_id opr_base_user_creator,
        created_at,
        updated_at,
        scai_execution.cod_execution,
        scai_execution.dat_processing
	  FROM
        crm_integration_stg.stg_ro_d_base_loss_reasons,
        (
          select
            rel_integr_proc.dat_processing,
            max(fac.cod_execution) cod_execution
          from
            crm_integration_anlt.t_lkp_scai_process proc,
            crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
            crm_integration_anlt.t_fac_scai_execution fac
          where
            rel_integr_proc.cod_process = proc.cod_process
            and rel_integr_proc.cod_country = 4
            and rel_integr_proc.cod_country = fac.cod_country
            and rel_integr_proc.cod_integration = 30000
            and rel_integr_proc.ind_active = 1
            and proc.dsc_process_short = 't_lkp_loss_reason'
            and fac.cod_process = rel_integr_proc.cod_process
            and fac.cod_integration = rel_integr_proc.cod_integration
            and rel_integr_proc.dat_processing = fac.dat_processing
            and fac.cod_status = 2
          group by 
            rel_integr_proc.dat_processing
        ) scai_execution
	) source,
    crm_integration_anlt.t_lkp_source_system lkp_source_system
	where source.opr_source_system = lkp_source_system.opr_source_system
	and lkp_source_system.cod_country = 4 -- Romania
	) source_table,
    crm_integration_anlt.t_lkp_base_user lkp_user_creator,
    (select coalesce(max(cod_loss_reason),0) max_cod from crm_integration_anlt.t_lkp_loss_reason) max_cod_loss_reason,
    (
			select
				*
			from
				(
					SELECT
						a.*,
						row_number()
						OVER (
							PARTITION BY opr_loss_reason, cod_source_system
							ORDER BY valid_to DESC ) rn
					FROM
						crm_integration_anlt.t_lkp_loss_reason a
				)
			where rn = 1
	) target
  where
    coalesce(source_table.opr_loss_reason,-1) = target.opr_loss_reason(+)
	and source_table.cod_source_system = target.cod_source_system (+)
    and coalesce(source_table.opr_base_user_creator,-1) = lkp_user_creator.opr_base_user (+)
	and source_table.cod_source_system = lkp_user_creator.cod_source_system (+) -- new
	and lkp_user_creator.valid_to (+) = 20991231;

analyze tmp_ro_load_loss_reason;
	

	
delete from crm_integration_anlt.t_lkp_loss_reason
using tmp_ro_load_loss_reason
where 
	tmp_ro_load_loss_reason.dml_type = 'I' 
	and t_lkp_loss_reason.opr_loss_reason = tmp_ro_load_loss_reason.opr_loss_reason 
	and t_lkp_loss_reason.valid_from = (select dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_loss_reason');


	
update crm_integration_anlt.t_lkp_loss_reason
set valid_to = (select rel_integr_proc.dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_loss_reason') 
from tmp_ro_load_loss_reason source
where source.cod_loss_reason = crm_integration_anlt.t_lkp_loss_reason.cod_loss_reason
and crm_integration_anlt.t_lkp_loss_reason.valid_to = 20991231
and source.dml_type in('U','D');



insert into crm_integration_anlt.t_lkp_loss_reason
	select
	  case
		when dml_type = 'I' then case when valid_from = (select dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc
														where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_loss_reason')
									then cod_loss_reason else max_cod + new_cod end
		when dml_type = 'U' then cod_loss_reason
	  end cod_loss_reason,
	  opr_loss_reason,
	  dsc_loss_reason,
	  cod_source_system,
	  (select rel_integr_proc.dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_loss_reason') valid_from, 
	  20991231 valid_to,
	  cod_base_user_creator,
	  created_at,
	  updated_at,
	  hash_loss_reason,
	  cod_execution
	from
	  tmp_ro_load_loss_reason
	where
	  dml_type in ('U','I');



analyze crm_integration_anlt.t_lkp_loss_reason;
	  
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
    (select coalesce(max(cod_execution),0) max_cod_exec from crm_integration_anlt.t_fac_scai_execution),
    crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
    crm_integration_anlt.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 4 -- Romania
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_lkp_loss_reason';

--$$$

-- #######################
-- ####    PASSO 6    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set cod_status = 1, -- Ok
last_processing_datetime = coalesce((select max(updated_at) from tmp_ro_load_loss_reason),last_processing_datetime)
/*from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc, crm_integration_anlt.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_loss_reason'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 4
  ) source*/
from crm_integration_anlt.t_lkp_scai_process proc 
where t_rel_scai_integration_process.cod_process = proc.cod_process
and t_rel_scai_integration_process.cod_status = 2
and t_rel_scai_integration_process.cod_country = 4
and proc.dsc_process_short = 't_lkp_loss_reason'
and t_rel_scai_integration_process.ind_active = 1
/*crm_integration_anlt.t_rel_scai_integration_process.cod_process = source.cod_process
and crm_integration_anlt.t_rel_scai_integration_process.cod_country = source.cod_country
and crm_integration_anlt.t_rel_scai_integration_process.cod_integration = source.cod_integration*/;


--$$$
	
-- #######################
-- ####    PASSO 3    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set dat_processing = source.dat_processing, execution_nbr = source.execution_nbr, cod_status = 2 -- Running
from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc, crm_integration_anlt.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_pipeline'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 4
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
    (select coalesce(max(cod_execution),0) max_cod_exec from crm_integration_anlt.t_fac_scai_execution),
    crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
    crm_integration_anlt.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 4 -- Romania
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_lkp_pipeline';

--$$$
	
-- #############################################
-- # 	          BASE - Romania               #
-- #		   LOADING t_lkp_pipeline          #
-- #############################################


create temp table tmp_ro_load_pipeline 
distkey(cod_source_system)
sortkey(cod_pipeline, opr_pipeline)
as
  select
    source_table.opr_pipeline,
    source_table.dsc_pipeline,
    source_table.cod_source_system,
	source_table.meta_event_type,
	source_table.meta_event_time,
    source_table.flg_disabled,
    source_table.created_at,
    source_table.updated_at,
    source_table.hash_pipeline,
    source_table.cod_execution,
    max_cod_pipeline.max_cod,
    row_number() over (order by source_table.opr_pipeline desc) new_cod,
    target.cod_pipeline,
	target.valid_from,
    case
      --when target.cod_pipeline is null then 'I'
	  when target.cod_pipeline is null or (source_table.hash_pipeline != target.hash_pipeline and target.valid_from = source_table.dat_processing) then 'I'
	  when source_table.meta_event_type = 'deleted' then 'D'
      when source_table.hash_pipeline != target.hash_pipeline then 'U'
        else 'X'
    end dml_type
  from
    (
	select
		source.*,
		lkp_source_system.cod_source_system,
        md5(coalesce(dsc_pipeline,'') + decode(flg_disabled, 1, 1, 0)) hash_pipeline
    from
	(
      SELECT
        id opr_pipeline,
        name dsc_pipeline,
        base_account_country + base_account_category opr_source_system,
		meta_event_type,
		meta_event_time,
        disabled flg_disabled,
        created_at,
        updated_at,
        scai_execution.cod_execution,
        scai_execution.dat_processing
	  FROM
        crm_integration_stg.stg_ro_d_base_pipelines,
        (
          select
            rel_integr_proc.dat_processing,
            max(fac.cod_execution) cod_execution
          from
            crm_integration_anlt.t_lkp_scai_process proc,
            crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
            crm_integration_anlt.t_fac_scai_execution fac
          where
            rel_integr_proc.cod_process = proc.cod_process
            and rel_integr_proc.cod_country = 4
            and rel_integr_proc.cod_country = fac.cod_country
            and rel_integr_proc.cod_integration = 30000
            and rel_integr_proc.ind_active = 1
            and proc.dsc_process_short = 't_lkp_pipeline'
            and fac.cod_process = rel_integr_proc.cod_process
            and fac.cod_integration = rel_integr_proc.cod_integration
            and rel_integr_proc.dat_processing = fac.dat_processing
            and fac.cod_status = 2
          group by 
            rel_integr_proc.dat_processing
        ) scai_execution
	) source,
    crm_integration_anlt.t_lkp_source_system lkp_source_system
	where source.opr_source_system = lkp_source_system.opr_source_system
	and lkp_source_system.cod_country = 4 -- Romania
	) source_table,
    (select coalesce(max(cod_pipeline),0) max_cod from crm_integration_anlt.t_lkp_pipeline) max_cod_pipeline,
    (
			select
				*
			from
				(
					SELECT
						a.*,
						row_number()
						OVER (
							PARTITION BY opr_pipeline, cod_source_system
							ORDER BY valid_to DESC ) rn
					FROM
						crm_integration_anlt.t_lkp_pipeline a
				)
			where rn = 1
	) target
  where
    coalesce(source_table.opr_pipeline,-1) = target.opr_pipeline(+)
	and source_table.cod_source_system = target.cod_source_system (+);

analyze tmp_ro_load_pipeline;
	

	
delete from crm_integration_anlt.t_lkp_pipeline
using tmp_ro_load_pipeline
where 
	tmp_ro_load_pipeline.dml_type = 'I' 
	and t_lkp_pipeline.opr_pipeline = tmp_ro_load_pipeline.opr_pipeline
	and t_lkp_pipeline.valid_from = (select dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_pipeline');


	
update crm_integration_anlt.t_lkp_pipeline
set valid_to = (select rel_integr_proc.dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_pipeline') 
from tmp_ro_load_pipeline source
where source.cod_pipeline = crm_integration_anlt.t_lkp_pipeline.cod_pipeline
and crm_integration_anlt.t_lkp_pipeline.valid_to = 20991231
and source.dml_type in('U','D');



insert into crm_integration_anlt.t_lkp_pipeline
    select
      case
        when dml_type = 'I' then case when valid_from = (select dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc
														where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_pipeline')
									then cod_pipeline else max_cod + new_cod end
        when dml_type = 'U' then cod_pipeline
      end cod_pipeline,
      opr_pipeline,
      dsc_pipeline,
      cod_source_system,
      (select rel_integr_proc.dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_pipeline') valid_from, 
      20991231 valid_to,
      decode(flg_disabled,1,1,0) flg_confirmed,
      created_at,
      updated_at,
      hash_pipeline,
	  cod_execution
    from
      tmp_ro_load_pipeline
    where
      dml_type in ('U','I');



analyze crm_integration_anlt.t_lkp_pipeline;
	  
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
    (select coalesce(max(cod_execution),0) max_cod_exec from crm_integration_anlt.t_fac_scai_execution),
    crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
    crm_integration_anlt.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 4 -- Romania
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_lkp_pipeline';

--$$$

-- #######################
-- ####    PASSO 6    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set cod_status = 1, -- Ok
last_processing_datetime = coalesce((select max(updated_at) from tmp_ro_load_pipeline),last_processing_datetime)
/*from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc, crm_integration_anlt.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_pipeline'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 4
  ) source*/
from crm_integration_anlt.t_lkp_scai_process proc 
where t_rel_scai_integration_process.cod_process = proc.cod_process
and t_rel_scai_integration_process.cod_status = 2
and t_rel_scai_integration_process.cod_country = 4
and proc.dsc_process_short = 't_lkp_pipeline'
and t_rel_scai_integration_process.ind_active = 1
/*crm_integration_anlt.t_rel_scai_integration_process.cod_process = source.cod_process
and crm_integration_anlt.t_rel_scai_integration_process.cod_country = source.cod_country
and crm_integration_anlt.t_rel_scai_integration_process.cod_integration = source.cod_integration*/;



--$$$
	
-- #######################
-- ####    PASSO 3    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set dat_processing = source.dat_processing, execution_nbr = source.execution_nbr, cod_status = 2 -- Running
from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc, crm_integration_anlt.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_stage'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 4
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
    (select coalesce(max(cod_execution),0) max_cod_exec from crm_integration_anlt.t_fac_scai_execution),
    crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
    crm_integration_anlt.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 4 -- Romania
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_lkp_stage';	

--$$$
	
-- #############################################
-- # 	          BASE - Romania               #
-- #		   LOADING t_lkp_stage             #
-- #############################################


create temp table tmp_ro_load_stage 
distkey(cod_source_system)
sortkey(cod_stage, opr_stage)
as
  select
    source_table.opr_stage,
    source_table.dsc_stage,
    coalesce(lkp_pipeline.cod_pipeline,-2) cod_pipeline,
    source_table.cod_source_system,
	source_table.meta_event_type,
	source_table.meta_event_time,
	source_table.updated_at,
    source_table.hash_stages,
    max_cod_stages.max_cod,
    row_number() over (order by source_table.opr_stage desc) new_cod,
    target.cod_stage,
    source_table.position,
    source_table.likelihood,
    source_table.flg_active,
    source_table.cod_execution,
	target.valid_from,
    case
      --when target.cod_stage is null then 'I'
	  when target.cod_stage is null or (source_table.hash_stages != target.hash_stages and target.valid_from = source_table.dat_processing) then 'I'
	  when source_table.meta_event_type = 'deleted' then 'D'
      when source_table.hash_stages != target.hash_stages then 'U'
        else 'X'
    end dml_type
  from
    (
	select
		source.*,
		lkp_source_system.cod_source_system,
        md5(coalesce(dsc_stage,'') + coalesce(position,0) + coalesce(likelihood,0) + decode(flg_active, 1, 1, 0)) hash_stages
	from
	(
      SELECT
        id opr_stage,
        name dsc_stage,
        base_account_country + base_account_category opr_source_system,
		meta_event_type,
		meta_event_time,
		updated_at,
        position,
        likelihood,
        active flg_active,
        pipeline_id opr_pipeline,
        scai_execution.cod_execution,
        scai_execution.dat_processing
      FROM
        crm_integration_stg.stg_ro_d_base_stages,
        (
          select
            rel_integr_proc.dat_processing,
            max(fac.cod_execution) cod_execution
          from
            crm_integration_anlt.t_lkp_scai_process proc,
            crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
            crm_integration_anlt.t_fac_scai_execution fac
          where
            rel_integr_proc.cod_process = proc.cod_process
            and rel_integr_proc.cod_country = 4
            and rel_integr_proc.cod_country = fac.cod_country
            and rel_integr_proc.cod_integration = 30000
            and rel_integr_proc.ind_active = 1
            and proc.dsc_process_short = 't_lkp_stage'
            and fac.cod_process = rel_integr_proc.cod_process
            and fac.cod_integration = rel_integr_proc.cod_integration
            and rel_integr_proc.dat_processing = fac.dat_processing
            and fac.cod_status = 2
          group by 
            rel_integr_proc.dat_processing
        ) scai_execution
	) source,
    crm_integration_anlt.t_lkp_source_system lkp_source_system
	where source.opr_source_system = lkp_source_system.opr_source_system
	and lkp_source_system.cod_country = 4 -- Romania
	) source_table,
    crm_integration_anlt.t_lkp_pipeline lkp_pipeline,
    (select coalesce(max(cod_pipeline),0) max_cod from crm_integration_anlt.t_lkp_stage) max_cod_stages,
    (
			select
				*
			from
				(
					SELECT
						a.*,
						row_number()
						OVER (
							PARTITION BY opr_stage, cod_source_system
							ORDER BY valid_to DESC ) rn
					FROM
						crm_integration_anlt.t_lkp_stage a
				)
			where rn = 1
	) target
  where
    coalesce(source_table.opr_stage,-1) = target.opr_stage(+)
	and source_table.cod_source_system = target.cod_source_system (+)
    and coalesce(source_table.opr_pipeline,-1) = lkp_pipeline.opr_pipeline (+)
	and source_table.cod_source_system = lkp_pipeline.cod_source_system (+)-- new
	and lkp_pipeline.valid_to (+) = 20991231;

analyze tmp_ro_load_stage;
	

	
delete from crm_integration_anlt.t_lkp_stage
using tmp_ro_load_stage
where 
	tmp_ro_load_stage.dml_type = 'I' 
	and t_lkp_stage.opr_stage = tmp_ro_load_stage.opr_stage
	and t_lkp_stage.valid_from = (select dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_stage');


	
update crm_integration_anlt.t_lkp_stage
set valid_to = (select rel_integr_proc.dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_stage') 
from tmp_ro_load_stage source
where source.cod_stage = crm_integration_anlt.t_lkp_stage.cod_stage
and crm_integration_anlt.t_lkp_stage.valid_to = 20991231
and source.dml_type in('U','D');


	
insert into crm_integration_anlt.t_lkp_stage
    select
      case
        when dml_type = 'I' then case when valid_from = (select dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc
														where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_stage')
									then cod_stage else max_cod + new_cod end
        when dml_type = 'U' then cod_stage
      end cod_stage,
      opr_stage,
      dsc_stage,
      cod_source_system,
      (select rel_integr_proc.dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_stage') valid_from, 
      20991231 valid_to,
      position,
      likelihood,
      decode(flg_active,1,1,0) flg_active,
      cod_pipeline,
      hash_stages,
	  cod_execution
    from
      tmp_ro_load_stage
    where
      dml_type in ('U','I');


analyze crm_integration_anlt.t_lkp_stage;
	  
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
    (select coalesce(max(cod_execution),0) max_cod_exec from crm_integration_anlt.t_fac_scai_execution),
    crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
    crm_integration_anlt.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 4 -- Romania
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_lkp_stage';

--$$$

-- #######################
-- ####    PASSO 6    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set cod_status = 1, -- Ok
last_processing_datetime = coalesce((select max(updated_at) from tmp_ro_load_stage),last_processing_datetime)
/*from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc, crm_integration_anlt.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_stage'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 4
  ) source*/
from crm_integration_anlt.t_lkp_scai_process proc 
where t_rel_scai_integration_process.cod_process = proc.cod_process
and t_rel_scai_integration_process.cod_status = 2
and t_rel_scai_integration_process.cod_country = 4
and proc.dsc_process_short = 't_lkp_stage'
and t_rel_scai_integration_process.ind_active = 1
/*crm_integration_anlt.t_rel_scai_integration_process.cod_process = source.cod_process
and crm_integration_anlt.t_rel_scai_integration_process.cod_country = source.cod_country
and crm_integration_anlt.t_rel_scai_integration_process.cod_integration = source.cod_integration*/;



--$$$
	
-- #######################
-- ####    PASSO 3    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set dat_processing = source.dat_processing, execution_nbr = source.execution_nbr, cod_status = 2 -- Running
from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc, crm_integration_anlt.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_deal'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 4
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
    (select coalesce(max(cod_execution),0) max_cod_exec from crm_integration_anlt.t_fac_scai_execution),
    crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
    crm_integration_anlt.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 4 -- Romania
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_lkp_deal';	

--$$$
	
-- #############################################
-- # 	          BASE - Romania               #
-- #           LOADING t_lkp_deal              #
-- #############################################

create temp table tmp_ro_load_deals 
distkey(cod_source_system)
sortkey(cod_deal, opr_deal)
as
  select
    source_table.opr_deal,
    source_table.dsc_deal,
    source_table.cod_source_system,
    source_table.meta_event_type,
    source_table.meta_event_time,
    source_table.last_activity_at,
    coalesce(lkp_contact.cod_contact,-2) cod_contact,
    coalesce(lkp_base_source.cod_base_source,-2) cod_base_source,
    source_table.estimated_close_date,
    source_table.dropbox_email,
    coalesce(lkp_base_user_creator.cod_base_user,-2) cod_base_user_creator,
    coalesce(lkp_loss_reason.cod_loss_reason,-2) cod_loss_reason,
    coalesce(lkp_currency.cod_currency,-2) cod_currency,
    source_table.updated_at,
    source_table.organization_id,
    source_table.last_stage_change_at,
    coalesce(lkp_base_user_owner.cod_base_user,-2) cod_base_user_owner,
    source_table.value,
    source_table.created_at,
    source_table.flg_hot,
    source_table.opr_base_user_last_change, -- ?
    coalesce(lkp_stages.cod_stage,-2) cod_stage,
    source_table.custom_fields,
    source_table.tags,
    source_table.hash_deal,
    source_table.cod_execution,
    max_cod_deals.max_cod,
    row_number() over (order by source_table.opr_deal desc) new_cod,
    target.cod_deal,
	target.valid_from,
    case
      --when target.cod_deal is null then 'I'
	  when target.cod_deal is null or (source_table.hash_deal != target.hash_deal and target.valid_from = source_table.dat_processing) then 'I'
	  when source_table.meta_event_type = 'deleted' then 'D'
      when source_table.hash_deal != target.hash_deal then 'U'
        else 'X'
    end dml_type
  from
    (
    	select
    		source.*,
    		lkp_source_system.cod_source_system,
        md5(
      		coalesce(dsc_deal                                                              ,'') +
      		coalesce(meta_event_type                                                   ,'') +
      		--coalesce(meta_event_time                                                   ,'2099-12-31 00:00:00.000000') +
      		--coalesce(last_activity_at                                                  ,'2099-12-31 00:00:00.000000') +
      		coalesce(opr_contact                                                        ,0) +
      		coalesce(opr_base_source                                                         ,0) +
      		coalesce(estimated_close_date                                              ,'2099-12-31') +
      		coalesce(dropbox_email                                                     ,'') +
      		coalesce(opr_base_user_creator                                                        ,0) +
      		coalesce(opr_loss_reason                                                    ,0) +
      		coalesce(opr_currency                                                          ,'') +
      		--coalesce(updated_at                                                      ,'') +
      		coalesce(organization_id                                                   ,0) +
      		--coalesce(last_stage_change_at                                              ,'2099-12-31 00:00:00.000000') +
      		coalesce(opr_base_user_owner                                                          ,0) +
      		coalesce(value                                                             ,0) +
      		--coalesce(created_at                                                      ,'') +
      		decode(flg_hot,1,1,0)                                                        +
      		coalesce(opr_base_user_last_change                                           ,0) +
      		coalesce(opr_stage                                                          ,0) +
      		coalesce(custom_fields                                                     ,'') +
      		coalesce(tags                                                              ,'')
    		) hash_deal
      from
        (
          select
            id opr_deal,
            name dsc_deal,
            base_account_country + base_account_category opr_source_system,
            meta_event_type,
            meta_event_time,
            last_activity_at,
            contact_id opr_contact,
            source_id opr_base_source,
            estimated_close_date,
            dropbox_email,
            creator_id opr_base_user_creator,
            loss_reason_id opr_loss_reason,
            currency opr_currency,
            updated_at,
            organization_id,
            last_stage_change_at,
            owner_id opr_base_user_owner,
            value,
            created_at,
            hot flg_hot,
            last_stage_change_by_id opr_base_user_last_change, -- ?
            stage_id opr_stage,
            custom_fields,
            tags,
            scai_execution.cod_execution,
            scai_execution.dat_processing
          from
            crm_integration_stg.stg_ro_d_base_deals,
            (
              select
                rel_integr_proc.dat_processing,
                max(fac.cod_execution) cod_execution
              from
                crm_integration_anlt.t_lkp_scai_process proc,
                crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
                crm_integration_anlt.t_fac_scai_execution fac
              where
                rel_integr_proc.cod_process = proc.cod_process
                and rel_integr_proc.cod_country = 4
                and rel_integr_proc.cod_integration = 30000
                and rel_integr_proc.ind_active = 1
                and proc.dsc_process_short = 't_lkp_deal'
                and fac.cod_process = rel_integr_proc.cod_process
                and fac.cod_integration = rel_integr_proc.cod_integration
                and rel_integr_proc.dat_processing = fac.dat_processing
                and fac.cod_status = 2
              group by 
                rel_integr_proc.dat_processing
            ) scai_execution
        ) source,
        crm_integration_anlt.t_lkp_source_system lkp_source_system
      where
        source.opr_source_system = lkp_source_system.opr_source_system
        and lkp_source_system.cod_country = 4 -- Romania
    ) source_table,
    (select coalesce(max(cod_deal),0) max_cod from crm_integration_anlt.t_lkp_deal) max_cod_deals,
    crm_integration_anlt.t_lkp_base_user lkp_base_user_creator,
    crm_integration_anlt.t_lkp_base_user lkp_base_user_owner,
    crm_integration_anlt.t_lkp_contact lkp_contact,
    crm_integration_anlt.t_lkp_base_source lkp_base_source,
    crm_integration_anlt.t_lkp_currency lkp_currency,
    crm_integration_anlt.t_lkp_stage lkp_stages,
    crm_integration_anlt.t_lkp_loss_reason lkp_loss_reason,
    (
			select
				*
			from
				(
					SELECT
						a.*,
						row_number()
						OVER (
							PARTITION BY opr_deal, cod_source_system
							ORDER BY valid_to DESC ) rn
					FROM
						crm_integration_anlt.t_lkp_deal a
				)
			where rn = 1
	) target
  where
    source_table.opr_deal = target.opr_deal(+)
    and source_table.cod_source_system = target.cod_source_system (+)
    and coalesce(source_table.opr_base_user_owner,-1) = lkp_base_user_owner.opr_base_user (+)
    and source_table.cod_source_system = lkp_base_user_owner.cod_source_system (+) -- new
    and lkp_base_user_owner.valid_to (+) = 20991231
    and coalesce(source_table.opr_base_user_creator,-1) = lkp_base_user_creator.opr_base_user (+)
    and source_table.cod_source_system = lkp_base_user_creator.cod_source_system (+) -- new
    and lkp_base_user_creator.valid_to (+) = 20991231
    and coalesce(source_table.opr_currency,'Unknown') = lkp_currency.opr_currency (+)
    and lkp_currency.valid_to (+) = 20991231
    and coalesce(source_table.opr_loss_reason,-1) = lkp_loss_reason.opr_loss_reason (+)
    and lkp_currency.valid_to (+) = 20991231
    and coalesce(source_table.opr_stage,-1) = lkp_stages.opr_stage (+)
    and source_table.cod_source_system = lkp_stages.cod_source_system (+) -- new
    and lkp_currency.valid_to (+) = 20991231
    and coalesce(source_table.opr_base_source,-1) = lkp_base_source.opr_base_source (+)
    and lkp_base_source.valid_to (+) = 20991231
    and coalesce(source_table.opr_contact,-1) = lkp_contact.opr_contact (+)
    and source_table.cod_source_system = lkp_contact.cod_source_system (+) -- new
    and lkp_contact.valid_to (+) = 20991231;

analyze tmp_ro_load_deals;
	

	
delete from crm_integration_anlt.t_lkp_deal
using tmp_ro_load_deals
where 
	tmp_ro_load_deals.dml_type = 'I' 
	and t_lkp_deal.opr_deal = tmp_ro_load_deals.opr_deal
	and t_lkp_deal.valid_from = (select dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_deal');


	
update crm_integration_anlt.t_lkp_deal
set valid_to = (select rel_integr_proc.dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_deal') 
from tmp_ro_load_deals source
where source.cod_deal = crm_integration_anlt.t_lkp_deal.cod_deal
and crm_integration_anlt.t_lkp_deal.valid_to = 20991231
and source.dml_type in('U','D');


	
insert into crm_integration_anlt.t_lkp_deal
    select
      case
        when dml_type = 'I' then case when valid_from = (select dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc
														where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_deal')
									then cod_deal else max_cod + new_cod end
        when dml_type = 'U' then cod_deal
      end cod_deal,
      opr_deal,
      dsc_deal,
      cod_source_system,
      (select rel_integr_proc.dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_deal') valid_from, 
      20991231 valid_to,
	  last_activity_at,
	  cod_contact,
	  cod_base_source,
	  estimated_close_date,
	  dropbox_email,
	  cod_base_user_creator,
	  cod_loss_reason,
	  cod_currency,
	  created_at,
	  updated_at,
	  last_stage_change_at,
	  cod_base_user_owner,
	  value,
	  flg_hot,
	  opr_base_user_last_change cod_base_user_last_change, --?
	  cod_stage,
	  custom_fields,
	  tags,
      hash_deal,
	  cod_execution
    from
      tmp_ro_load_deals
    where
      dml_type in ('U','I');


	  
analyze crm_integration_anlt.t_lkp_deal;
	  
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
    (select coalesce(max(cod_execution),0) max_cod_exec from crm_integration_anlt.t_fac_scai_execution),
    crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
    crm_integration_anlt.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 4 -- Romania
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_lkp_deal';

--$$$

-- #######################
-- ####    PASSO 6    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set cod_status = 1, -- Ok
last_processing_datetime = coalesce((select max(updated_at) from tmp_ro_load_deals),last_processing_datetime)
/*from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc, crm_integration_anlt.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_deal'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 4
  ) source*/
from crm_integration_anlt.t_lkp_scai_process proc 
where t_rel_scai_integration_process.cod_process = proc.cod_process
and t_rel_scai_integration_process.cod_status = 2
and t_rel_scai_integration_process.cod_country = 4
and proc.dsc_process_short = 't_lkp_deal'
and t_rel_scai_integration_process.ind_active = 1
/*crm_integration_anlt.t_rel_scai_integration_process.cod_process = source.cod_process
and crm_integration_anlt.t_rel_scai_integration_process.cod_country = source.cod_country
and crm_integration_anlt.t_rel_scai_integration_process.cod_integration = source.cod_integration*/;


--$$$
	
-- #######################
-- ####    PASSO 3    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set dat_processing = source.dat_processing, execution_nbr = source.execution_nbr, cod_status = 2 -- Running
from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc, crm_integration_anlt.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_fac_call'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 4
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
    (select coalesce(max(cod_execution),0) max_cod_exec from crm_integration_anlt.t_fac_scai_execution),
    crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
    crm_integration_anlt.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 4 -- Romania
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_fac_call';	

--$$$
	
-- #############################################
-- # 	          BASE - Romania               #
-- #           LOADING t_fac_call              #
-- #############################################


create temp table tmp_ro_load_calls 
distkey(cod_source_system)
sortkey(opr_call)
as
select source.*, coalesce(lkp_call_outcome.cod_call_outcome,-1) cod_call_outcome
from
  (
    select
      --source_table.opr_source_system,
      source_table.opr_call,
      --source_table.opr_base_user,
      source_table.phone_number,
      source_table.flg_missed,
      source_table.opr_associated_deal,
      source_table.created_at,
      source_table.updated_at,
      source_table.summary,
      --source_table.opr_call_outcome,
      source_table.call_duration,
      source_table.flg_incoming,
      source_table.recording_url,
      --source_table.opr_resource_type,
      source_table.hash_call,
      source_table.cod_source_system,
      case
        when coalesce(lkp_resource_type.cod_resource_type,-2) in (2,5) then coalesce(lkp_contact.cod_contact,-2)
        else -2
      end cod_contact,
      case
        when coalesce(lkp_resource_type.cod_resource_type,-2) = 4 then coalesce(lkp_lead.cod_lead,-2)
        else -2
      end cod_lead,
      coalesce(lkp_base_user.cod_base_user,-2) cod_base_user,
      source_table.opr_call_outcome,
      source_table.cod_execution,
      coalesce(lkp_resource_type.cod_resource_type,-2) cod_resource_type,
      max_cod_calls.max_cod,
      row_number() over (order by source_table.opr_call desc) new_cod,
      target.cod_call,
      case
        when target.cod_call is null then 'I'
        when source_table.hash_call != target.hash_call then 'U'
          else 'X'
      end dml_type
    from
      (
      select
        *,
        md5
        (coalesce(opr_base_user,0) +
        coalesce(opr_resource,0) +
        coalesce(phone_number,'') +
        decode(flg_missed,1,1,0) +
        coalesce(opr_associated_deal,'') +
        coalesce(summary,'') +
        coalesce(opr_call_outcome,0) +
        coalesce(call_duration,0) +
        decode(flg_incoming,1,1,0) +
        coalesce(recording_url,'') +
        coalesce(opr_resource_type,'')
        ) hash_call
      from
        (
          select
            base_account_country + base_account_category opr_source_system,
            id opr_call,
            resource_id opr_resource,
            user_id opr_base_user,
            phone_number,
            missed flg_missed,
            associated_deal_ids opr_associated_deal,
            made_at created_at,
            updated_at,
            summary,
            outcome_id opr_call_outcome,
            duration call_duration,
            incoming flg_incoming,
            recording_url,
            resource_type opr_resource_type,
            b.cod_source_system,
            row_number() over (partition by id order by meta_event_type desc) rn,
            scai_execution.cod_execution,
            scai_execution.dat_processing
          from
            crm_integration_stg.stg_ro_d_base_calls a,
            crm_integration_anlt.t_lkp_source_system b,
            (
              select
                rel_integr_proc.dat_processing,
                max(fac.cod_execution) cod_execution
              from
                crm_integration_anlt.t_lkp_scai_process proc,
                crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
                crm_integration_anlt.t_fac_scai_execution fac
              where
                rel_integr_proc.cod_process = proc.cod_process
                and rel_integr_proc.cod_country = 4
                and rel_integr_proc.cod_integration = 30000
                and rel_integr_proc.ind_active = 1
                and proc.dsc_process_short = 't_fac_call'
                and fac.cod_process = rel_integr_proc.cod_process
                and fac.cod_integration = rel_integr_proc.cod_integration
                and rel_integr_proc.dat_processing = fac.dat_processing
                and fac.cod_status = 2
              group by
                rel_integr_proc.dat_processing
            ) scai_execution
          where
            (base_account_country + base_account_category) = opr_source_system
            and cod_country = 4
        )
      ) source_table,
      crm_integration_anlt.t_lkp_base_user lkp_base_user,
      crm_integration_anlt.t_lkp_contact lkp_contact,
      crm_integration_anlt.t_lkp_lead lkp_lead,
      crm_integration_anlt.t_lkp_resource_type lkp_resource_type,
      (select coalesce(max(cod_call),0) max_cod from crm_integration_anlt.t_fac_call) max_cod_calls,
      crm_integration_anlt.t_fac_call target
    where
      source_table.opr_call = target.opr_call(+)
      and source_table.cod_source_system = target.cod_source_system (+)
      and coalesce(source_table.opr_base_user,-1) = lkp_base_user.opr_base_user (+)
      and source_table.cod_source_system = lkp_base_user.cod_source_system (+)
      and lkp_base_user.valid_to (+) = 20991231
      and coalesce(source_table.opr_resource,-1) = lkp_contact.opr_contact(+)
      and source_table.cod_source_system = lkp_contact.cod_source_system(+)
      and lkp_contact.valid_to(+) = 20991231
      and coalesce(source_table.opr_resource,-1) = lkp_lead.opr_lead(+)
      and source_table.cod_source_system = lkp_lead.cod_source_system(+)
      and lkp_lead.valid_to(+) = 20991231
      and coalesce(source_table.opr_resource_type,'') = lkp_resource_type.opr_resource_type (+)
      and lkp_resource_type.valid_to (+) = 20991231
      and source_table.rn = 1
  ) source,
  crm_integration_anlt.t_lkp_call_outcome lkp_call_outcome
where
  coalesce(source.opr_call_outcome,-1) = lkp_call_outcome.opr_call_outcome (+)
  and source.cod_source_system = lkp_call_outcome.cod_source_system (+)
  and lkp_call_outcome.valid_to (+) = 20991231;

analyze tmp_ro_load_calls;
	

	
insert into crm_integration_anlt.t_hst_call
    select
      target.*
    from
      crm_integration_anlt.t_fac_call target,
      tmp_ro_load_calls source
    where
      target.opr_call = source.opr_call
      and source.dml_type = 'U';


	
delete from crm_integration_anlt.t_fac_call
using tmp_ro_load_calls
where crm_integration_anlt.t_fac_call.opr_call = tmp_ro_load_calls.opr_call
and tmp_ro_load_calls.dml_type = 'U';


	
insert into crm_integration_anlt.t_fac_call
    select
      case
        when dml_type = 'I' then max_cod + new_cod
        when dml_type = 'U' then cod_call
      end cod_call,
      opr_call,
      cod_contact,
	  cod_lead,
      cod_base_user,
      phone_number,
      flg_missed,
      opr_associated_deal, -- int to varchar
      cod_source_system,
      created_at,
      updated_at,
      summary,
      cod_call_outcome,
      call_duration,
      flg_incoming,
      recording_url,
      cod_resource_type,
      hash_call,
	  cod_execution
    from
      tmp_ro_load_calls
    where
      dml_type in ('U','I');



analyze crm_integration_anlt.t_fac_call;
	  

	
insert into crm_integration_anlt.t_fac_call_deal
select
	call_deal.*,
	scai_execution.cod_execution
from
	(
	select
	  cod_call,
	  case when cod_deal is null then -1 else cod_deal end cod_deal,
	  all_deals.cod_source_system
	from
	  (
			SELECT
			  b.cod_source_system,
			  case when cod_call is null then new_cod else cod_call end cod_call,
			  opr_associated_deal,
			  replace(replace(opr_associated_deal,'[',''),']','') cases -- 0
			FROM
			  tmp_ro_load_calls a,
			  crm_integration_anlt.t_lkp_source_system b
			WHERE
			  a.cod_source_system = b.cod_source_system
			  and cod_country = 4
			  and REGEXP_COUNT (opr_associated_deal,'[,]{1}') = 0
			union
			SELECT
			  b.cod_source_system,
			  case when cod_call is null then new_cod else cod_call end cod_call,
			  opr_associated_deal,
			  replace(replace(split_part(opr_associated_deal, ',', 1),'[',''),']','') cases -- 1
			FROM
			  tmp_ro_load_calls a,
			  crm_integration_anlt.t_lkp_source_system b
			WHERE
			  a.cod_source_system = b.cod_source_system
			  and cod_country = 4
			  and REGEXP_COUNT (opr_associated_deal,'[,]{1}') > 0
			union
			SELECT
			  b.cod_source_system,
			  case when cod_call is null then new_cod else cod_call end cod_call,
			  opr_associated_deal,
			  replace(replace(split_part(opr_associated_deal, ',', 2),']',''),'[','') cases -- 2
			FROM
			  tmp_ro_load_calls a,
			  crm_integration_anlt.t_lkp_source_system b
			WHERE
			  a.cod_source_system = b.cod_source_system
			  and cod_country = 4
			  and REGEXP_COUNT (opr_associated_deal,'[,]{1}') > 0
			union
			SELECT
			  b.cod_source_system,
			  case when cod_call is null then new_cod else cod_call end cod_call,
			  opr_associated_deal,
			  replace(replace(split_part(opr_associated_deal, ',', 3),']',''),'[','') cases -- 3
			FROM
			  tmp_ro_load_calls a,
			  crm_integration_anlt.t_lkp_source_system b
			WHERE
			  a.cod_source_system = b.cod_source_system
			  and cod_country = 4
			  and REGEXP_COUNT (opr_associated_deal,'[,]{1}') > 0
			union
			SELECT
			  b.cod_source_system,
			  case when cod_call is null then new_cod else cod_call end cod_call,
			  opr_associated_deal,
			  replace(replace(split_part(opr_associated_deal, ',', 4),']',''),'[','') cases -- 4
			FROM
			  tmp_ro_load_calls a,
			  crm_integration_anlt.t_lkp_source_system b
			WHERE
			  a.cod_source_system = b.cod_source_system
			  and cod_country = 4
			  and REGEXP_COUNT (opr_associated_deal,'[,]{1}') > 0
			union
			SELECT
			  b.cod_source_system,
			  case when cod_call is null then new_cod else cod_call end cod_call,
			  opr_associated_deal,
			  replace(replace(split_part(opr_associated_deal, ',', 5),']',''),'[','') cases -- 5
			FROM
			  tmp_ro_load_calls a,
			  crm_integration_anlt.t_lkp_source_system b
			WHERE
			  a.cod_source_system = b.cod_source_system
			  and cod_country = 4
			  and REGEXP_COUNT (opr_associated_deal,'[,]{1}') > 0
	) all_deals, crm_integration_anlt.t_lkp_deal lkp_deals
	where all_deals.cases = lkp_deals.opr_deal (+)
	and all_deals.cod_source_system = lkp_deals.cod_source_system (+)
	and cases != ''
	union all
	SELECT
		b.cod_source_system,
		case when cod_call is null then new_cod else cod_call end cod_call,
		-2 cod_deal
	FROM
		tmp_ro_load_calls a,
		crm_integration_anlt.t_lkp_source_system b
	WHERE
		a.cod_source_system = b.cod_source_system
		and cod_country = 4
		and opr_associated_deal = '[]'
	) call_deal,
	(
	  select
        rel_integr_proc.dat_processing,
		max(fac.cod_execution) cod_execution
	  from
		crm_integration_anlt.t_lkp_scai_process proc,
		crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
		crm_integration_anlt.t_fac_scai_execution fac
	  where
		rel_integr_proc.cod_process = proc.cod_process
		and rel_integr_proc.cod_country = 4
		and rel_integr_proc.cod_integration = 30000
		and rel_integr_proc.ind_active = 1
		and proc.dsc_process_short = 't_fac_call'
		and fac.cod_process = rel_integr_proc.cod_process
		and fac.cod_integration = rel_integr_proc.cod_integration
		and rel_integr_proc.dat_processing = fac.dat_processing
		and fac.cod_status = 2
      group by 
        rel_integr_proc.dat_processing
	) scai_execution;



analyze crm_integration_anlt.t_fac_call_deal;
	
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
    (select coalesce(max(cod_execution),0) max_cod_exec from crm_integration_anlt.t_fac_scai_execution),
    crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
    crm_integration_anlt.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 4 -- Romania
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_fac_call';

--$$$

-- #######################
-- ####    PASSO 6    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set cod_status = 1, -- Ok
last_processing_datetime = coalesce((select max(updated_at) from tmp_ro_load_calls),last_processing_datetime)
/*from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc, crm_integration_anlt.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_fac_call'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 4
  ) source*/
from crm_integration_anlt.t_lkp_scai_process proc 
where t_rel_scai_integration_process.cod_process = proc.cod_process
and t_rel_scai_integration_process.cod_status = 2
and t_rel_scai_integration_process.cod_country = 4
and proc.dsc_process_short = 't_fac_call'
and t_rel_scai_integration_process.ind_active = 1
/*crm_integration_anlt.t_rel_scai_integration_process.cod_process = source.cod_process
and crm_integration_anlt.t_rel_scai_integration_process.cod_country = source.cod_country
and crm_integration_anlt.t_rel_scai_integration_process.cod_integration = source.cod_integration*/;


--$$$
	
-- #######################
-- ####    PASSO 3    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set dat_processing = source.dat_processing, execution_nbr = source.execution_nbr, cod_status = 2 -- Running
from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc, crm_integration_anlt.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_fac_order'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 4
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
    (select coalesce(max(cod_execution),0) max_cod_exec from crm_integration_anlt.t_fac_scai_execution),
    crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
    crm_integration_anlt.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 4 -- Romania
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_fac_order';	

--$$$
	
-- #############################################
-- # 	          BASE - Romania               #
-- #           LOADING t_fac_order             #
-- #############################################


create temp table tmp_ro_load_orders 
distkey(cod_source_system)
sortkey(opr_order)
as
  select
    --source_table.opr_source_system,
    source_table.dat_order,
    source_table.opr_order,
    coalesce(lkp_deals.cod_deal,-2) cod_deal,
    source_table.opr_deal,
    source_table.val_discount,
    source_table.created_at,
    source_table.updated_at,
    source_table.hash_order,
    source_table.cod_source_system,
    source_table.cod_execution,
    max_cod_orders.max_cod,
    row_number() over (order by source_table.opr_order desc) new_cod,
    target.cod_order,
    case
      when target.cod_order is null then 'I'
      when source_table.hash_order != target.hash_order then 'U'
        else 'X'
    end dml_type
  from
    (
      SELECT
        base_account_country + base_account_category opr_source_system,
        cast(to_char(created_at, 'YYYYMMDD') as bigint) dat_order,
        id opr_order,
        deal_id opr_deal,
        discount val_discount,
        created_at,
        updated_at,
		b.cod_source_system,
		row_number() over (partition by id order by meta_event_type desc) rn,
        md5(coalesce(deal_id,0) +
            coalesce(discount,0)
        ) hash_order,
        scai_execution.cod_execution,
        scai_execution.dat_processing
      FROM
        crm_integration_stg.stg_ro_d_base_orders a,
        crm_integration_anlt.t_lkp_source_system b,
        (
          select
            rel_integr_proc.dat_processing,
            max(fac.cod_execution) cod_execution
          from
            crm_integration_anlt.t_lkp_scai_process proc,
            crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
            crm_integration_anlt.t_fac_scai_execution fac
          where
            rel_integr_proc.cod_process = proc.cod_process
            and rel_integr_proc.cod_country = 4
            and rel_integr_proc.cod_country = fac.cod_country
            and rel_integr_proc.cod_integration = 30000
            and rel_integr_proc.ind_active = 1
            and proc.dsc_process_short = 't_fac_order'
            and fac.cod_process = rel_integr_proc.cod_process
            and fac.cod_integration = rel_integr_proc.cod_integration
            and rel_integr_proc.dat_processing = fac.dat_processing
            and fac.cod_status = 2
          group by 
            rel_integr_proc.dat_processing
        ) scai_execution
      WHERE
		base_account_country + base_account_category = opr_source_system
        and cod_country = 4
	) source_table,
    (select coalesce(max(cod_order),0) max_cod from crm_integration_anlt.t_fac_order) max_cod_orders,
    crm_integration_anlt.t_lkp_deal lkp_deals,
    crm_integration_anlt.t_fac_order target
  where
    source_table.opr_order = target.opr_order(+)
	and source_table.cod_source_system = target.cod_source_system (+)
    and source_table.rn = 1
	and source_table.opr_deal = lkp_deals.opr_deal (+)
	and source_table.cod_source_system = lkp_deals.cod_source_system (+)
    and lkp_deals.valid_to (+) = 20991231;

analyze tmp_ro_load_orders;
	

	
insert into crm_integration_anlt.t_hst_order
    select
      target.*
    from
      crm_integration_anlt.t_fac_order target,
      tmp_ro_load_orders source
    where
      target.opr_order = source.opr_order
      and source.dml_type = 'U';

	
delete from crm_integration_anlt.t_fac_order
using tmp_ro_load_orders
where crm_integration_anlt.t_fac_order.opr_order = tmp_ro_load_orders.opr_order
and tmp_ro_load_orders.dml_type = 'U';

insert into crm_integration_anlt.t_fac_order
    select
      case
        when dml_type = 'I' then max_cod + new_cod
        when dml_type = 'U' then cod_order
      end cod_order,
      dat_order,
      opr_order,
      cod_source_system,
      cod_deal,
      val_discount,
      created_at,
      updated_at,
      hash_order,
	  cod_execution
    from
      tmp_ro_load_orders
    where
      dml_type in ('U','I');



analyze crm_integration_anlt.t_fac_order;

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
    (select coalesce(max(cod_execution),0) max_cod_exec from crm_integration_anlt.t_fac_scai_execution),
    crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
    crm_integration_anlt.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 4 -- Romania
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_fac_order';

--$$$

-- #######################
-- ####    PASSO 6    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set cod_status = 1, -- Ok
last_processing_datetime = coalesce((select max(updated_at) from tmp_ro_load_orders),last_processing_datetime)
/*from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc, crm_integration_anlt.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_fac_order'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 4
  ) source*/
from crm_integration_anlt.t_lkp_scai_process proc 
where t_rel_scai_integration_process.cod_process = proc.cod_process
and t_rel_scai_integration_process.cod_status = 2
and t_rel_scai_integration_process.cod_country = 4
and proc.dsc_process_short = 't_fac_order'
and t_rel_scai_integration_process.ind_active = 1
/*crm_integration_anlt.t_rel_scai_integration_process.cod_process = source.cod_process
and crm_integration_anlt.t_rel_scai_integration_process.cod_country = source.cod_country
and crm_integration_anlt.t_rel_scai_integration_process.cod_integration = source.cod_integration*/;


--$$$
	
-- #######################
-- ####    PASSO 3    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set dat_processing = source.dat_processing, execution_nbr = source.execution_nbr, cod_status = 2 -- Running
from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc, crm_integration_anlt.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_fac_order_line_item'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 4
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
    (select coalesce(max(cod_execution),0) max_cod_exec from crm_integration_anlt.t_fac_scai_execution),
    crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
    crm_integration_anlt.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 4 -- Romania
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_fac_order_line_item';	

--$$$
	
-- #############################################
-- # 	          BASE - Romania               #
-- #      LOADING t_fac_order_line_item        #
-- #############################################


create temp table tmp_ro_load_order_line_items 
distkey(cod_source_system)
sortkey(opr_order_line_item)
as
  select
    --source_table.opr_source_system,
    source_table.opr_order_line_item,
    --source_table.opr_sku,
    source_table.dsc_order_line_item,
    coalesce(lkp_orders.cod_order,-2) cod_order,
    --source_table.opr_order,
    --source_table.opr_deal,
    source_table.value,
    source_table.price,
    source_table.opr_currency,
    source_table.variation,
    source_table.quantity,
    source_table.created_at,
    source_table.updated_at,
    source_table.hash_order_line_item,
    source_table.cod_source_system,
    coalesce(lkp_product.cod_sku,-2) cod_sku,
    source_table.opr_order,
    source_table.cod_execution,
    coalesce(lkp_currency.cod_currency,-2) cod_currency,
    max_cod_order_line_items.max_cod,
    row_number() over (order by source_table.opr_order_line_item desc) new_cod,
    target.cod_order_line_item,
    case
      when target.cod_order_line_item is null then 'I'
      when source_table.hash_order_line_item != target.hash_order_line_item then 'U'
        else 'X'
    end dml_type
  from
    (
      SELECT
        base_account_country + base_account_category opr_source_system,
        id opr_order_line_item,
        sku opr_sku,
        description dsc_order_line_item,
        order_id opr_order,
        value,
        price,
        currency opr_currency,
        variation,
        quantity,
        created_at,
        updated_at,
		b.cod_source_system,
		row_number() over (partition by id order by meta_event_type desc) rn,
         md5(coalesce(sku,'') + coalesce(description,'') + coalesce(order_id,0) + coalesce(deal_id,0)
            + cast(coalesce(value,0) as varchar) + cast(coalesce(price,0) as varchar) + coalesce(currency,'')
            + cast(coalesce(variation,0) as varchar) + coalesce(quantity,0)
        ) hash_order_line_item,
        scai_execution.cod_execution,
        scai_execution.dat_processing
      FROM
        crm_integration_stg.stg_ro_d_base_line_items a,
        crm_integration_anlt.t_lkp_source_system b,
        (
          select
            rel_integr_proc.dat_processing,
            max(fac.cod_execution) cod_execution
          from
            crm_integration_anlt.t_lkp_scai_process proc,
            crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
            crm_integration_anlt.t_fac_scai_execution fac
          where
            rel_integr_proc.cod_process = proc.cod_process
            and rel_integr_proc.cod_country = 4
            and rel_integr_proc.cod_country = fac.cod_country
            and rel_integr_proc.cod_integration = 30000
            and rel_integr_proc.ind_active = 1
            and proc.dsc_process_short = 't_fac_order_line_item'
            and fac.cod_process = rel_integr_proc.cod_process
            and fac.cod_integration = rel_integr_proc.cod_integration
            and rel_integr_proc.dat_processing = fac.dat_processing
            and fac.cod_status = 2
          group by 
            rel_integr_proc.dat_processing
        ) scai_execution
      WHERE
		base_account_country + base_account_category = opr_source_system
        and cod_country = 4
	) source_table,
    crm_integration_anlt.t_lkp_product lkp_product,
    crm_integration_anlt.t_lkp_currency lkp_currency,
    (select coalesce(max(cod_order_line_item),0) max_cod from crm_integration_anlt.t_fac_order_line_item) max_cod_order_line_items,
    crm_integration_anlt.t_fac_order lkp_orders,
    crm_integration_anlt.t_fac_order_line_item target
  where
    coalesce(source_table.opr_order_line_item,-1) = target.opr_order_line_item(+)
	and source_table.cod_source_system = target.cod_source_system (+)
    and coalesce(source_table.opr_sku,'Unknown') = lkp_product.opr_sku (+)
	and source_table.cod_source_system = lkp_product.cod_source_system (+) -- new
    and lkp_product.valid_to (+) = 20991231
    and coalesce(source_table.opr_currency,'Unknown') = lkp_currency.opr_currency (+)
    and lkp_currency.valid_to (+) = 20991231
    and coalesce(source_table.opr_order,-1) = lkp_orders.opr_order (+) -- TAMBÉM DEVEREMOS CONSIDERAR A DATA DAT_ORDER
	and source_table.cod_source_system = lkp_orders.cod_source_system (+)
	and source_table.rn = 1; -- new

analyse tmp_ro_load_order_line_items;
	

	
insert into crm_integration_anlt.t_hst_order_line_item
    select
      target.*
    from
      crm_integration_anlt.t_fac_order_line_item target,
      tmp_ro_load_order_line_items source
    where
      target.opr_order_line_item = source.opr_order_line_item
      and source.dml_type = 'U';


	
delete from crm_integration_anlt.t_fac_order_line_item
using tmp_ro_load_order_line_items
where crm_integration_anlt.t_fac_order_line_item.opr_order_line_item=tmp_ro_load_order_line_items.opr_order_line_item
and tmp_ro_load_order_line_items.dml_type = 'U';


	
insert into crm_integration_anlt.t_fac_order_line_item
    select
      case
        when dml_type = 'I' then max_cod + new_cod
        when dml_type = 'U' then cod_order_line_item
      end cod_order_line_item,
      opr_order_line_item,
      dsc_order_line_item,
      cod_source_system,
      cod_sku,
      cod_order,
      cod_currency,
      value,
      price,
      variation,
      quantity,
      created_at,
      updated_at,
	  hash_order_line_item,
	  cod_execution
    from
      tmp_ro_load_order_line_items
    where
      dml_type in ('U','I');



analyze crm_integration_anlt.t_fac_order_line_item;
	  
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
    (select coalesce(max(cod_execution),0) max_cod_exec from crm_integration_anlt.t_fac_scai_execution),
    crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
    crm_integration_anlt.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 4 -- Romania
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_fac_order_line_item';

--$$$

-- #######################
-- ####    PASSO 6    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set cod_status = 1, -- Ok
last_processing_datetime = coalesce((select max(updated_at) from tmp_ro_load_order_line_items),last_processing_datetime)
/*from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc, crm_integration_anlt.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_fac_order_line_item'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 4
  ) source*/
from crm_integration_anlt.t_lkp_scai_process proc 
where t_rel_scai_integration_process.cod_process = proc.cod_process
and t_rel_scai_integration_process.cod_status = 2
and t_rel_scai_integration_process.cod_country = 4
and proc.dsc_process_short = 't_fac_order_line_item'
and t_rel_scai_integration_process.ind_active = 1
/*crm_integration_anlt.t_rel_scai_integration_process.cod_process = source.cod_process
and crm_integration_anlt.t_rel_scai_integration_process.cod_country = source.cod_country
and crm_integration_anlt.t_rel_scai_integration_process.cod_integration = source.cod_integration*/;

--$$$

-- #######################
-- ####    PASSO 3    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set dat_processing = source.dat_processing, execution_nbr = source.execution_nbr, cod_status = 2 -- Running
from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc, crm_integration_anlt.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_paidad_index'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 4
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
    (select coalesce(max(cod_execution),0) max_cod_exec from crm_integration_anlt.t_fac_scai_execution),
    crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
    crm_integration_anlt.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 4 -- Romania
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_lkp_paidad_index';	

--$$$
	
-- #############################################
-- # 		     ATLAS - ROMANIA               #
-- #		LOADING t_lkp_paidad_index  	   #
-- #############################################


create temp table tmp_ro_load_paidad_index 
distkey(cod_source_system)
sortkey(cod_paidad_index, opr_paidad_index)
as
select
    source_table.opr_paidad_index,
    source_table.dsc_paidad_index,
    source_table.dsc_paidad_index_pt,
    source_table.dsc_paidad_index_en,
    coalesce(lkp_paidad_index_type.cod_paidad_index_type,-2) cod_paidad_index_type,
    source_table.cod_source_system,
	source_table.operation_type,
	source_table.operation_timestamp,
    source_table.hash_paidad_index,
    max_cod_paidad_index.max_cod,
    row_number() over (order by source_table.opr_paidad_index desc) new_cod,
    target.cod_paidad_index,
    source_table.paidad_index_code,
    source_table.name_pl,
    source_table.name_en,
    source_table.parameters,
    source_table.duration,
    source_table.display_order,
    source_table.simple_user,
    source_table.business_user,
    source_table.fixed_price,
    source_table.lead_pl,
    source_table.lead_en,
    source_table.fk_name,
    source_table.fk_id,
    source_table.user_specific,
    source_table.simple_user_help,
    source_table.opr_paidad_index_related,
    source_table.name_pt,
    source_table.invoiceable,
    source_table.business_promoter_help,
    source_table.business_promoter,
    source_table.business_manager_help,
    source_table.business_manager,
    source_table.business_developer_help,
    source_table.business_developer,
    source_table.business_consultant_help,
    source_table.business_consultant,
    source_table.business_agency_help,
    source_table.business_agency,
    source_table.lead_pt,
    source_table.name_ro,
    source_table.lead_ro,
    source_table.name_ru,
    source_table.name_uk,
    source_table.display_default,
    source_table.lead_hi,
    source_table.name_hi,
    source_table.bonus_credits,
    source_table.loadaccount,
    source_table.free_refresh,
    source_table.free_refresh_frequency,
    source_table.makes_account_premium,
    source_table.recurrencies,
	target.flg_aut_deal_exclude,
    source_table.cod_execution,
    case
      --when target.cod_paidad_index is null then 'I'
	  when target.cod_paidad_index is null or (source_table.hash_paidad_index != target.hash_paidad_index and target.valid_from = (select dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_paidad_index')) then 'I'
	  when source_table.operation_type = 'delete' then 'D'
      when source_table.hash_paidad_index != target.hash_paidad_index then 'U'
        else 'X'
    end dml_type
  from
    (
         select
        source.*,
		lkp_source_system.cod_source_system,
        md5(coalesce(dsc_paidad_index,'') + coalesce(paidad_index_code,'') + coalesce(opr_paidad_index_type,'') + coalesce(name_pl,'') + coalesce(name_en,'') + coalesce(parameters,'') + coalesce(duration,0) + coalesce(display_order,0)
        + coalesce(simple_user,0) + coalesce(business_user,0) + coalesce(fixed_price,0) + coalesce(lead_pl,'') + coalesce(lead_en,'') + coalesce(fk_name,'') + coalesce(fk_id,0)
        + coalesce(user_specific,0) + coalesce(simple_user_help,'') + coalesce(opr_paidad_index_related,0) + coalesce(name_pt,'') + coalesce(invoiceable,0) + coalesce(dsc_paidad_index_pt,'')
        + coalesce(dsc_paidad_index_en,'') + coalesce(business_promoter_help,'') + coalesce(business_promoter,0) + coalesce(business_manager_help,'') + coalesce(business_manager,0)
        + coalesce(business_developer_help,'') + coalesce(business_developer,0) + coalesce(business_consultant_help,'') + coalesce(business_consultant,0)
        + coalesce(business_agency_help,'') + coalesce(business_agency,0) + coalesce(lead_pt,'') + coalesce(name_ro,'') + coalesce(lead_ro,'') + coalesce(name_ru,'')
        + coalesce(name_uk,'') + coalesce(display_default,0) + coalesce(lead_hi,'') + coalesce(name_hi,'') + coalesce(bonus_credits,0) + cast(coalesce(loadaccount,0) as varchar)
        + coalesce(free_refresh,0) + coalesce(free_refresh_frequency,0) + coalesce(makes_account_premium,0) + coalesce(recurrencies,0)
        ) hash_paidad_index
     from
      (
        select
           id opr_paidad_index,
           description dsc_paidad_index,
           livesync_dbname opr_source_system,
		   operation_type,
		   operation_timestamp,
           code paidad_index_code,
          type opr_paidad_index_type,
           name_pl,
           name_en,
           name_pt,
           name_ro,
           name_ru,
           name_uk,
           name_hi,
           parameters,
           duration,
           display_order,
           simple_user,
           business_user,
           fixed_price,
           lead_pl,
           lead_en,
           fk_name,
           fk_id,
           user_specific,
           simple_user_help,
           related_index opr_paidad_index_related, -- Related Index
           invoiceable,
           description_pt dsc_paidad_index_pt,
           description_en dsc_paidad_index_en,
           business_promoter_help,
           business_promoter,
           business_manager_help,
           business_manager,
           business_developer_help,
           business_developer,
           business_consultant_help,
           business_consultant,
           business_agency_help,
           business_agency,
           lead_pt,
           lead_ro,
           display_default,
           lead_hi,
           cast(null as bigint)            bonus_credits,
           cast(null as numeric(8,2))            loadaccount,
           cast(null as bigint)            free_refresh,
           cast(null as bigint)            free_refresh_frequency,
           cast(null as bigint)            makes_account_premium,
           cast(null as bigint)            recurrencies,
           scai_execution.cod_execution
        from
           crm_integration_stg.stg_ro_db_atlas_verticals_paidads_indexes a,
           crm_integration_anlt.t_lkp_source_system b,
          (
            select
              max(fac.cod_execution) cod_execution
            from
              crm_integration_anlt.t_lkp_scai_process proc,
              crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
              crm_integration_anlt.t_fac_scai_execution fac
            where
              rel_integr_proc.cod_process = proc.cod_process
              and rel_integr_proc.cod_country = 4
              and rel_integr_proc.cod_integration = 30000
              and rel_integr_proc.ind_active = 1
              and proc.dsc_process_short = 't_lkp_paidad_index'
              and fac.cod_process = rel_integr_proc.cod_process
              and fac.cod_integration = rel_integr_proc.cod_integration
              and rel_integr_proc.dat_processing = fac.dat_processing
              and fac.cod_status = 2
           ) scai_execution
        where
           a.livesync_dbname = b.opr_source_system
           and b.cod_business_type = 1 -- Verticals
           and b.cod_country = 4 -- Romania
		   --and 1 = 0
        union all
        select
           id opr_paidad_index,
           description dsc_paidad_index,
           'olxro' opr_source_system,
		   operation_type,
		   operation_timestamp,
           code paidad_index_code,
          type opr_paidad_index_type,
           null name_pl,
           null name_en,
           null name_pt,
           name_ro,
           null name_ru,
           null name_uk,
           null name_hi,
           parameters,
           duration,
           display_order,
           simple_user,
           business_user,
           null fixed_price,
           null lead_pl,
           null lead_en,
           null fk_name,
           null fk_id,
           null user_specific,
           null simple_user_help,
           null opr_paidad_index_related,
           null invoiceable,
           null dsc_paidad_index_pt,
           null dsc_paidad_index_en,
           null business_promoter_help,
           null business_promoter,
           null business_manager_help,
           null business_manager,
           null business_developer_help,
           null business_developer,
           null business_consultant_help,
           null business_consultant,
           null business_agency_help,
           null business_agency,
           null lead_pt,
           null lead_ro,
           null display_default,
           null lead_hi,
           bonus_credits,
           null loadaccount,
           null free_refresh,
           null free_refresh_frequency,
           null makes_account_premium,
           null recurrencies,
           scai_execution.cod_execution
        from
          crm_integration_stg.stg_ro_db_atlas_olxro_paidads_indexes,
          (
            select
              max(fac.cod_execution) cod_execution
            from
              crm_integration_anlt.t_lkp_scai_process proc,
              crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
              crm_integration_anlt.t_fac_scai_execution fac
            where
              rel_integr_proc.cod_process = proc.cod_process
              and rel_integr_proc.cod_country = 4
              and rel_integr_proc.cod_integration = 30000
              and rel_integr_proc.ind_active = 1
              and proc.dsc_process_short = 't_lkp_paidad_index'
              and fac.cod_process = rel_integr_proc.cod_process
              and fac.cod_integration = rel_integr_proc.cod_integration
              and rel_integr_proc.dat_processing = fac.dat_processing
              and fac.cod_status = 2
           ) scai_execution
		--where 1 = 0
       ) source,
    crm_integration_anlt.t_lkp_source_system lkp_source_system
	where source.opr_source_system = lkp_source_system.opr_source_system
    ) source_table,
    crm_integration_anlt.t_lkp_paidad_index_type lkp_paidad_index_type,
    (select coalesce(max(cod_paidad_index),0) max_cod from crm_integration_anlt.t_lkp_paidad_index) max_cod_paidad_index,
    (
			select
				*
			from
				(
					SELECT
						a.*,
						row_number()
						OVER (
							PARTITION BY opr_paidad_index, cod_source_system
							ORDER BY valid_to DESC ) rn
					FROM
						crm_integration_anlt.t_lkp_paidad_index a
				)
			where rn = 1
	) target
  where
    coalesce(source_table.opr_paidad_index,-1) = target.opr_paidad_index(+)
	and source_table.cod_source_system = target.cod_source_system (+)
    and coalesce(source_table.opr_paidad_index_type,'Unknown') = lkp_paidad_index_type.opr_paidad_index_type (+)
	and source_table.cod_source_system = lkp_paidad_index_type.cod_source_system (+) -- new
	and lkp_paidad_index_type.valid_to (+) = 20991231;

analyze tmp_ro_load_paidad_index;
	

	
delete from crm_integration_anlt.t_lkp_paidad_index
using tmp_ro_load_paidad_index
where 
	tmp_ro_load_paidad_index.dml_type = 'I' 
	and t_lkp_paidad_index.opr_paidad_index = tmp_ro_load_paidad_index.opr_paidad_index
	and t_lkp_paidad_index.valid_from = (select dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_paidad_index');


	
update crm_integration_anlt.t_lkp_paidad_index
set valid_to = (select rel_integr_proc.dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_paidad_index') 
from tmp_ro_load_paidad_index source
where source.cod_paidad_index = crm_integration_anlt.t_lkp_paidad_index.cod_paidad_index
and crm_integration_anlt.t_lkp_paidad_index.valid_to = 20991231
and source.dml_type in('U','D');


	
insert into crm_integration_anlt.t_lkp_paidad_index
    select
      case
        when dml_type = 'I' then max_cod + new_cod
        when dml_type = 'U' then cod_paidad_index
      end cod_paidad_index,
      opr_paidad_index,
      dsc_paidad_index,
      dsc_paidad_index_pt,
      dsc_paidad_index_en,
      cod_paidad_index_type,
      (select rel_integr_proc.dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_paidad_index') valid_from, 
      20991231 valid_to,
      -2,-- FALTA INDEX RELATED
      paidad_index_code,
      name_pl,
      name_en,
      name_pt,
      name_ro,
      name_ru,
      name_uk,
      name_hi,
      parameters,
      duration,
      display_order,
      simple_user,
      business_user,
      fixed_price,
      lead_pt,
      lead_en,
      lead_hi,
      lead_pl,
      lead_ro,
      user_specific,
      simple_user_help,
      invoiceable,
      business_promoter_help,
      business_promoter,
      business_manager_help,
      business_manager,
      business_developer_help,
      business_developer,
      business_consultant_help,
      business_consultant,
      business_agency_help,
      business_agency,
      display_default,
      fk_id,
      fk_name,
      cast(bonus_credits as bigint)            bonus_credits,
      cast(loadaccount as numeric(8,2))            loadaccount,
      cast(free_refresh as bigint)            free_refresh,
      cast(free_refresh_frequency as bigint)            free_refresh_frequency,
      cast(makes_account_premium as bigint)            makes_account_premium,
      cast(recurrencies as bigint)            recurrencies,
	  flg_aut_deal_exclude,
      cod_source_system,
      hash_paidad_index,
	  cod_execution
    from
      tmp_ro_load_paidad_index
    where
      dml_type in ('U','I');



analyze crm_integration_anlt.t_lkp_paidad_index;
	  

	
update crm_integration_anlt.t_lkp_paidad_index
set cod_paidad_index_related = lkp.cod_paidad_index
from tmp_ro_load_paidad_index source, crm_integration_anlt.t_lkp_paidad_index lkp, crm_integration_anlt.t_lkp_source_system ss
where coalesce(source.opr_paidad_index_related,-1) = lkp.opr_paidad_index
and source.cod_source_system = ss.cod_source_system
and ss.cod_country = 4
and lkp.cod_paidad_index_related = -2
and lkp.valid_to = 20991231;

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
    (select coalesce(max(cod_execution),0) max_cod_exec from crm_integration_anlt.t_fac_scai_execution),
    crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
    crm_integration_anlt.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 4 -- Romania
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_lkp_paidad_index';

--$$$

-- #######################
-- ####    PASSO 6    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set cod_status = 1, -- Ok
last_processing_datetime = coalesce((select max(operation_timestamp) from tmp_ro_load_paidad_index),last_processing_datetime)
/*from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc, crm_integration_anlt.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_paidad_index'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 4
  ) source*/
from crm_integration_anlt.t_lkp_scai_process proc 
where t_rel_scai_integration_process.cod_process = proc.cod_process
and t_rel_scai_integration_process.cod_status = 2
and t_rel_scai_integration_process.cod_country = 4
and proc.dsc_process_short = 't_lkp_paidad_index'
and t_rel_scai_integration_process.ind_active = 1
/*crm_integration_anlt.t_rel_scai_integration_process.cod_process = source.cod_process
and crm_integration_anlt.t_rel_scai_integration_process.cod_country = source.cod_country
and crm_integration_anlt.t_rel_scai_integration_process.cod_integration = source.cod_integration*/;



--$$$

-- #######################
-- ####    PASSO 3    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set dat_processing = source.dat_processing, execution_nbr = source.execution_nbr, cod_status = 2 -- Running
from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc, crm_integration_anlt.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_invoice'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 4
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
    (select coalesce(max(cod_execution),0) max_cod_exec from crm_integration_anlt.t_fac_scai_execution),
    crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
    crm_integration_anlt.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 4 -- Romania
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_lkp_invoice';	

--$$$
	
-- #############################################
-- # 		     ATLAS - ROMANIA               #
-- #		LOADING t_lkp_invoice		  	   #
-- #############################################


create temp table tmp_ro_lkp_invoice_storiaro
distkey(cod_invoice)
sortkey(cod_invoice, opr_invoice_id)
as
  select
    source_table.cod_atlas_user,
    source_table.opr_atlas_id,
    source_table.opr_invoice_id,
    source_table.is_paid,
	  source_table.cod_month,
	  source_table.invoice_date,
    source_table.total_revenue,
    source_table.insertions,
    source_table.promo_units,
    source_table.promo_revenue,
    source_table.listing_revenue,
    source_table.paid_invoices_revenue,
    source_table.hash_invoice,
    source_table.cod_source_system,
    max_cod_invoice.max_cod,
    row_number() over (order by source_table.opr_atlas_id desc) new_cod,
    target.cod_invoice,
	  target.valid_from,
    source_table.cod_execution,
    case
	  when target.cod_invoice is null or (source_table.hash_invoice != target.hash_invoice and target.valid_from = source_table.dat_processing) then 'I'
      when source_table.hash_invoice != target.hash_invoice then 'U'
        else 'X'
    end dml_type
  from
    (
	select
		source.*,
		lkp_source_system.cod_source_system,
		md5
		(
        coalesce(cod_atlas_user   ,0) +
        coalesce(opr_atlas_id   ,0) +
        coalesce(opr_invoice_id   ,0) +
        coalesce(is_paid   , -1) +
        coalesce(total_revenue   ,0) +
        coalesce(insertions   ,0) +
        coalesce(promo_units   ,0) +
        coalesce(promo_revenue   ,0) +
        coalesce(listing_revenue   ,0) +
        coalesce(paid_invoices_revenue   ,0)
    ) hash_invoice
	from
	(
      SELECT
        cod_atlas_user,
        opr_atlas_id,
        opr_invoice_id,
        is_paid,
        cod_month,
        invoice_date,
        total_revenue,
        insertions,
        promo_units,
        promo_revenue,
        listing_revenue,
        paid_invoices_revenue,
        scai_execution.cod_execution,
        scai_execution.dat_processing
      FROM
        (SELECT
            cod_atlas_user,
            opr_atlas_id,
            opr_invoice_id,
            is_paid,
            cod_month,
            invoice_date,
            total_revenue,
            insertions,
            promo_units,
            promo_revenue,
            case
              when (total_revenue - promo_revenue) >= 0 then (total_revenue - promo_revenue)
              else round(total_revenue - (promo_revenue/2),2)
            end listing_revenue,
            case
              when is_paid = 1 then total_revenue
              else 0
            end paid_invoices_revenue
          FROM
            (
          SELECT
              atlas_user.cod_atlas_user,
              pup.id_user opr_atlas_id,
              fi.id opr_invoice_id,
              case
                when fi.paid_value = 0 then 0
                else 1
              end is_paid,
              to_char(fi.created_at,'YYYYMM') cod_month,
              fi.created_at  invoice_date,
              round(fi.total_gross_amount/(100*4.6), 2) total_revenue,
              count(case
                  when pi.type in ('paid_for_post')
                      and (c.id in (1, 2, 11)
                          or c.parent_id in (1, 2, 11))
                  then pup.id_ad
              end) insertions,
              count(case
                  when pi.type not in ('paid_for_post', 'export_olx')
                      then pup.price
              end) promo_units,
              round(sum(case
                  when pi.type not in ('paid_for_post', 'export_olx')
                then abs(pup.price)
                  else 0
              end), 2) promo_revenue
          from
              db_atlas_verticals.paidads_user_payments pup
                  join db_atlas_verticals.paidads_indexes pi
                      on pi.id = pup.id_index
                  join db_atlas_verticals.users_business ub
                      on ub.id = pup.id_user
              join db_atlas_verticals.users u
                on u.id = ub.id
                  join db_atlas_verticals.sap_invoices fi
                      on fi.id = pup.sap_id_invoice
                  join db_atlas_verticals.ads a
                      on pup.id_ad = a.id
                  join db_atlas_verticals.categories c
                      on c.id = a.category_id
                  left outer join crm_integration_anlt.t_lkp_atlas_user atlas_user
                      on atlas_user.opr_atlas_user = u.id and atlas_user.cod_source_system = 1 and atlas_user.valid_to = 20991231
          where
              pi.type in ('topads',
          'paid_for_post',
          'ad_homepage',
          'pushup',
          'export_olx',
          'promo_packet_topads',
          'promo_packet_pushup',
          'promo_packet_topads_7',
          'promo_packet_topads_30',
          'exclusive_offer',
          'promo_packet_export_olx',
          'header'
          )
                  and pup.payment_provider not in ('admin')
                  and to_char(fi.created_at,'YYYYMMDD') >= 20180101  
                  and pi.livesync_dbname = 'storiaro'
                  and pi.livesync_dbname = ub.livesync_dbname
                  and pi.livesync_dbname = u.livesync_dbname
                  and pi.livesync_dbname = a.livesync_dbname
                  and pi.livesync_dbname = c.livesync_dbname
                  and pi.livesync_dbname = fi.livesync_dbname
          group by
              1, 2, 3, 4, fi.created_at, fi.total_gross_amount
          order by
              1, 2 desc) a
            where 1=1),
        (
          select
            rel_integr_proc.dat_processing,
            max(fac.cod_execution) cod_execution
          from
            crm_integration_anlt.t_lkp_scai_process proc,
            crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
            crm_integration_anlt.t_fac_scai_execution fac
          where
            rel_integr_proc.cod_process = proc.cod_process
            and rel_integr_proc.cod_country = 4
            and rel_integr_proc.cod_country = fac.cod_country
            and rel_integr_proc.cod_integration = 30000
            and rel_integr_proc.ind_active = 1
            and proc.dsc_process_short = 't_lkp_invoice'
            and fac.cod_process = rel_integr_proc.cod_process
            and fac.cod_integration = rel_integr_proc.cod_integration
            and rel_integr_proc.dat_processing = fac.dat_processing
            and fac.cod_status = 2
          group by
            rel_integr_proc.dat_processing
        ) scai_execution
	) source,
    crm_integration_anlt.t_lkp_source_system lkp_source_system
	where lkp_source_system.cod_source_system = 1
	and lkp_source_system.cod_country = 4 -- Romania
	) source_table,
    (select coalesce(max(cod_invoice),0) max_cod from crm_integration_anlt.t_lkp_invoice) max_cod_invoice,
    (SELECT a.*
        FROM
          crm_integration_anlt.t_lkp_invoice a
        where 1=1
        and cod_source_system = 1
        and valid_to = 20991231
	) target
  where
    coalesce(source_table.opr_invoice_id,-1) = target.opr_invoice_id(+)
	and source_table.cod_source_system = target.cod_source_system (+)
;


analyze tmp_ro_lkp_invoice_storiaro;

 
delete from crm_integration_anlt.t_lkp_invoice
using tmp_ro_lkp_invoice_storiaro
where
	tmp_ro_lkp_invoice_storiaro.dml_type = 'I'
	and t_lkp_invoice.opr_atlas_id = tmp_ro_lkp_invoice_storiaro.opr_atlas_id
	and t_lkp_invoice.valid_from = (select dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_invoice');



update crm_integration_anlt.t_lkp_invoice
	set valid_to = (select rel_integr_proc.dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_invoice')
from tmp_ro_lkp_invoice_storiaro source
where 
	source.opr_atlas_id = crm_integration_anlt.t_lkp_invoice.opr_atlas_id
	and crm_integration_anlt.t_lkp_invoice.valid_to = 20991231
	and source.dml_type in('U','D');



insert into crm_integration_anlt.t_lkp_invoice
	 select
      case
        when dml_type = 'I' then case when valid_from = (select dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc
														where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_invoice')
									then cod_invoice else max_cod + new_cod end
        when dml_type = 'U' then cod_invoice
      end cod_invoice,
      cod_source_system,
	  (select rel_integr_proc.dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_invoice') valid_from,
	  20991231 valid_to,
	  cod_atlas_user,
	  opr_atlas_id,
	  opr_invoice_id,
	  is_paid,
	  cod_month,
	  invoice_date,
	  total_revenue,
	  insertions,
	  promo_units,
	  promo_revenue,
	  listing_revenue,
	  paid_invoices_revenue,
	  hash_invoice,
	  cod_execution
    from
      tmp_ro_lkp_invoice_storiaro
    where
      dml_type in ('U','I');


analyze crm_integration_anlt.t_lkp_invoice;






create temp table tmp_ro_lkp_invoice_autovitro
distkey(cod_invoice)
sortkey(cod_invoice, opr_invoice_id)
as
  select
    source_table.cod_atlas_user,
    source_table.opr_atlas_id,
    source_table.opr_invoice_id,
    source_table.is_paid,
	  source_table.cod_month,
	  source_table.invoice_date,
    source_table.total_revenue,
    source_table.insertions,
    source_table.promo_units,
    source_table.promo_revenue,
    source_table.listing_revenue,
    source_table.paid_invoices_revenue,
    source_table.hash_invoice,
    source_table.cod_source_system,
    max_cod_invoice.max_cod,
    row_number() over (order by source_table.opr_atlas_id desc) new_cod,
    target.cod_invoice,
	  target.valid_from,
    source_table.cod_execution,
    case
	  when target.cod_invoice is null or (source_table.hash_invoice != target.hash_invoice and target.valid_from = source_table.dat_processing) then 'I'
      when source_table.hash_invoice != target.hash_invoice then 'U'
        else 'X'
    end dml_type
  from
    (
	select
		source.*,
		lkp_source_system.cod_source_system,
		md5
		(
        coalesce(cod_atlas_user   ,0) +
        coalesce(opr_atlas_id   ,0) +
        coalesce(opr_invoice_id   ,0) +
        coalesce(is_paid   , -1) +
        coalesce(total_revenue   ,0) +
        coalesce(insertions   ,0) +
        coalesce(promo_units   ,0) +
        coalesce(promo_revenue   ,0) +
        coalesce(listing_revenue   ,0) +
        coalesce(paid_invoices_revenue   ,0)
    ) hash_invoice
	from
	(
      SELECT
        cod_atlas_user,
        opr_atlas_id,
        opr_invoice_id,
        is_paid,
        cod_month,
        invoice_date,
        total_revenue,
        insertions,
        promo_units,
        promo_revenue,
        listing_revenue,
        paid_invoices_revenue,
        scai_execution.cod_execution,
        scai_execution.dat_processing
      FROM
        (SELECT
            cod_atlas_user,
            opr_atlas_id,
            opr_invoice_id,
            is_paid,
            cod_month,
            invoice_date,
            total_revenue,
            insertions,
            promo_units,
            promo_revenue,
            case
                when (total_revenue - promo_revenue) >= 0 then (total_revenue - promo_revenue)
                else round(total_revenue - (promo_revenue/2),2)
              end listing_revenue,
              case
                when is_paid = 1 then total_revenue
                else 0
              end paid_invoices_revenue
            FROM
              (
            SELECT
                atlas_user.cod_atlas_user,
                pup.id_user opr_atlas_id,
                pup.id_user user_id,
                fi.id opr_invoice_id,
                case
                  when fi.paid_value = 0 then 0
                  else 1
                end is_paid,
                to_char(fi.created_at,'YYYYMM') cod_month,
                date(fi.created_at) invoice_date,
                case
                  when base_user.dsc_base_user is null or base_user.dsc_base_user = 'N/A' then 'Unassigned'
                  else base_user.dsc_base_user
                end region,
                coalesce(round(fi.total_gross_amount/(100*4.6), 2),0) total_revenue,
                count(case
                    when pi.type in ('paid_for_post')
                        and (c.id in (1, 9, 29, 31, 57, 65, 67, 81)
                            or c.parent_id in (1, 9, 29, 31, 57, 65, 67, 81))
                    then pup.id_ad
                end) insertions,
                count(case
                    when pi.type not in ('paid_for_post', 'export_olx')
                        then pup.price
                end) promo_units,
                round(sum(case
                    when pi.type not in ('paid_for_post', 'export_olx')
                  and ub.country_id = 1
                  then abs(pup.price)
                when pi.type not in ('paid_for_post', 'export_olx')
                  and ub.country_id != 1
                  then abs(pup.price)/1.19
                    else 0
                end), 2) promo_revenue
            from
                db_atlas_verticals.paidads_user_payments pup
                    join db_atlas_verticals.paidads_indexes pi
                        on pi.id = pup.id_index
                    join db_atlas_verticals.users_business ub
                        on ub.id = pup.id_user
                    join db_atlas_verticals.users u
                        on u.id = ub.id
                    join db_atlas_verticals.sap_invoices fi
                        on fi.id = pup.id_invoice_sap
                    join db_atlas_verticals.ads a
                        on pup.id_ad = a.id
                    join db_atlas_verticals.categories c
                        on c.id = a.category_id
                  left outer join crm_integration_anlt.t_lkp_atlas_user atlas_user
                      on atlas_user.opr_atlas_user = u.id and atlas_user.cod_source_system = 5 and atlas_user.valid_to = 20991231
                    left join (
                        select
                          *
                        from
                          (
                            select
                              base_contact.cod_contact,
                              base_contact.email,
                              base_contact.cod_contact_parent,
                              base_contact.cod_base_user_owner,
                              base_contact.cod_source_system,
                              atlas_user.opr_atlas_user,
                              row_number() over (partition by atlas_user.opr_atlas_user order by cod_contact_parent desc) rn
                            from
                              crm_integration_anlt.t_lkp_contact base_contact,
                              crm_integration_anlt.t_lkp_atlas_user atlas_user
                            where
                              base_contact.valid_to = 20991231
                              and atlas_user.valid_to = 20991231
                              and lower(base_contact.email) = lower(atlas_user.dsc_atlas_user)
                              and coalesce(base_contact.email,'') != ''
                              and atlas_user.cod_source_system = 5
                              and base_contact.cod_source_system = 18
                          )
                        where rn = 1
                        ) contact on contact.opr_atlas_user = ub.id
                    left join crm_integration_anlt.t_lkp_base_user base_user
                        on base_user.cod_base_user = contact.cod_base_user_owner
                        and base_user.cod_source_system = contact.cod_source_system
                        and base_user.valid_to = 20991231
                        and base_user.cod_source_system = 18
            where
                pi.type in ('ad_bighomepage', 'ad_homepage', 'bump_up',
                    'highlight', 'topads', 'paid_for_post', 'export_olx')
                    and pup.payment_provider not in ('admin')
                    and date(fi.created_at) >= '2018-01-01'
                    and ub.id not in (521225, 521223, 521213, 521215, 522479,
                        522495, 24280, 419691, 522665, 18964, 1030125, 522513,
                        1057085, 1058565, 1066171, 1285659, 1285679, 829479,
                        17628)
                    and pup.livesync_dbname = 'autovitro'
                    and pup.livesync_dbname = pi.livesync_dbname
                    and pup.livesync_dbname = ub.livesync_dbname
                    and pup.livesync_dbname = u.livesync_dbname
                    and pup.livesync_dbname = c.livesync_dbname
                    and pup.livesync_dbname = fi.livesync_dbname
                    and pup.livesync_dbname = a.livesync_dbname
            group by
                1,2,3,4, fi.paid_value, fi.created_at, base_user.dsc_base_user, fi.total_gross_amount
            order by
                1, 2 desc) a
            where 1=1),
        (
          select
            rel_integr_proc.dat_processing,
            max(fac.cod_execution) cod_execution
          from
            crm_integration_anlt.t_lkp_scai_process proc,
            crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
            crm_integration_anlt.t_fac_scai_execution fac
          where
            rel_integr_proc.cod_process = proc.cod_process
            and rel_integr_proc.cod_country = 4
            and rel_integr_proc.cod_country = fac.cod_country
            and rel_integr_proc.cod_integration = 30000
            and rel_integr_proc.ind_active = 1
            and proc.dsc_process_short = 't_lkp_invoice'
            and fac.cod_process = rel_integr_proc.cod_process
            and fac.cod_integration = rel_integr_proc.cod_integration
            and rel_integr_proc.dat_processing = fac.dat_processing
            and fac.cod_status = 2
          group by
            rel_integr_proc.dat_processing
        ) scai_execution
	) source,
    crm_integration_anlt.t_lkp_source_system lkp_source_system
	where lkp_source_system.cod_source_system = 5
	and lkp_source_system.cod_country = 4 -- Romania
	) source_table,
    (select coalesce(max(cod_invoice),0) max_cod from crm_integration_anlt.t_lkp_invoice) max_cod_invoice,
    (SELECT a.*
        FROM
          crm_integration_anlt.t_lkp_invoice a
        where 1=1
        and cod_source_system = 5
        and valid_to = 20991231
	) target
  where
    coalesce(source_table.opr_invoice_id,-1) = target.opr_invoice_id(+)
	and source_table.cod_source_system = target.cod_source_system (+)
;


analyze tmp_ro_lkp_invoice_autovitro;

 

delete from crm_integration_anlt.t_lkp_invoice
using tmp_ro_lkp_invoice_autovitro
where
	tmp_ro_lkp_invoice_autovitro.dml_type = 'I'
	and t_lkp_invoice.opr_atlas_id = tmp_ro_lkp_invoice_autovitro.opr_atlas_id
	and t_lkp_invoice.valid_from = (select dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_invoice');



update crm_integration_anlt.t_lkp_invoice
set valid_to = (select rel_integr_proc.dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_invoice')
from tmp_ro_lkp_invoice_autovitro source
where source.opr_atlas_id = crm_integration_anlt.t_lkp_invoice.opr_atlas_id
and crm_integration_anlt.t_lkp_invoice.valid_to = 20991231
and source.dml_type in('U','D');



insert into crm_integration_anlt.t_lkp_invoice
	 select
      case
        when dml_type = 'I' then case when valid_from = (select dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc
														where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_invoice')
									then cod_invoice else max_cod + new_cod end
        when dml_type = 'U' then cod_invoice
      end cod_invoice,
     cod_source_system,
	  (select rel_integr_proc.dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_invoice') valid_from,
    20991231 valid_to,
	  cod_atlas_user,
	  opr_atlas_id,
	  opr_invoice_id,
	  is_paid,
	  cod_month,
	  invoice_date,
	  total_revenue,
	  insertions,
	  promo_units,
	  promo_revenue,
	  listing_revenue,
	  paid_invoices_revenue,
	  hash_invoice,
	  cod_execution
    from
      tmp_ro_lkp_invoice_autovitro
    where
      dml_type in ('U','I');


analyze crm_integration_anlt.t_lkp_invoice;


-- ROMANIA - REVENUE (Storia, Autovit)

delete from crm_integration_anlt.t_fac_crm_revenue_month_agg where cod_source_system_adapted in(1,2);

insert into crm_integration_anlt.t_fac_crm_revenue_month_agg (cod_month, cod_source_system_adapted, cod_index_type, cod_base_user, val_revenue, val_revenue_mtd, num_users, num_users_mtd)
select
    cast(core.cod_month as numeric) cod_month,
    core.cod_source_system_adapted,
    1 cod_index_type, -- VAS
    core.cod_base_user,
    round(sum(core.promo_revenue) / 1.19,2) val_revenue,
    round(sum(revenue_mtd.promo_revenue) / 1.19,2) val_revenue_mtd,
    count(distinct core.atlas_cod_atlas_user) num_users,
    count(distinct revenue_mtd.atlas_cod_atlas_user) num_users_mtd
  from
    (
      select
        *,
        row_number() over (partition by a.cod_atlas_user,b.cod_month,b.opr_invoice_id order by cod_base_user) rn
      from
        (
          select
            distinct coalesce(base.cod_base_user_owner,-2) cod_base_user,
            atlas.cod_atlas_user,
            atlas.cod_atlas_user atlas_cod_atlas_user,
            atlas.cod_source_system,
            atlas.opr_atlas_user
          from
            (
              select
                a.*
              from
                crm_integration_anlt.t_lkp_contact a,
                crm_integration_anlt.t_lkp_source_system b
              where
                a.cod_source_system = b.cod_source_system
                and a.valid_to = 20991231
                and b.cod_source_system = 19
                --and cod_atlas_user = 141029676
            ) base,
            (
              select
                a.*
              from
                crm_integration_anlt.t_lkp_atlas_user a,
                crm_integration_anlt.t_lkp_source_system b
              where
                a.cod_source_system = b.cod_source_system
                and a.valid_to = 20991231
                and b.cod_source_system = 1
                and a.flg_business = 1
                --and a.opr_atlas_user in (206)
            ) atlas
          where
            atlas.cod_atlas_user = base.cod_atlas_user(+)
        ) a,
        crm_integration_anlt.t_lkp_invoice b,
        crm_integration_anlt.t_lkp_source_system c,
        crm_integration_anlt.v_lkp_source_system_adapted d
      where
        b.opr_atlas_id
        and b.cod_source_system = c.cod_source_system
				and b.cod_source_system = 1
				and b.valid_to = 20991231
        and a.cod_source_system = c.cod_source_system
        and b.is_paid = 1
        and c.opr_source_system = d.dsc_source_system
        and b.promo_revenue != 0
      ) core,
    (
      select
        *,
        row_number() over (partition by a.cod_atlas_user,b.cod_month,b.opr_invoice_id order by cod_base_user) rn
      from
        (
          select
            distinct coalesce(base.cod_base_user_owner,-2) cod_base_user,
            atlas.cod_atlas_user,
            atlas.cod_atlas_user atlas_cod_atlas_user,
            atlas.cod_source_system,
            atlas.opr_atlas_user
          from
            (
              select
                a.*
              from
                crm_integration_anlt.t_lkp_contact a,
                crm_integration_anlt.t_lkp_source_system b
              where
                a.cod_source_system = b.cod_source_system
                and a.valid_to = 20991231
                and b.cod_source_system = 19
                --and cod_atlas_user = 141029676
            ) base,
            (
              select
                a.*
              from
                crm_integration_anlt.t_lkp_atlas_user a,
                crm_integration_anlt.t_lkp_source_system b
              where
                a.cod_source_system = b.cod_source_system
                and a.valid_to = 20991231
                and b.cod_source_system = 1
                and a.flg_business = 1
                --and a.opr_atlas_user in (206)
            ) atlas
          where
            atlas.cod_atlas_user = base.cod_atlas_user(+)
        ) a,
        (select invoice.*, case when invoice.cod_month < 201804 then substring(invoice.cod_month,1,4)+'-'+substring(invoice.cod_month,5,2)+'-'+substring(to_char(invoice_date,'yyyy-mm-dd'),3,2) else to_char(invoice_date,'yyyy-mm-dd') end invoice_date_final from crm_integration_anlt.t_lkp_invoice invoice) b,
        crm_integration_anlt.t_lkp_source_system c,
        crm_integration_anlt.v_lkp_source_system_adapted d,
        (select max(cod_month), max(b.invoice_date_final) invoice_date_final_day from (select invoice.*, case when invoice.cod_month < 201804 then substring(invoice.cod_month,1,4)+'-'+substring(invoice.cod_month,5,2)+'-'+substring(to_char(invoice_date,'yyyy-mm-dd'),3,2) else to_char(invoice_date,'yyyy-mm-dd') end invoice_date_final from crm_integration_anlt.t_lkp_invoice invoice) b
          where b.cod_source_system = 1
		  and b.valid_to = 20991231
            and is_paid = 1
            ) date
      where
        a.opr_atlas_user = b.opr_atlas_id
        and b.cod_source_system = c.cod_source_system
				and b.cod_source_system = 1
				and b.valid_to = 20991231
        and b.is_paid = 1
        and c.opr_source_system = d.dsc_source_system
        and b.promo_revenue != 0
        and b.invoice_date_final between substring(b.invoice_date_final,1,8)+'01' and substring(b.invoice_date_final,1,8)+substring(date.invoice_date_final_day,9,2)
      ) revenue_mtd
  where
    core.rn = 1
    and revenue_mtd.rn (+) = 1
    and core.cod_base_user = revenue_mtd.cod_base_user (+)
    and core.cod_month = revenue_mtd.cod_month (+)
    and core.opr_invoice_id = revenue_mtd.opr_invoice_id (+)
    and core.cod_source_system_adapted = revenue_mtd.cod_source_system_adapted (+)
  group by
    core.cod_month,
    core.cod_source_system_adapted,
    1,
    core.cod_base_user

  union all

  select
    cast(core.cod_month as numeric) cod_month,
    core.cod_source_system_adapted,
    2 cod_index_type, -- Packages
    core.cod_base_user,
    round(sum(core.listing_revenue) / 1.19,2) val_revenue,
    round(sum(revenue_mtd.listing_revenue) / 1.19,2) val_revenue_mtd,
    count(distinct core.atlas_cod_atlas_user) num_users,
    count(distinct revenue_mtd.atlas_cod_atlas_user) num_users_mtd
  from
    (
      select
        *,
        row_number() over (partition by a.cod_atlas_user,b.cod_month,b.opr_invoice_id order by cod_base_user) rn
      from
        (
          select
            distinct coalesce(base.cod_base_user_owner,-2) cod_base_user,
            atlas.cod_atlas_user,
            atlas.cod_atlas_user atlas_cod_atlas_user,
            atlas.cod_source_system,
            atlas.opr_atlas_user
          from
            (
              select
                a.*
              from
                crm_integration_anlt.t_lkp_contact a,
                crm_integration_anlt.t_lkp_source_system b
              where
                a.cod_source_system = b.cod_source_system
                and a.valid_to = 20991231
                and b.cod_source_system = 19
                --and cod_atlas_user = 141029676
            ) base,
            (
              select
                a.*
              from
                crm_integration_anlt.t_lkp_atlas_user a,
                crm_integration_anlt.t_lkp_source_system b
              where
                a.cod_source_system = b.cod_source_system
                and a.valid_to = 20991231
                and b.cod_source_system = 1
                and a.flg_business = 1
                --and a.opr_atlas_user in (206)
            ) atlas
          where
            atlas.cod_atlas_user = base.cod_atlas_user(+)
        ) a,
        crm_integration_anlt.t_lkp_invoice b,
        crm_integration_anlt.t_lkp_source_system c,
        crm_integration_anlt.v_lkp_source_system_adapted d
      where
        a.opr_atlas_user = b.opr_atlas_id
        and b.cod_source_system = c.cod_source_system
				and b.cod_source_system = 1
				and b.valid_to = 20991231
        and b.is_paid = 1
        and c.opr_source_system = d.dsc_source_system
        and b.listing_revenue != 0
      ) core,
    (
      select
        *,
        row_number() over (partition by a.cod_atlas_user,b.cod_month,b.opr_invoice_id order by cod_base_user) rn
      from
        (
          select
            distinct coalesce(base.cod_base_user_owner,-2) cod_base_user,
            atlas.cod_atlas_user,
            atlas.cod_atlas_user atlas_cod_atlas_user,
            atlas.cod_source_system,
            atlas.opr_atlas_user
          from
            (
              select
                a.*
              from
                crm_integration_anlt.t_lkp_contact a,
                crm_integration_anlt.t_lkp_source_system b
              where
                a.cod_source_system = b.cod_source_system
                and a.valid_to = 20991231
                and b.cod_source_system = 19
                --and cod_atlas_user = 141029676
            ) base,
            (
              select
                a.*
              from
                crm_integration_anlt.t_lkp_atlas_user a,
                crm_integration_anlt.t_lkp_source_system b
              where
                a.cod_source_system = b.cod_source_system
                and a.valid_to = 20991231
                and b.cod_source_system = 1
                and a.flg_business = 1
                --and a.opr_atlas_user in (206)
            ) atlas
          where
            atlas.cod_atlas_user = base.cod_atlas_user(+)
        ) a,
        (select invoice.*, case when cod_month < 201804 then substring(cod_month,1,4)+'-'+substring(cod_month,5,2)+'-'+substring(to_char(invoice_date,'yyyy-mm-dd'),3,2) else to_char(invoice_date,'yyyy-mm-dd') end invoice_date_final from crm_integration_anlt.t_lkp_invoice invoice) b,
        crm_integration_anlt.t_lkp_source_system c,
        crm_integration_anlt.v_lkp_source_system_adapted d,
        (select max(cod_month), max(b.invoice_date_final) invoice_date_final_day from (select invoice.*, case when cod_month < 201804 then substring(cod_month,1,4)+'-'+substring(cod_month,5,2)+'-'+substring(to_char(invoice_date,'yyyy-mm-dd'),3,2) else to_char(invoice_date,'yyyy-mm-dd') end invoice_date_final from crm_integration_anlt.t_lkp_invoice invoice) b
          where b.cod_source_system = 1
		  and b.valid_to = 20991231
            and is_paid = 1
            ) date
      where
        a.opr_atlas_user = b.opr_atlas_id
        and b.cod_source_system = c.cod_source_system
				and b.cod_source_system = 1
				and b.valid_to = 20991231
        and b.is_paid = 1
        and c.opr_source_system = d.dsc_source_system
        and b.listing_revenue != 0
        and b.invoice_date_final between substring(b.invoice_date_final,1,8)+'01' and substring(b.invoice_date_final,1,8)+substring(date.invoice_date_final_day,9,2)
      ) revenue_mtd
  where
    core.rn = 1
    and revenue_mtd.rn (+) = 1
    and core.cod_base_user = revenue_mtd.cod_base_user (+)
    and core.cod_month = revenue_mtd.cod_month (+)
    and core.opr_invoice_id = revenue_mtd.opr_invoice_id (+)
    and core.cod_source_system_adapted = revenue_mtd.cod_source_system_adapted (+)
  group by
    core.cod_month,
    core.cod_source_system_adapted,
    2,
    core.cod_base_user

  union all

  select
    cast(core.cod_month as numeric) cod_month,
    core.cod_source_system_adapted,
    1 cod_index_type, -- VAS
    core.cod_base_user,
    round(sum(core.promo_revenue) / 1.19,2) val_revenue,
    round(sum(revenue_mtd.promo_revenue) / 1.19,2) val_revenue_mtd,
    count(distinct core.atlas_cod_atlas_user) num_users,
    count(distinct revenue_mtd.atlas_cod_atlas_user) num_users_mtd
  from
    (
      select
        *,
        row_number() over (partition by a.cod_atlas_user,b.cod_month,b.opr_invoice_id order by cod_base_user) rn
      from
        (
          select
            distinct coalesce(base.cod_base_user_owner,-2) cod_base_user,
            atlas.cod_atlas_user,
            atlas.cod_atlas_user atlas_cod_atlas_user,
            atlas.cod_source_system,
            atlas.opr_atlas_user
          from
            (
              select
                a.*
              from
                crm_integration_anlt.t_lkp_contact a,
                crm_integration_anlt.t_lkp_source_system b
              where
                a.cod_source_system = b.cod_source_system
                and a.valid_to = 20991231
                and b.cod_source_system = 18
                --and cod_atlas_user = 141029676
            ) base,
            (
              select
                a.*
              from
                crm_integration_anlt.t_lkp_atlas_user a,
                crm_integration_anlt.t_lkp_source_system b
              where
                a.cod_source_system = b.cod_source_system
                and a.valid_to = 20991231
                and b.cod_source_system = 5
                and a.flg_business = 1
                --and a.opr_atlas_user in (206)
            ) atlas
          where
            atlas.cod_atlas_user = base.cod_atlas_user(+)
        ) a,
        crm_integration_anlt.t_lkp_invoice b,
        crm_integration_anlt.t_lkp_source_system c,
        crm_integration_anlt.v_lkp_source_system_adapted d
      where
        a.opr_atlas_user = b.opr_atlas_id
        and b.cod_source_system = c.cod_source_system
				and b.cod_source_system = 1
				and b.valid_to = 20991231
        and b.is_paid = 1
        and c.opr_source_system = d.dsc_source_system
        and b.promo_revenue != 0
      ) core,
    (
      select
        *,
        row_number() over (partition by a.cod_atlas_user,b.cod_month,b.opr_invoice_id order by cod_base_user) rn
      from
        (
          select
            distinct coalesce(base.cod_base_user_owner,-2) cod_base_user,
            atlas.cod_atlas_user,
            atlas.cod_atlas_user atlas_cod_atlas_user,
            atlas.cod_source_system,
            atlas.opr_atlas_user
          from
            (
              select
                a.*
              from
                crm_integration_anlt.t_lkp_contact a,
                crm_integration_anlt.t_lkp_source_system b
              where
                a.cod_source_system = b.cod_source_system
                and a.valid_to = 20991231
                and b.cod_source_system = 18
                --and cod_atlas_user = 141029676
            ) base,
            (
              select
                a.*
              from
                crm_integration_anlt.t_lkp_atlas_user a,
                crm_integration_anlt.t_lkp_source_system b
              where
                a.cod_source_system = b.cod_source_system
                and a.valid_to = 20991231
                and b.cod_source_system = 5
                and a.flg_business = 1
                --and a.opr_atlas_user in (206)
            ) atlas
          where
            atlas.cod_atlas_user = base.cod_atlas_user(+)
        ) a,
        (select invoice.*, case when cod_month < 201804 then substring(cod_month,1,4)+'-'+substring(cod_month,5,2)+'-'+substring(to_char(invoice_date,'yyyy-mm-dd'),3,2) else to_char(invoice_date,'yyyy-mm-dd') end invoice_date_final from crm_integration_anlt.t_lkp_invoice invoice) b,
        crm_integration_anlt.t_lkp_source_system c,
        crm_integration_anlt.v_lkp_source_system_adapted d,
        (select max(cod_month), max(b.invoice_date_final) invoice_date_final_day from (select invoice.*, case when cod_month < 201804 then substring(cod_month,1,4)+'-'+substring(cod_month,5,2)+'-'+substring(to_char(invoice_date,'yyyy-mm-dd'),3,2) else to_char(invoice_date,'yyyy-mm-dd') end invoice_date_final from crm_integration_anlt.t_lkp_invoice invoice) b
          where b.cod_source_system = 5
		  and b.valid_to = 20991231
            and is_paid = 1
            ) date
      where
        a.opr_atlas_user = b.opr_atlas_id
        and b.cod_source_system = c.cod_source_system
				and b.cod_source_system = 5
				and b.valid_to = 20991231
        and b.is_paid = 1
        and c.opr_source_system = d.dsc_source_system
        and b.promo_revenue != 0
        and b.invoice_date_final between substring(b.invoice_date_final,1,8)+'01' and substring(b.invoice_date_final,1,8)+substring(date.invoice_date_final_day,9,2)
      ) revenue_mtd
  where
    core.rn = 1
    and revenue_mtd.rn (+) = 1
    and core.cod_base_user = revenue_mtd.cod_base_user (+)
    and core.cod_month = revenue_mtd.cod_month (+)
    and core.opr_invoice_id = revenue_mtd.opr_invoice_id (+)
    and core.cod_source_system_adapted = revenue_mtd.cod_source_system_adapted (+)
  group by
    core.cod_month,
    core.cod_source_system_adapted,
    1,
    core.cod_base_user

   union all

  select
    cast(core.cod_month as numeric) cod_month,
    core.cod_source_system_adapted,
    2 cod_index_type, -- Packages
    core.cod_base_user,
    round(sum(core.listing_revenue) / 1.19,2) val_revenue,
    round(sum(revenue_mtd.listing_revenue) / 1.19,2) val_revenue_mtd,
    count(distinct core.atlas_cod_atlas_user) num_users,
    count(distinct revenue_mtd.atlas_cod_atlas_user) num_users_mtd
  from
    (
      select
        *,
        row_number() over (partition by a.cod_atlas_user,b.cod_month,b.opr_invoice_id order by cod_base_user) rn
      from
        (
          select
            distinct coalesce(base.cod_base_user_owner,-2) cod_base_user,
            atlas.cod_atlas_user,
            atlas.cod_atlas_user atlas_cod_atlas_user,
            atlas.cod_source_system,
            atlas.opr_atlas_user
          from
            (
              select
                a.*
              from
                crm_integration_anlt.t_lkp_contact a,
                crm_integration_anlt.t_lkp_source_system b
              where
                a.cod_source_system = b.cod_source_system
                and a.valid_to = 20991231
                and b.cod_source_system = 18
                --and cod_atlas_user = 141029676
            ) base,
            (
              select
                a.*
              from
                crm_integration_anlt.t_lkp_atlas_user a,
                crm_integration_anlt.t_lkp_source_system b
              where
                a.cod_source_system = b.cod_source_system
                and a.valid_to = 20991231
                and b.cod_source_system = 5
                and a.flg_business = 1
                --and a.opr_atlas_user in (206)
            ) atlas
          where
            atlas.cod_atlas_user = base.cod_atlas_user(+)
        ) a,
        crm_integration_anlt.t_lkp_invoice b,
        crm_integration_anlt.t_lkp_source_system c,
        crm_integration_anlt.v_lkp_source_system_adapted d
      where
        a.opr_atlas_user = b.opr_atlas_id
        and b.cod_source_system = c.cod_source_system
				and b.cod_source_system = 5
				and b.valid_to = 20991231
        and b.is_paid = 1
        and c.opr_source_system = d.dsc_source_system
        and b.listing_revenue != 0
      ) core,
    (
      select
        *,
        row_number() over (partition by a.cod_atlas_user,b.cod_month,b.opr_invoice_id order by cod_base_user) rn
      from
        (
          select
            distinct coalesce(base.cod_base_user_owner,-2) cod_base_user,
            atlas.cod_atlas_user,
            atlas.cod_atlas_user atlas_cod_atlas_user,
            atlas.cod_source_system,
            atlas.opr_atlas_user
          from
            (
              select
                a.*
              from
                crm_integration_anlt.t_lkp_contact a,
                crm_integration_anlt.t_lkp_source_system b
              where
                a.cod_source_system = b.cod_source_system
                and a.valid_to = 20991231
                and b.cod_source_system = 18
                --and cod_atlas_user = 141029676
            ) base,
            (
              select
                a.*
              from
                crm_integration_anlt.t_lkp_atlas_user a,
                crm_integration_anlt.t_lkp_source_system b
              where
                a.cod_source_system = b.cod_source_system
                and a.valid_to = 20991231
                and b.cod_source_system = 5
                and a.flg_business = 1
                --and a.opr_atlas_user in (206)
            ) atlas
          where
            atlas.cod_atlas_user = base.cod_atlas_user(+)
        ) a,
        (select invoice.*, case when cod_month < 201804 then substring(cod_month,1,4)+'-'+substring(cod_month,5,2)+'-'+substring(to_char(invoice_date,'yyyy-mm-dd'),3,2) else to_char(invoice_date,'yyyy-mm-dd') end invoice_date_final from crm_integration_anlt.t_lkp_invoice invoice) b,
        crm_integration_anlt.t_lkp_source_system c,
        crm_integration_anlt.v_lkp_source_system_adapted d,
        (select max(cod_month), max(b.invoice_date_final) invoice_date_final_day from (select invoice.*, case when cod_month < 201804 then substring(cod_month,1,4)+'-'+substring(cod_month,5,2)+'-'+substring(to_char(invoice_date,'yyyy-mm-dd'),3,2) else to_char(invoice_date,'yyyy-mm-dd') end invoice_date_final from crm_integration_anlt.t_lkp_invoice invoice) b
          where b.cod_source_system = 5
		  and b.valid_to = 20991231
            and is_paid = 1
            ) date
      where
        a.opr_atlas_user = b.opr_atlas_id
        and b.cod_source_system = c.cod_source_system
				and b.cod_source_system = 5
				and b.valid_to = 20991231
        and b.is_paid = 1
        and c.opr_source_system = d.dsc_source_system
        and b.listing_revenue != 0
        and b.invoice_date_final between substring(b.invoice_date_final,1,8)+'01' and substring(b.invoice_date_final,1,8)+substring(date.invoice_date_final_day,9,2)
      ) revenue_mtd
  where
    core.rn = 1
    and revenue_mtd.rn (+) = 1
    and core.cod_base_user = revenue_mtd.cod_base_user (+)
    and core.cod_month = revenue_mtd.cod_month (+)
    and core.opr_invoice_id = revenue_mtd.opr_invoice_id (+)
    and core.cod_source_system_adapted = revenue_mtd.cod_source_system_adapted (+)
  group by
    core.cod_month,
    core.cod_source_system_adapted,
    2,
    core.cod_base_user
;


update crm_integration_anlt.t_fac_crm_revenue_month_agg
set
  val_revenue_prev = source.val_revenue_prev,
  val_revenue_hom = source.val_revenue_hom
from
  (
    select
      cod_month,
      cod_source_system_adapted,
      cod_index_type,
      cod_base_user,
      val_revenue,
      lag(val_revenue) over (partition by cod_base_user,cod_source_system_adapted,cod_index_type order by cod_month) val_revenue_prev,
      lag(val_revenue,12) over (partition by cod_base_user,cod_source_system_adapted,cod_index_type order by cod_month) val_revenue_hom
    from
      crm_integration_anlt.t_fac_crm_revenue_month_agg
  ) source
where
  crm_integration_anlt.t_fac_crm_revenue_month_agg.cod_month = source.cod_month
  and crm_integration_anlt.t_fac_crm_revenue_month_agg.cod_base_user = source.cod_base_user
  and crm_integration_anlt.t_fac_crm_revenue_month_agg.cod_source_system_adapted = source.cod_source_system_adapted
  and crm_integration_anlt.t_fac_crm_revenue_month_agg.cod_index_type = source.cod_index_type;
  
  
  

delete from crm_integration_anlt.t_fac_crm_revenue_month_agg where cod_source_system_adapted = 3;

insert into crm_integration_anlt.t_fac_crm_revenue_month_agg (cod_month,cod_source_system_adapted,cod_index_type,cod_base_user,val_revenue, val_revenue_mtd, num_users, num_users_mtd)
select
    cast(core.cod_month as int) cod_month,
    3 cod_source_system_adapted,
    core.cod_index_type,
    coalesce(base.cod_base_user_owner,-2) cod_base_user,
    sum(core.revenue) val_revenue,
    sum(revenue_mtd.revenue_mtd) revenue_mtd,
  count(distinct core.cod_atlas_user) num_users,
  count(distinct revenue_mtd.cod_atlas_user) num_users_mtd

    --val_revenue_prev,
    --val_revenue_hom
from
    (
    select cod_month,opr_atlas_user,dsc_atlas_user,cod_atlas_user,cod_index_type,sum(revenue) revenue
    from
        (
        select
            to_char(pup.date, 'yyyymm') as cod_month,
            users.opr_atlas_user,
            users.dsc_atlas_user,
            users.cod_atlas_user,
            --case when pup.payment_provider = 'account' then pup.price * (-1) end revenue_wallet,
            case when pup.payment_provider not in ('account') then pup.price * (-1) else 0 end revenue,
            case
                when pit.opr_paidad_index_type = 'nnl' then 2
                when pit.opr_paidad_index_type in ('topads', 'ad_homepage', 'bundle', 'pushup', 'logo') then 1
                    else -1
            end cod_index_type
        from
            db_atlas.olxro_paidads_user_payments pup,
            crm_integration_anlt.t_lkp_atlas_user users,
            crm_integration_anlt.t_lkp_paidad_index pi,
            crm_integration_anlt.t_lkp_paidad_index_type pit,
            db_atlas.olxro_ads ad,
            crm_integration_anlt.v_lkp_category c
        where
            trunc(pup.date) >= '2017-01-01'
            and id_user = users.opr_atlas_user
            and users.cod_source_system = 10
            and users.valid_to = 20991231
            and pup.id_index = pi.opr_paidad_index
            and pi.valid_to = 20991231
            and pi.cod_source_system = 10
            and pi.cod_paidad_index_type = pit.cod_paidad_index_type
            and pit.cod_source_system = 10
            and pit.valid_to = 20991231
            and pup.payment_provider not in ('admin')
            and pup.id_ad = ad.id
            and ad.category_id = c.id1
            and c.livesync_dbname = 'olxro'
            and c.id0 = 4
            --and users.cod_atlas_user = 126065831
        ) x
    group by cod_month,opr_atlas_user,dsc_atlas_user,cod_atlas_user,cod_index_type
    ) core,
    (
        select
          a.*
        from
          crm_integration_anlt.t_lkp_contact a,
          crm_integration_anlt.t_lkp_source_system b
        where
          a.cod_source_system = b.cod_source_system
          and a.valid_to = 20991231
          and b.cod_source_system = 20
          --and cod_atlas_user = 141029676
    ) base,
    (
    select cod_month,opr_atlas_user,dsc_atlas_user,cod_atlas_user,cod_index_type,sum(revenue_mtd) revenue_mtd
    from
        (
        select
            to_char(pup.date, 'yyyymm') as cod_month,
            users.opr_atlas_user,
            users.dsc_atlas_user,
            users.cod_atlas_user,
            --case when pup.payment_provider = 'account' then pup.price * (-1) end revenue_wallet,
            case when pup.payment_provider not in ('account') then pup.price * (-1) else 0 end revenue_mtd,
            case
                when pit.opr_paidad_index_type = 'nnl' then 2
                when pit.opr_paidad_index_type in ('topads', 'ad_homepage', 'bundle', 'pushup', 'logo') then 1
                    else -1
            end cod_index_type
        from
            db_atlas.olxro_paidads_user_payments pup,
            crm_integration_anlt.t_lkp_atlas_user users,
            crm_integration_anlt.t_lkp_paidad_index pi,
            crm_integration_anlt.t_lkp_paidad_index_type pit,
            db_atlas.olxro_ads ad,
            crm_integration_anlt.v_lkp_category c,
            (select max(trunc(date)) invoice_date_final_day from db_atlas.olxro_paidads_user_payments) date
        where
            trunc(pup.date) between substring(trunc(pup.date),1,8)+'01' and substring(trunc(pup.date),1,8)+substring(date.invoice_date_final_day,9,2)
            and trunc(pup.date) >= '2017-01-01'
            and id_user = users.opr_atlas_user
            and users.cod_source_system = 10
            and users.valid_to = 20991231
            and pup.id_index = pi.opr_paidad_index
            and pi.valid_to = 20991231
            and pi.cod_source_system = 10
            and pi.cod_paidad_index_type = pit.cod_paidad_index_type
            and pit.cod_source_system = 10
            and pit.valid_to = 20991231
            and pup.payment_provider not in ('admin')
            and pup.id_ad = ad.id
            and ad.category_id = c.id1
            and c.livesync_dbname = 'olxro'
            and c.id0 = 4
            --and users.cod_atlas_user = 126065831
        ) x
    group by cod_month,opr_atlas_user,dsc_atlas_user,cod_atlas_user,cod_index_type
    ) revenue_mtd
where
    core.cod_atlas_user = base.cod_atlas_user(+)
    and core.cod_atlas_user = revenue_mtd.cod_atlas_user (+)
    and core.cod_month = revenue_mtd.cod_month (+)
    and core.cod_index_type = revenue_mtd.cod_index_type (+)
group by
    cast(core.cod_month as int),
    3,
    core.cod_index_type,
    coalesce(base.cod_base_user_owner,-2);
	
update crm_integration_anlt.t_fac_crm_revenue_month_agg
set
  val_revenue_prev = source.val_revenue_prev, val_revenue_hom = source.val_revenue_hom
from
  (
    select
      cod_month,
      cod_source_system_adapted,
      cod_index_type,
      cod_base_user,
      val_revenue,
      lag(val_revenue) over (partition by cod_base_user,cod_source_system_adapted,cod_index_type order by cod_month) val_revenue_prev,
      lag(val_revenue,12) over (partition by cod_base_user,cod_source_system_adapted,cod_index_type order by cod_month) val_revenue_hom
    from
      crm_integration_anlt.t_fac_crm_revenue_month_agg
    where
      cod_source_system_adapted = 3
  ) source
where
  crm_integration_anlt.t_fac_crm_revenue_month_agg.cod_month = source.cod_month
  and crm_integration_anlt.t_fac_crm_revenue_month_agg.cod_base_user = source.cod_base_user
  and crm_integration_anlt.t_fac_crm_revenue_month_agg.cod_source_system_adapted = source.cod_source_system_adapted
  and crm_integration_anlt.t_fac_crm_revenue_month_agg.cod_index_type = source.cod_index_type
  and crm_integration_anlt.t_fac_crm_revenue_month_agg.cod_source_system_adapted = 3;  
  
  
  
delete from crm_integration_anlt.t_fac_crm_users_month_agg;

-- ROMANIA - PPU, APPU and NRPPU (Storia, Autovit)
insert into crm_integration_anlt.t_fac_crm_users_month_agg (cod_month,cod_source_system_adapted,cod_base_user,val_ppu,val_ppu_mtd, val_appu,val_nrppu, val_nrppu_mtd)
select
  cod_month,
  cod_source_system_adapted,
  cod_base_user,
  sum(val_ppu) val_ppu,
  sum(val_ppu_mtd) val_ppu_mtd,
  null val_appu,--sum(val_appu) val_appu,
  sum(val_nrppu) val_nrppu,
  sum(val_nrppu_mtd) val_nrppu_mtd
from
  (
    select
      cod_month,
      cod_source_system_adapted,
      cod_base_user,
      cod_atlas_user,
      val_ppu,
      val_ppu_mtd,
      /*case
        when max_duration > datediff(days,min_dat_payment,dat_last_day) then 1
          else 0
      end val_appu,*/
      case
        when prev_max_payment is null then 0
        when datediff(days,prev_max_payment,min_dat_payment) >= 30 then 1
          else 0
      end val_nrppu,
       case
        when prev_max_payment_mtd is null then 0
        when datediff(days,prev_max_payment_mtd,min_dat_payment_mtd) >= 30 then 1
          else 0
      end val_nrppu_mtd
    from
      (
        select
          cod_base_user,
          cod_atlas_user,
          cod_month,
          cod_source_system_adapted,
          dat_last_day,
          --max_duration,
          val_ppu,
          val_ppu_mtd,
          min_dat_payment,
          max_dat_payment,
          min_dat_payment_mtd,
          max_dat_payment_mtd,
          lag(max_dat_payment) over (partition by cod_atlas_user order by cod_month) prev_max_payment,
          lead(min_dat_payment) over (partition by cod_atlas_user order by cod_month) next_min_payment,
          lag(max_dat_payment_mtd) over (partition by cod_atlas_user order by cod_month) prev_max_payment_mtd,
          lead(min_dat_payment_mtd) over (partition by cod_atlas_user order by cod_month) next_min_payment_mtd
        from
          (
            select
              a.cod_base_user,
              a.cod_atlas_user,
              f.cod_month,
              2 cod_source_system_adapted,
              f.dat_last_day,
              b.opr_invoice_id invoice,
              row_number() over (partition by a.cod_atlas_user,f.cod_month order by b.opr_invoice_id desc, a.cod_contact desc) rn,
              --max(g.duration) max_duration,
              coalesce(min(b.invoice_date),'1900-01-01') min_dat_payment,
              coalesce(max(b.invoice_date),'1900-01-01') max_dat_payment,
              case when max(b.invoice_date) between substring(max(b.invoice_date),1,8)+'01' and substring(max(b.invoice_date),1,8)+substring(max(date.invoice_date_final_day),9,2) then coalesce(min(b.invoice_date),'1900-01-01') else null end min_dat_payment_mtd,
              case when max(b.invoice_date) between substring(max(b.invoice_date),1,8)+'01' and substring(max(b.invoice_date),1,8)+substring(max(date.invoice_date_final_day),9,2) then coalesce(max(b.invoice_date),'1900-01-01') else null end max_dat_payment_mtd,
              count(distinct b.user_id) val_ppu,
              case when max(b.invoice_date) between substring(max(b.invoice_date),1,8)+'01' and substring(max(b.invoice_date),1,8)+substring(max(date.invoice_date_final_day),9,2) then count(distinct b.user_id) else null end val_ppu_mtd
            from
              (
                select
                  coalesce(base.cod_base_user_owner,-2) cod_base_user,
				  base.cod_contact,
                  atlas.cod_atlas_user,
                  atlas.opr_atlas_user,
                  atlas.cod_source_system
                from
                  (
                    select
                      a.*
                    from
                      crm_integration_anlt.t_lkp_contact a,
                      crm_integration_anlt.t_lkp_source_system b
                    where
                      a.cod_source_system = b.cod_source_system
                      and a.valid_to = 20991231
                      and b.cod_source_system = 19
                  ) base,
                  (
                    select
                      a.*
                    from
                      (
                        select
                          *
                        from
                          (
                            SELECT
                              a.*,
                              row_number()
                              OVER (
                                PARTITION BY opr_atlas_user, cod_source_system
                                ORDER BY valid_to DESC ) rn
                            FROM
                              crm_integration_anlt.t_lkp_atlas_user a
                          )
                        where rn = 1
                      ) a,
                      crm_integration_anlt.t_lkp_source_system b
                    where
                      a.cod_source_system = b.cod_source_system
                      and b.cod_source_system = 1
                      and a.flg_business = 1
                      --and a.opr_atlas_user in (6478,258828)
                  ) atlas
                where
                  atlas.cod_atlas_user = base.cod_atlas_user(+)
              ) a,
              (select opr_atlas_id user_id, opr_invoice_id, is_paid, cod_month, to_date(case when cod_month < 201804 then substring(cod_month,1,4)+'-'+substring(cod_month,5,2)+'-'+substring(to_char(invoice_date,'yyyy-mm-dd'),3,2) else to_char(invoice_date,'yyyy-mm-dd') end,'yyyy-mm-dd') invoice_date from crm_integration_anlt.t_lkp_invoice where cod_source_system = 1 and valid_to = 20991231) b,
              crm_integration_anlt.t_lkp_month f,
            (select max(cod_month), max(b.invoice_date_final) invoice_date_final_day from (select invoice.*, case when cod_month < 201804 then substring(cod_month,1,4)+'-'+substring(cod_month,5,2)+'-'+substring(to_char(invoice_date,'yyyy-mm-dd'),3,2) else to_char(invoice_date,'yyyy-mm-dd') end invoice_date_final from crm_integration_anlt.t_lkp_invoice invoice) b
        where cod_source_system = 1
		and b.valid_to = 20991231
          and is_paid = 1
            ) date
            where
              b.user_id = a.opr_atlas_user
              and b.cod_month >= 201701
              and f.cod_month = b.cod_month
			  and b.is_paid = 1
            group by
              a.cod_base_user,
              a.cod_atlas_user,
              f.cod_month,
              f.dat_last_day,
              b.opr_invoice_id,
			  a.cod_contact
          ) inner_core
        where
          rn = 1
      ) inner_core
  ) core
group by
  cod_month,
  cod_source_system_adapted,
  cod_base_user--)
--where cod_month = 201803
--group by cod_source_system_adapted
;  



insert into crm_integration_anlt.t_fac_crm_users_month_agg (cod_month,cod_source_system_adapted,cod_base_user,val_ppu,val_ppu_mtd, val_appu,val_nrppu, val_nrppu_mtd)
select
  cod_month,
  cod_source_system_adapted,
  cod_base_user,
  sum(val_ppu) val_ppu,
  sum(val_ppu_mtd) val_ppu_mtd,
  null val_appu,--sum(val_appu) val_appu,
  sum(val_nrppu) val_nrppu,
  sum(val_nrppu_mtd) val_nrppu_mtd
from
  (
    select
      cod_month,
      cod_source_system_adapted,
      cod_base_user,
      cod_atlas_user,
      val_ppu,
      val_ppu_mtd,
      /*case
        when max_duration > datediff(days,min_dat_payment,dat_last_day) then 1
          else 0
      end val_appu,*/
      case
        when prev_max_payment is null then 0
        when datediff(days,prev_max_payment,min_dat_payment) >= 30 then 1
          else 0
      end val_nrppu,
       case
        when prev_max_payment_mtd is null then 0
        when datediff(days,prev_max_payment_mtd,min_dat_payment_mtd) >= 30 then 1
          else 0
      end val_nrppu_mtd
    from
      (
        select
          cod_base_user,
          cod_atlas_user,
          cod_month,
          cod_source_system_adapted,
          dat_last_day,
          --max_duration,
          val_ppu,
          val_ppu_mtd,
          min_dat_payment,
          max_dat_payment,
          min_dat_payment_mtd,
          max_dat_payment_mtd,
          lag(max_dat_payment) over (partition by cod_atlas_user order by cod_month) prev_max_payment,
          lead(min_dat_payment) over (partition by cod_atlas_user order by cod_month) next_min_payment,
          lag(max_dat_payment_mtd) over (partition by cod_atlas_user order by cod_month) prev_max_payment_mtd,
          lead(min_dat_payment_mtd) over (partition by cod_atlas_user order by cod_month) next_min_payment_mtd
        from
          (
            select
              a.cod_base_user,
              a.cod_atlas_user,
              f.cod_month,
              1 cod_source_system_adapted,
              f.dat_last_day,
              b.opr_invoice_id invoice,
              row_number() over (partition by a.cod_atlas_user,f.cod_month order by b.opr_invoice_id desc, a.cod_contact desc) rn,
              --max(g.duration) max_duration,
              coalesce(min(b.invoice_date),'1900-01-01') min_dat_payment,
              coalesce(max(b.invoice_date),'1900-01-01') max_dat_payment,
              case when max(b.invoice_date) between substring(max(b.invoice_date),1,8)+'01' and substring(max(b.invoice_date),1,8)+substring(max(date.invoice_date_final_day),9,2) then coalesce(min(b.invoice_date),'1900-01-01') else null end min_dat_payment_mtd,
              case when max(b.invoice_date) between substring(max(b.invoice_date),1,8)+'01' and substring(max(b.invoice_date),1,8)+substring(max(date.invoice_date_final_day),9,2) then coalesce(max(b.invoice_date),'1900-01-01') else null end max_dat_payment_mtd,
              count(distinct b.user_id) val_ppu,
              case when max(b.invoice_date) between substring(max(b.invoice_date),1,8)+'01' and substring(max(b.invoice_date),1,8)+substring(max(date.invoice_date_final_day),9,2) then count(distinct b.user_id) else null end val_ppu_mtd
            from
              (
                select
                  coalesce(base.cod_base_user_owner,-2) cod_base_user,
				  base.cod_contact,
                  atlas.cod_atlas_user,
                  atlas.opr_atlas_user,
                  atlas.cod_source_system
                from
                  (
                    select
                      a.*
                    from
                      crm_integration_anlt.t_lkp_contact a,
                      crm_integration_anlt.t_lkp_source_system b
                    where
                      a.cod_source_system = b.cod_source_system
                      and a.valid_to = 20991231
                      and b.cod_source_system = 18
                  ) base,
                  (
                    select
                      a.*
                    from
                      (
                        select
                          *
                        from
                          (
                            SELECT
                              a.*,
                              row_number()
                              OVER (
                                PARTITION BY opr_atlas_user, cod_source_system
                                ORDER BY valid_to DESC ) rn
                            FROM
                              crm_integration_anlt.t_lkp_atlas_user a
                            where
                              a.opr_atlas_user not in (521225, 521223, 521213, 521215, 522479, 522495, 24280, 419691, 522665, 18964, 1030125, 522513,
                              1057085, 1058565, 1066171, 1285659, 1285679, 829479, 17628)
                              and not (a.dsc_atlas_user like '%@autovit%' or a.dsc_atlas_user like '%@sunfra.%' or a.dsc_atlas_user like '%@olx.%' or a.dsc_atlas_user like '%@tablica.%'
                              or a.dsc_atlas_user like '%@fixeads.%' or a.dsc_atlas_user like'%@otomoto.%' or a.dsc_atlas_user like '%@otodom.%' or a.dsc_atlas_user like '%@slando.%')
                          )
                        where rn = 1
                      ) a,
                      crm_integration_anlt.t_lkp_source_system b
                    where
                      a.cod_source_system = b.cod_source_system
                      and b.cod_source_system = 5
                      and a.flg_business = 1
                      --and a.opr_atlas_user in (6478,258828)
                  ) atlas
                where
                  atlas.cod_atlas_user = base.cod_atlas_user(+)
              ) a,
              (select opr_atlas_id user_id , opr_invoice_id, is_paid, cod_month, to_date(case when cod_month < 201804 then substring(cod_month,1,4)+'-'+substring(cod_month,5,2)+'-'+substring(to_char(invoice_date,'yyyy-mm-dd'),3,2) else to_char(invoice_date,'yyyy-mm-dd') end,'yyyy-mm-dd') invoice_date from crm_integration_anlt.t_lkp_invoice where cod_source_system = 5 and valid_to = 20991231) b,
              crm_integration_anlt.t_lkp_month f,
            (select max(cod_month), max(b.invoice_date_final) invoice_date_final_day from (select invoice.*, case when cod_month < 201804 then substring(cod_month,1,4)+'-'+substring(cod_month,5,2)+'-'+substring(to_char(invoice_date,'yyyy-mm-dd'),3,2) else to_char(invoice_date,'yyyy-mm-dd') end invoice_date_final from crm_integration_anlt.t_lkp_invoice invoice) b
        where cod_source_system = 5
		and b.valid_to = 20991231
          and is_paid = 1
            ) date
            where
              b.user_id = a.opr_atlas_user
              and b.cod_month >= 201701
              and f.cod_month = b.cod_month
			  and b.is_paid = 1
            group by
              a.cod_base_user,
              a.cod_atlas_user,
              f.cod_month,
              f.dat_last_day,
              b.opr_invoice_id,
			  a.cod_contact
          ) inner_core
        where
          rn = 1
      ) inner_core
  ) core
group by
  cod_month,
  cod_source_system_adapted,
  cod_base_user--)
--where cod_month = 201803
--group by cod_source_system_adapted
;


-- OLX
insert into crm_integration_anlt.t_fac_crm_users_month_agg (cod_month,cod_source_system_adapted,cod_base_user,val_ppu,val_ppu_mtd, val_appu,val_nrppu, val_nrppu_mtd)
select
  cod_month,
  cod_source_system_adapted,
  cod_base_user,
  sum(val_ppu) val_ppu,
  sum(val_ppu_mtd) val_ppu_mtd,
  null val_appu,--sum(val_appu) val_appu,
  sum(val_nrppu) val_nrppu,
  sum(val_nrppu_mtd) val_nrppu_mtd
from
  (
    select
      cod_month,
      cod_source_system_adapted,
      cod_base_user,
      cod_atlas_user,
      val_ppu,
      val_ppu_mtd,
      /*case
        when max_duration > datediff(days,min_dat_payment,dat_last_day) then 1
          else 0
      end val_appu,*/
      case
        when prev_max_payment is null then 0
        when datediff(days,prev_max_payment,min_dat_payment) >= 30 then 1
          else 0
      end val_nrppu,
       case
        when prev_max_payment_mtd is null then 0
        when datediff(days,prev_max_payment_mtd,min_dat_payment_mtd) >= 30 then 1
          else 0
      end val_nrppu_mtd
    from
      (
        select
          cod_base_user,
          cod_atlas_user,
          cod_month,
          cod_source_system_adapted,
          val_ppu,
          val_ppu_mtd,
          min_dat_payment,
          max_dat_payment,
          min_dat_payment_mtd,
          max_dat_payment_mtd,
          lag(max_dat_payment) over (partition by cod_atlas_user order by cod_month) prev_max_payment,
          lead(min_dat_payment) over (partition by cod_atlas_user order by cod_month) next_min_payment,
          lag(max_dat_payment_mtd) over (partition by cod_atlas_user order by cod_month) prev_max_payment_mtd,
          lead(min_dat_payment_mtd) over (partition by cod_atlas_user order by cod_month) next_min_payment_mtd
        from
          (
          SELECT
            cast(core.cod_month AS INT)            cod_month,
            3                                      cod_source_system_adapted,
            core.cod_atlas_user,
            coalesce(base.cod_base_user_owner, -2) cod_base_user,
            count(distinct core.cod_atlas_user)         val_ppu,
            case when trunc(max(core.date)) between substring(trunc(max(core.date)),1,8)+'01' and substring(trunc(max(core.date)),1,8)+substring(max(date.invoice_date_final_day),9,2) then count(distinct core.cod_atlas_user) else null end val_ppu_mtd,
            coalesce(trunc(min(core.date)),'1900-01-01') min_dat_payment,
            coalesce(trunc(max(core.date)),'1900-01-01') max_dat_payment,
            case when trunc(max(core.date)) between substring(trunc(max(core.date)),1,8)+'01' and substring(trunc(max(core.date)),1,8)+substring(max(date.invoice_date_final_day),9,2) then coalesce(trunc(min(core.date)),'1900-01-01') else null end min_dat_payment_mtd,
            case when trunc(max(core.date)) between substring(trunc(max(core.date)),1,8)+'01' and substring(trunc(max(core.date)),1,8)+substring(max(date.invoice_date_final_day),9,2) then coalesce(trunc(max(core.date)),'1900-01-01') else null end max_dat_payment_mtd
          FROM
            (
              SELECT
                to_char(core.date, 'yyyymm') AS cod_month,
                core.date,
                users.cod_atlas_user
              FROM
                db_atlas.olxro_paidads_user_payments core,
                crm_integration_anlt.t_lkp_atlas_user users,
                db_atlas.olxro_ads ad,
                crm_integration_anlt.v_lkp_category c
              WHERE
                trunc(core.date) >= '2017-01-01'
                AND id_user = users.opr_atlas_user
                AND users.cod_source_system = 10
                AND users.valid_to = 20991231
                AND core.payment_provider NOT IN ('admin')
                AND core.id_ad = ad.id
                AND ad.category_id = c.id1
                AND c.livesync_dbname = 'olxro'
                AND c.id0 = 4
            ) core,
            (
              SELECT a.*
              FROM
                crm_integration_anlt.t_lkp_contact a,
                crm_integration_anlt.t_lkp_source_system b
              WHERE
                a.cod_source_system = b.cod_source_system
                AND a.valid_to = 20991231
                AND b.cod_source_system = 20
              --and cod_atlas_user = 141029676
            ) base,
      (select max(trunc(date)) invoice_date_final_day from db_atlas.olxro_paidads_user_payments) date
          WHERE
            core.cod_atlas_user = base.cod_atlas_user (+)
          GROUP BY
            cast(core.cod_month AS INT),
            3,
            coalesce(base.cod_base_user_owner, -2)
            )
        ) inner_core
  ) core
group by
  cod_month,
  cod_source_system_adapted,
  cod_base_user;
  
  
  
  

update crm_integration_anlt.t_fac_crm_users_month_agg
set
  val_ppu_prev = source.val_ppu_prev, val_ppu_hom = source.val_ppu_hom,
  val_nrppu_prev = source.val_nrppu_prev, val_nrppu_hom = source.val_nrppu_hom,
  val_appu_prev = source.val_appu_prev, val_appu_hom = source.val_appu_hom
from
  (
    select
      cod_month,
      cod_source_system_adapted,
      cod_base_user,
      val_ppu,
      lag(val_ppu) over (partition by cod_base_user,cod_source_system_adapted order by cod_month) val_ppu_prev,
      lag(val_ppu,12) over (partition by cod_base_user,cod_source_system_adapted order by cod_month) val_ppu_hom,
      val_appu,
      lag(val_appu) over (partition by cod_base_user,cod_source_system_adapted order by cod_month) val_appu_prev,
      lag(val_appu,12) over (partition by cod_base_user,cod_source_system_adapted order by cod_month) val_appu_hom,
      val_nrppu,
      lag(val_nrppu) over (partition by cod_base_user,cod_source_system_adapted order by cod_month) val_nrppu_prev,
      lag(val_nrppu,12) over (partition by cod_base_user,cod_source_system_adapted order by cod_month) val_nrppu_hom
    from
      crm_integration_anlt.t_fac_crm_users_month_agg
  ) source
where
  crm_integration_anlt.t_fac_crm_users_month_agg.cod_month = source.cod_month
  and crm_integration_anlt.t_fac_crm_users_month_agg.cod_base_user = source.cod_base_user
  and crm_integration_anlt.t_fac_crm_users_month_agg.cod_source_system_adapted = source.cod_source_system_adapted;  
  
  
  



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
    (select coalesce(max(cod_execution),0) max_cod_exec from crm_integration_anlt.t_fac_scai_execution),
    crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
    crm_integration_anlt.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 4 -- Romania
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_lkp_invoice';

--$$$

-- #######################
-- ####    PASSO 6    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set cod_status = 1, -- Ok
last_processing_datetime = coalesce((select max(operation_timestamp) from tmp_ro_load_paidad_index),last_processing_datetime)
/*from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc, crm_integration_anlt.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_paidad_index'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 4
  ) source*/
from crm_integration_anlt.t_lkp_scai_process proc 
where t_rel_scai_integration_process.cod_process = proc.cod_process
and t_rel_scai_integration_process.cod_status = 2
and t_rel_scai_integration_process.cod_country = 4
and proc.dsc_process_short = 't_lkp_invoice'
and t_rel_scai_integration_process.ind_active = 1
/*crm_integration_anlt.t_rel_scai_integration_process.cod_process = source.cod_process
and crm_integration_anlt.t_rel_scai_integration_process.cod_country = source.cod_country
and crm_integration_anlt.t_rel_scai_integration_process.cod_integration = source.cod_integration*/;


--$$$
	
-- #######################
-- ####    PASSO 3    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set dat_processing = source.dat_processing, execution_nbr = source.execution_nbr, cod_status = 2 -- Running
from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc, crm_integration_anlt.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_atlas_user'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 4
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
    (select coalesce(max(cod_execution),0) max_cod_exec from crm_integration_anlt.t_fac_scai_execution),
    crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
    crm_integration_anlt.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 4 -- Romania
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_lkp_atlas_user';

--$$$
	
-- #############################################
-- # 		      ATLAS - ROMANIA              #
-- #	      LOADING t_lkp_atlas_user     	   #
-- #############################################


create temp table tmp_ro_load_atlas_user 
distkey(cod_source_system)
sortkey(cod_atlas_user, opr_atlas_user)
as
select a.*  from (
	select
        source_table.opr_atlas_user,
	source_table.dsc_atlas_user,
	source_table.email_original,
	source_table.operation_type,
	source_table.operation_timestamp,
	source_table.type,
	source_table.created_at,
	source_table.last_login_at,
	source_table.default_lang, 
	source_table.ban_reason_id, 
	source_table.credits, 
	coalesce(source_table.opr_source,'Unknown') opr_source, 
	source_table.flg_external_login,
	source_table.flg_business, 
	source_table.suspend_reason, 
	source_table.last_modification_date,
	source_table.flg_autorenew, 
	source_table.bonus_credits,
	source_table.bonus_credits_expire_at, 
	source_table.flg_uses_crm, 
	source_table.opr_city,
    source_table.cod_source_system,
    source_table.hash_atlas_user,
	source_table.cod_execution,
    max_cod_atlas_user.max_cod,
    row_number() over (order by source_table.opr_atlas_user desc) new_cod,
    target.cod_atlas_user,
	target.valid_from,
    case
      --when target.cod_atlas_user is null then 'I'
	  when target.cod_atlas_user is null or (source_table.hash_atlas_user != target.hash_atlas_user and target.valid_from = (select dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_atlas_user')) then 'I'
	  when source_table.operation_type = 'delete' then 'D'
      when source_table.hash_atlas_user != target.hash_atlas_user then 'U'
        else 'X'
    end dml_type
  from
    (
	SELECT
		source.*,
		lkp_source_system.cod_source_system,
		md5
		(
			coalesce(dsc_atlas_user                                      ,'') +
			coalesce(email_original                                      ,'') +
			coalesce(type                                                ,'') +
			coalesce(created_at                                          ,'2099-12-31 00:00:00.000000') +
			coalesce(last_login_at                                       ,'2099-12-31 00:00:00.000000') +
			coalesce(default_lang                                        ,'') + 
			coalesce(ban_reason_id                                       ,0) +
			coalesce(opr_city                                            ,0) +
			cast(coalesce(credits                                        ,0) as varchar) + 
			coalesce(opr_source                                          ,'') + 
			coalesce(flg_external_login                                  ,0) +
			coalesce(flg_business                                     ,0) + 
			coalesce(suspend_reason                                      ,'') + 
			--coalesce(last_modification_date                              ,'2099-12-31 00:00:00.000000') +
			coalesce(flg_autorenew                                       ,0) + 
			cast(coalesce(bonus_credits                                  ,0) as varchar) +
			--coalesce(bonus_credits_expire_at                             ,'2099-12-31 00:00:00.000000') + 
			coalesce(flg_uses_crm                                        ,0)
	    ) hash_atlas_user
	  FROM
	  (
      SELECT
		id opr_atlas_user,
        livesync_dbname opr_source_system,
		operation_type,
		operation_timestamp,
		email dsc_atlas_user,
		email_original, 
		type,
		created_at,
		last_login_at,
		default_lang, 
		ban_reason_id, 
		default_city_id opr_city, 
		credits, 
		source opr_source, 
		external_login flg_external_login,
		is_business flg_business, 
		suspend_reason, 
		last_modification_date,
		autorenew flg_autorenew, 
		cast(null as numeric(10,2)) bonus_credits,
		cast(null as timestamp) bonus_credits_expire_at, 
		cast(null as bigint) flg_uses_crm, 
		scai_execution.cod_execution
      FROM
        crm_integration_stg.stg_ro_db_atlas_verticals_users a,
    		crm_integration_anlt.t_lkp_source_system b,
       (
          select
            max(fac.cod_execution) cod_execution
          from
            crm_integration_anlt.t_lkp_scai_process proc,
            crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
            crm_integration_anlt.t_fac_scai_execution fac
          where
            rel_integr_proc.cod_process = proc.cod_process
            and rel_integr_proc.cod_country = 4
            and rel_integr_proc.cod_country = fac.cod_country
            and rel_integr_proc.cod_integration = 30000
            and rel_integr_proc.ind_active = 1
            and proc.dsc_process_short = 't_lkp_atlas_user'
            and fac.cod_process = rel_integr_proc.cod_process
            and fac.cod_integration = rel_integr_proc.cod_integration
            and rel_integr_proc.dat_processing = fac.dat_processing
            and fac.cod_status = 2
        ) scai_execution
	  where
      a.livesync_dbname = b.opr_source_system
      and b.cod_business_type = 1 -- Verticals
      and b.cod_country = 4 -- Romania
		--and 1 = 0
	  union all
	  SELECT
		id opr_atlas_user,
        'olxro' opr_source_system,
		operation_type,
		operation_timestamp,
		email dsc_atlas_user,
		email_original, 
		type,
		created_at,
		last_login_at,
		null default_lang, 
		ban_reason_id, 
		default_city_id opr_city, 
		credits, 
		source opr_source, 
		external_login flg_external_login,
		is_business flg_business, 
		null suspend_reason, 
		null last_modification_date,
		null flg_autorenew, 
		bonus_credits,
		bonus_credits_expire_at, 
		uses_crm flg_uses_crm, 
		scai_execution.cod_execution
	  FROM
		crm_integration_stg.stg_ro_db_atlas_olxro_users,
     (
      select
        max(fac.cod_execution) cod_execution
      from
        crm_integration_anlt.t_lkp_scai_process proc,
        crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
        crm_integration_anlt.t_fac_scai_execution fac
      where
        rel_integr_proc.cod_process = proc.cod_process
        and rel_integr_proc.cod_country = 4
        and rel_integr_proc.cod_integration = 30000
        and rel_integr_proc.ind_active = 1
        and proc.dsc_process_short = 't_lkp_atlas_user'
        and fac.cod_process = rel_integr_proc.cod_process
        and fac.cod_integration = rel_integr_proc.cod_integration
        and rel_integr_proc.dat_processing = fac.dat_processing
        and fac.cod_status = 2
    ) scai_execution
	  --where 1 = 0
	) source,
    crm_integration_anlt.t_lkp_source_system lkp_source_system
	where source.opr_source_system = lkp_source_system.opr_source_system
    ) source_table, 
    (select coalesce(max(cod_atlas_user),0) max_cod from crm_integration_anlt.t_lkp_atlas_user) max_cod_atlas_user,
    (
			select
				*
			from
				(
					SELECT
						a.*,
						row_number()
						OVER (
							PARTITION BY opr_atlas_user, cod_source_system
							ORDER BY valid_to DESC ) rn
					FROM
						crm_integration_anlt.t_lkp_atlas_user a
				)
			where rn = 1
	) target
  where
    coalesce(source_table.opr_atlas_user,-1) = target.opr_atlas_user(+)
	and source_table.cod_source_system = target.cod_source_system (+) 
	) a ;

analyze tmp_ro_load_atlas_user;	


	
delete from crm_integration_anlt.t_lkp_atlas_user
using tmp_ro_load_atlas_user
where 
	tmp_ro_load_atlas_user.dml_type = 'I' 
	and t_lkp_atlas_user.opr_atlas_user = tmp_ro_load_atlas_user.opr_atlas_user
	and t_lkp_atlas_user.valid_from = (select dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_atlas_user');
	

	
update crm_integration_anlt.t_lkp_atlas_user
set valid_to = (select rel_integr_proc.dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_atlas_user') 
from tmp_ro_load_atlas_user source
where source.cod_atlas_user = crm_integration_anlt.t_lkp_atlas_user.cod_atlas_user
and crm_integration_anlt.t_lkp_atlas_user.valid_to = 20991231
and source.dml_type in('U','D');

	
insert into crm_integration_anlt.t_lkp_atlas_user
	 select
       case
        when dml_type = 'I' then case when valid_from = (select dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc
														where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_atlas_user')
									then cod_atlas_user else max_cod + new_cod end
        when dml_type = 'U' then cod_atlas_user
      end cod_atlas_user,
	  opr_atlas_user,
	  dsc_atlas_user,
	  (select rel_integr_proc.dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_atlas_user') valid_from, 
      20991231 valid_to,
	  cod_source_system,
	  opr_source,
	  opr_city,
	  email_original, 
	  type,
	  created_at,
	  last_login_at,
	  default_lang, 
	  ban_reason_id, 
	  credits, 
	  flg_external_login,
	  flg_business, 
	  suspend_reason, 
	  last_modification_date,
	  flg_autorenew, 
	  cast(bonus_credits as numeric(10,2)) bonus_credits,
	  cast(bonus_credits_expire_at as timestamp) bonus_credits_expire_at, 
	  cast(flg_uses_crm as bigint) flg_uses_crm, 
      hash_atlas_user,
	  cod_execution
    from
      tmp_ro_load_atlas_user
    where
      dml_type in ('U','I');



analyze crm_integration_anlt.t_lkp_atlas_user;
	  
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
    (select coalesce(max(cod_execution),0) max_cod_exec from crm_integration_anlt.t_fac_scai_execution),
    crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
    crm_integration_anlt.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 4 -- Romania
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_lkp_atlas_user';

--$$$

-- #######################
-- ####    PASSO 6    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set cod_status = 1, -- Ok
last_processing_datetime = coalesce((select max(operation_timestamp) from tmp_ro_load_atlas_user),last_processing_datetime)
/*from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc, crm_integration_anlt.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_atlas_user'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 4
  ) source*/
from crm_integration_anlt.t_lkp_scai_process proc 
where t_rel_scai_integration_process.cod_process = proc.cod_process
and t_rel_scai_integration_process.cod_status = 2
and t_rel_scai_integration_process.cod_country = 4
and proc.dsc_process_short = 't_lkp_atlas_user'
and t_rel_scai_integration_process.ind_active = 1
/*crm_integration_anlt.t_rel_scai_integration_process.cod_process = source.cod_process
and crm_integration_anlt.t_rel_scai_integration_process.cod_country = source.cod_country
and crm_integration_anlt.t_rel_scai_integration_process.cod_integration = source.cod_integration*/;

--$$$

-- #######################
-- ####    PASSO 3    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set dat_processing = source.dat_processing, execution_nbr = source.execution_nbr, cod_status = 2 -- Running
from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc, crm_integration_anlt.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_contact_upd_atlas_user'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 4
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
    (select coalesce(max(cod_execution),0) max_cod_exec from crm_integration_anlt.t_fac_scai_execution),
    crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
    crm_integration_anlt.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 4 -- Romania
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
  and rel_country_integr.ind_active = 1
  and rel_integr_proc.ind_active = 1
  and proc.dsc_process_short = 't_lkp_contact_upd_atlas_user';

--$$$

-- ##########################################################
-- #        ATLAS / BASE - ROMANIA                          #
-- #        LOADING t_lkp_contact - Update COD_ATLAS_USER   #
-- ##########################################################

-- Updating BASE CONTACT - OLX
update crm_integration_anlt.t_lkp_contact
set cod_atlas_user = source.cod_atlas_user
from
  (
    select
      scai_valid_from.dat_processing valid_from,
      base_contact.cod_contact,
      base_contact.cod_source_system,
      atlas_user.cod_atlas_user
    from
      (
        select
          rel_integr_proc.dat_processing
        from
          crm_integration_anlt.t_lkp_scai_process proc,
          crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc
        where
          rel_integr_proc.cod_process = proc.cod_process
          and rel_integr_proc.cod_country = 4
          and rel_integr_proc.cod_integration = 30000
          and rel_integr_proc.ind_active = 1
          and proc.dsc_process_short = 't_lkp_contact'
      ) scai_valid_from,
      crm_integration_anlt.t_lkp_atlas_user atlas_user,
      crm_integration_anlt.t_lkp_contact base_contact
    where
      atlas_user.cod_source_system = 10
      and atlas_user.valid_to = 20991231
      and lower(base_contact.email) = lower(atlas_user.dsc_atlas_user)
	  and trim(base_contact.email) != ''
      and base_contact.cod_source_system = 20
      and base_contact.valid_from = scai_valid_from.dat_processing
  ) source
where
  t_lkp_contact.cod_contact = source.cod_contact
  and t_lkp_contact.valid_from = source.valid_from
  and t_lkp_contact.cod_source_system = source.cod_source_system;

--$$$

-- Updating BASE CONTACT - Autovit
update crm_integration_anlt.t_lkp_contact
set cod_atlas_user = source.cod_atlas_user
from
  (
    select
      scai_valid_from.dat_processing valid_from,
      base_contact.cod_contact,
      base_contact.cod_source_system,
      atlas_user.cod_atlas_user
    from
      (
        select
          rel_integr_proc.dat_processing
        from
          crm_integration_anlt.t_lkp_scai_process proc,
          crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc
        where
          rel_integr_proc.cod_process = proc.cod_process
          and rel_integr_proc.cod_country = 4
          and rel_integr_proc.cod_integration = 30000
          and rel_integr_proc.ind_active = 1
          and proc.dsc_process_short = 't_lkp_contact'
      ) scai_valid_from,
      crm_integration_anlt.t_lkp_atlas_user atlas_user,
      crm_integration_anlt.t_lkp_contact base_contact
    where
      atlas_user.cod_source_system = 5
      and atlas_user.valid_to = 20991231
      and lower(base_contact.email) = lower(atlas_user.dsc_atlas_user)
	  and trim(base_contact.email) != ''
      and base_contact.cod_source_system = 18
      and base_contact.valid_from = scai_valid_from.dat_processing
  ) source
where
  t_lkp_contact.cod_contact = source.cod_contact
  and t_lkp_contact.valid_from = source.valid_from
  and t_lkp_contact.cod_source_system = source.cod_source_system;

--$$$

-- Updating BASE CONTACT - Storia
update crm_integration_anlt.t_lkp_contact
set cod_atlas_user = source.cod_atlas_user
from
  (
    select
      scai_valid_from.dat_processing valid_from,
      base_contact.cod_contact,
      base_contact.cod_source_system,
      atlas_user.cod_atlas_user
    from
      (
        select
          rel_integr_proc.dat_processing
        from
          crm_integration_anlt.t_lkp_scai_process proc,
          crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc
        where
          rel_integr_proc.cod_process = proc.cod_process
          and rel_integr_proc.cod_country = 4
          and rel_integr_proc.cod_integration = 30000
          and rel_integr_proc.ind_active = 1
          and proc.dsc_process_short = 't_lkp_contact'
      ) scai_valid_from,
      crm_integration_anlt.t_lkp_atlas_user atlas_user,
      crm_integration_anlt.t_lkp_contact base_contact
    where
      atlas_user.cod_source_system = 1
      and atlas_user.valid_to = 20991231
      and lower(base_contact.email) = lower(atlas_user.dsc_atlas_user)
	  and trim(base_contact.email) != ''
      and base_contact.cod_source_system = 19
      and base_contact.valid_from = scai_valid_from.dat_processing
  ) source
where
  t_lkp_contact.cod_contact = source.cod_contact
  and t_lkp_contact.valid_from = source.valid_from
  and t_lkp_contact.cod_source_system = source.cod_source_system;

--$$$

-- Updating BASE CONTACT - Not found
update crm_integration_anlt.t_lkp_contact
set cod_atlas_user = -2
where cod_atlas_user is null
and valid_from =
    (
      select
        rel_integr_proc.dat_processing
      from
        crm_integration_anlt.t_lkp_scai_process proc,
        crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc
      where
        rel_integr_proc.cod_process = proc.cod_process
        and rel_integr_proc.cod_country = 4
        and rel_integr_proc.cod_integration = 30000
        and rel_integr_proc.ind_active = 1
        and proc.dsc_process_short = 't_lkp_contact'
    )
and cod_source_system in (select cod_source_system from crm_integration_anlt.t_lkp_source_system where cod_country = 4);

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
    (select coalesce(max(cod_execution),0) max_cod_exec from crm_integration_anlt.t_fac_scai_execution),
    crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
    crm_integration_anlt.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 4 -- Romania
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
    and rel_country_integr.ind_active = 1
    and rel_integr_proc.ind_active = 1
    and proc.dsc_process_short = 't_lkp_contact_upd_atlas_user';

--$$$
	
-- #######################
-- ####    PASSO 6    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set cod_status = 1, -- Ok
last_processing_datetime = null
from crm_integration_anlt.t_lkp_scai_process proc
where t_rel_scai_integration_process.cod_process = proc.cod_process
and t_rel_scai_integration_process.cod_status = 2
and t_rel_scai_integration_process.cod_country = 4
and proc.dsc_process_short = 't_lkp_contact_upd_atlas_user'
and t_rel_scai_integration_process.ind_active = 1;




