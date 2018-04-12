-- #######################
-- ####    PASSO 1    ####
-- #######################
update crm_integration_anlt.t_rel_scai_country_integration
    set dat_processing = cast(to_char(trunc(sysdate),'yyyymmdd') as int),
      execution_nbr = case
                        when trunc(sysdate) - to_date(dat_processing,'yyyymmdd') > 1 then 1
                          else execution_nbr + 1
                      end,
      cod_status = 2 -- Running
where
    cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and cod_country = 4 -- Romania
	and ind_active = 1; 

-- #######################
-- ####    PASSO 2    ####
-- #######################
insert into crm_integration_anlt.t_fac_scai_execution
  select
    max_cod_exec + 1 cod_execution,
    cod_country,
    cod_integration,
    -1 cod_process,
    2 cod_status, -- Running
    1 cod_execution_type, -- Begin
    dat_processing,
    execution_nbr,
    sysdate
  from
    crm_integration_anlt.t_rel_scai_country_integration,
    (select coalesce(max(cod_execution),0) max_cod_exec from crm_integration_anlt.t_fac_scai_execution)
  where
    cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and cod_country = 4 -- Romania
	and ind_active = 1;

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

drop table if exists crm_integration_anlt.tmp_ro_load_base_source;

create table crm_integration_anlt.tmp_ro_load_base_source 
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
    crm_integration_anlt.t_lkp_base_source target
  where
    coalesce(source_table.opr_base_source,-1) = target.opr_base_source(+)
	and source_table.cod_source_system = target.cod_source_system (+)
    and target.valid_to(+) = 20991231
    and coalesce(source_table.opr_resource_type,'Unknown') = lkp_resource_type.opr_resource_type
	and lkp_resource_type.valid_to = 20991231;

analyze crm_integration_anlt.tmp_ro_load_base_source;
	
--$$$
	
delete from crm_integration_anlt.t_lkp_base_source
using crm_integration_anlt.tmp_ro_load_base_source
where 
	tmp_ro_load_base_source.dml_type = 'I' 
	and t_lkp_base_source.opr_base_source = tmp_ro_load_base_source.opr_base_source 
	and t_lkp_base_source.valid_from = (select dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_base_source');

--$$$
	
update crm_integration_anlt.t_lkp_base_source
set valid_to = (select rel_integr_proc.dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_base_source')
from crm_integration_anlt.tmp_ro_load_base_source source
where source.cod_base_source = crm_integration_anlt.t_lkp_base_source.cod_base_source
and crm_integration_anlt.t_lkp_base_source.valid_to = 20991231
and source.dml_type in('U','D');

--$$$

insert into crm_integration_anlt.t_lkp_base_source
    select
      case
        when dml_type = 'I' then max_cod + new_cod
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
      crm_integration_anlt.tmp_ro_load_base_source
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

-- #######################
-- ####    PASSO 6    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set cod_status = 1, -- Ok
last_processing_datetime = coalesce((select max(updated_at) from crm_integration_anlt.tmp_ro_load_base_source),last_processing_datetime)
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

drop table if exists crm_integration_anlt.tmp_ro_load_base_source;

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

drop table if exists crm_integration_anlt.tmp_ro_load_base_user;

create table crm_integration_anlt.tmp_ro_load_base_user 
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
		md5(coalesce(dsc_base_user,'') + coalesce(email,'') + coalesce(role,'') + coalesce(status,'') + decode(flg_confirmed, 1, 1, 0)) hash_base_user
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
    crm_integration_anlt.t_lkp_base_user target
  where
    coalesce(source_table.opr_base_user,-1) = target.opr_base_user(+)
	and source_table.cod_source_system = target.cod_source_system (+)
    and target.valid_to(+) = 20991231; -- Romania

analyze crm_integration_anlt.tmp_ro_load_base_user;

--$$$
	
delete from crm_integration_anlt.t_lkp_base_user
using crm_integration_anlt.tmp_ro_load_base_user
where 
	tmp_ro_load_base_user.dml_type = 'I' 
	and t_lkp_base_user.opr_base_user = tmp_ro_load_base_user.opr_base_user 
	and t_lkp_base_user.valid_from = (select dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_base_user');

--$$$

-- update valid_to in the updated/deleted records on source	
update crm_integration_anlt.t_lkp_base_user
set valid_to = (select rel_integr_proc.dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_base_user') 
from crm_integration_anlt.tmp_ro_load_base_user source
where source.cod_base_user = crm_integration_anlt.t_lkp_base_user.cod_base_user
and crm_integration_anlt.t_lkp_base_user.valid_to = 20991231
and source.dml_type in('U','D');

--$$$

insert into crm_integration_anlt.t_lkp_base_user
    select
      case
        when dml_type = 'I' then max_cod + new_cod
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
      created_at,
      updated_at,
      deleted_at,
      hash_base_user,
	  cod_execution
    from
      crm_integration_anlt.tmp_ro_load_base_user
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

-- #######################
-- ####    PASSO 6    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set cod_status = 1, -- Ok
last_processing_datetime = coalesce((select max(updated_at) from crm_integration_anlt.tmp_ro_load_base_user),last_processing_datetime)
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

drop table if exists crm_integration_anlt.tmp_ro_load_base_user;

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

drop table if exists crm_integration_anlt.tmp_ro_load_task;

create table crm_integration_anlt.tmp_ro_load_task 
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
    crm_integration_anlt.t_lkp_task target
  where
    coalesce(source_table.opr_task,-1) = target.opr_task(+)
	and source_table.cod_source_system = target.cod_source_system (+)
    and target.valid_to(+) = 20991231
	and coalesce(source_table.opr_base_user_owner,'-1') = lkp_base_user_owner.opr_base_user
	and source_table.cod_source_system = lkp_base_user_owner.cod_source_system -- new
	and lkp_base_user_owner.valid_to = 20991231
    and coalesce(source_table.opr_base_user_creator,-1) = lkp_base_user_creator.opr_base_user
	and source_table.cod_source_system = lkp_base_user_creator.cod_source_system -- new
	and lkp_base_user_creator.valid_to = 20991231
    and coalesce(source_table.opr_resource_type,'Unknown') = lkp_resource_type.opr_resource_type
	and lkp_resource_type.valid_to = 20991231;

analyze crm_integration_anlt.tmp_ro_load_task;
	
	--$$$
	
delete from crm_integration_anlt.t_lkp_task
using crm_integration_anlt.tmp_ro_load_task
where 
	tmp_ro_load_task.dml_type = 'I' 
	and t_lkp_task.opr_task = tmp_ro_load_task.opr_task 
	and t_lkp_task.valid_from = (select dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_task');

	--$$$
	
update crm_integration_anlt.t_lkp_task
set valid_to = (select rel_integr_proc.dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_task')
from crm_integration_anlt.tmp_ro_load_task source
where source.cod_task = crm_integration_anlt.t_lkp_task.cod_task
and crm_integration_anlt.t_lkp_task.valid_to = 20991231
and source.dml_type in('U','D');

--$$$

insert into crm_integration_anlt.t_lkp_task
    select
      case
        when dml_type = 'I' then max_cod + new_cod
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
      crm_integration_anlt.tmp_ro_load_task
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

-- #######################
-- ####    PASSO 6    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set cod_status = 1, -- Ok
last_processing_datetime = coalesce((select max(updated_at) from crm_integration_anlt.tmp_ro_load_task),last_processing_datetime)
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

drop table if exists crm_integration_anlt.tmp_ro_load_task;

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

	--$$$ -- 20
	
-- #############################################
-- # 	          BASE - Romania               #
-- #        LOADING t_lkp_call_outcome         #
-- #############################################

drop table if exists crm_integration_anlt.tmp_ro_load_call_outcome;

create table crm_integration_anlt.tmp_ro_load_call_outcome 
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
    crm_integration_anlt.t_lkp_call_outcome target
  where
    coalesce(source_table.opr_call_outcome,-1) = target.opr_call_outcome(+)
	and source_table.cod_source_system = target.cod_source_system (+)
    and target.valid_to(+) = 20991231
    ) source, crm_integration_anlt.t_lkp_base_user lkp_user_creator
    where coalesce(source.opr_base_user,-1) = lkp_user_creator.opr_base_user (+)
	and source.cod_source_system = lkp_user_creator.cod_source_system (+) -- new
	and lkp_user_creator.valid_to (+) = 20991231;

analyze crm_integration_anlt.tmp_ro_load_call_outcome;

	--$$$
	
delete from crm_integration_anlt.t_lkp_call_outcome
using crm_integration_anlt.tmp_ro_load_call_outcome
where 
	tmp_ro_load_call_outcome.dml_type = 'I' 
	and t_lkp_call_outcome.opr_call_outcome = tmp_ro_load_call_outcome.opr_call_outcome 
	and t_lkp_call_outcome.valid_from = (select dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_call_outcome');

	--$$$
	
update crm_integration_anlt.t_lkp_call_outcome
set valid_to = (select rel_integr_proc.dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_call_outcome') 
from crm_integration_anlt.tmp_ro_load_call_outcome source
where source.cod_call_outcome = crm_integration_anlt.t_lkp_call_outcome.cod_call_outcome
and crm_integration_anlt.t_lkp_call_outcome.valid_to = 20991231
and source.dml_type in('U','D');

--$$$

insert into crm_integration_anlt.t_lkp_call_outcome
    select
      case
        when dml_type = 'I' then max_cod + new_cod
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
      crm_integration_anlt.tmp_ro_load_call_outcome
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

-- #######################
-- ####    PASSO 6    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set cod_status = 1, -- Ok
last_processing_datetime = coalesce((select max(updated_at) from crm_integration_anlt.tmp_ro_load_call_outcome),last_processing_datetime)
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

drop table if exists crm_integration_anlt.tmp_ro_load_call_outcome;

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
    crm_integration_anlt.t_lkp_contact target
  where
	source_table.opr_contact = target.opr_contact(+)
	and source_table.cod_source_system = target.cod_source_system (+)
    and target.valid_to(+) = 20991231
	and coalesce(source_table.opr_base_user_owner,'-1') = lkp_base_user_owner.opr_base_user
	and source_table.cod_source_system = lkp_base_user_owner.cod_source_system -- new
	and lkp_base_user_owner.valid_to = 20991231
    and coalesce(source_table.opr_base_user_creator,-1) = lkp_base_user_creator.opr_base_user
	and source_table.cod_source_system = lkp_base_user_creator.cod_source_system -- new
	and lkp_base_user_creator.valid_to = 20991231
    and coalesce(source_table.opr_industry,'Unknown') = lkp_industry.opr_industry
	and source_table.cod_source_system = lkp_industry.cod_source_system -- new
	and lkp_industry.valid_to = 20991231;

analyze crm_integration_anlt.tmp_ro_load_contact;
	
	--$$$
	
delete from crm_integration_anlt.t_lkp_contact
using crm_integration_anlt.tmp_ro_load_contact
where 
	tmp_ro_load_contact.dml_type = 'I' 
	and t_lkp_contact.opr_contact = tmp_ro_load_contact.opr_contact 
	and t_lkp_contact.valid_from = (select dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_contact');

	--$$$
	
-- update valid_to in the updated/deleted records on source	
update crm_integration_anlt.t_lkp_contact
set valid_to = (select rel_integr_proc.dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_contact') 
from crm_integration_anlt.tmp_ro_load_contact source
where source.cod_contact = crm_integration_anlt.t_lkp_contact.cod_contact
and crm_integration_anlt.t_lkp_contact.valid_to = 20991231
and source.dml_type in('U','D');

	--$$$
	
insert into crm_integration_anlt.t_lkp_contact
    select
      case
        when dml_type = 'I' then max_cod + new_cod
        when dml_type = 'U' then cod_contact
      end cod_contact,
      opr_contact,
      dsc_contact,
      cod_source_system,
      (select rel_integr_proc.dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_contact') valid_from, 
      20991231 valid_to,
	  cod_base_user_creator cod_base_user,
    null cod_atlas_user,
	  contact_id,
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

drop table if exists crm_integration_anlt.tmp_ro_contact_custom_field;

create table crm_integration_anlt.tmp_ro_contact_custom_field 
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
        crm_integration_anlt.tmp_ro_load_contact ts,
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
            gen_num between 1 and (select max(regexp_count(custom_fields, '\\","') + 1) from crm_integration_anlt.tmp_ro_load_contact)
        ) s
      where
        split_part(custom_fields, '","', s.gen_num) != ''
        and custom_fields != '{}'
    )
;

analyze crm_integration_anlt.tmp_ro_contact_custom_field;

drop table if exists crm_integration_anlt.tmp_ro_load_custom_field;

	--$$$
	
create table crm_integration_anlt.tmp_ro_load_custom_field as
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
        crm_integration_anlt.tmp_ro_contact_custom_field,
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
    crm_integration_anlt.t_lkp_custom_field target,
    crm_integration_anlt.t_lkp_custom_field_context cf_context
  where
    coalesce(source_table.opr_custom_field,'-1') = target.opr_custom_field(+)
    and target.valid_to(+) = 20991231
    and cf_context.opr_custom_field_context = 'Contacts';

analyze crm_integration_anlt.tmp_ro_load_custom_field;
	
	--$$$
	
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
      crm_integration_anlt.tmp_ro_load_custom_field
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

--drop table if exists crm_integration_anlt.tmp_ro_contact_custom_field; ######### ESTA TABELA É ELIMINADA NO PROCESSO SEGUINTE
drop table if exists crm_integration_anlt.tmp_ro_load_custom_field;

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

drop table if exists crm_integration_anlt.tmp_ro_rel_contact_custom_field;

create table crm_integration_anlt.tmp_ro_rel_contact_custom_field as
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
        crm_integration_anlt.tmp_ro_contact_custom_field tmp_cf,
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

analyze crm_integration_anlt.tmp_ro_rel_contact_custom_field;
	
	--$$$
	
update crm_integration_anlt.t_rel_contact_custom_field
set valid_to = (select rel_integr_proc.dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_rel_contact_custom_field') 
from crm_integration_anlt.tmp_ro_rel_contact_custom_field source
where source.cod_contact = crm_integration_anlt.t_rel_contact_custom_field.cod_contact
and source.cod_custom_field = crm_integration_anlt.t_rel_contact_custom_field.cod_custom_field
and source.cod_source_system = crm_integration_anlt.t_rel_contact_custom_field.cod_source_system
and crm_integration_anlt.t_rel_contact_custom_field.valid_to = 20991231
and source.dml_type = 'U';

	--$$$
	
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
    crm_integration_anlt.tmp_ro_rel_contact_custom_field
  where
    dml_type in ('I','U');

analyze crm_integration_anlt.t_rel_contact_custom_field;
	
	--$$$ -- 40
	
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

drop table if exists crm_integration_anlt.tmp_ro_rel_contact_custom_field;
drop table if exists crm_integration_anlt.tmp_ro_contact_custom_field;
drop table if exists crm_integration_anlt.tmp_ro_load_contact;

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

drop table if exists crm_integration_anlt.tmp_ro_load_lead;

create table crm_integration_anlt.tmp_ro_load_lead 
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
    crm_integration_anlt.t_lkp_lead target
  where
    coalesce(source_table.opr_lead,-1) = target.opr_lead(+)
	and source_table.cod_source_system = target.cod_source_system (+)
    and target.valid_to(+) = 20991231
    and coalesce(source_table.opr_base_user_owner,-1) = lkp_base_user_owner.opr_base_user
	and source_table.cod_source_system = lkp_base_user_owner.cod_source_system -- new
	and lkp_base_user_owner.valid_to = 20991231
    and coalesce(source_table.opr_base_user_creator,-1) = lkp_base_user_creator.opr_base_user
	and source_table.cod_source_system = lkp_base_user_creator.cod_source_system -- new
	and lkp_base_user_creator.valid_to = 20991231
    and coalesce(source_table.opr_industry,'Unknown') = lkp_industry.opr_industry -- LOST DATA
	and source_table.cod_source_system = lkp_industry.cod_source_system -- new
	and lkp_industry.valid_to = 20991231
    and coalesce(source_table.opr_lead_status,'Unknown') = lkp_lead_status.opr_lead_status
	and source_table.cod_source_system = lkp_lead_status.cod_source_system -- new
	and lkp_lead_status.valid_to = 20991231;

analyze crm_integration_anlt.tmp_ro_load_lead;
	
	--$$$
	
delete from crm_integration_anlt.t_lkp_lead
using crm_integration_anlt.tmp_ro_load_lead
where 
	tmp_ro_load_lead.dml_type = 'I' 
	and t_lkp_lead.opr_lead = tmp_ro_load_lead.opr_lead 
	and t_lkp_lead.valid_from = (select dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_lead');

	--$$$
	
-- update valid_to in the updated/deleted records on source	
update crm_integration_anlt.t_lkp_lead
set valid_to = (select rel_integr_proc.dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_lead') 
from crm_integration_anlt.tmp_ro_load_lead source
where source.cod_lead = crm_integration_anlt.t_lkp_lead.cod_lead
and crm_integration_anlt.t_lkp_lead.valid_to = 20991231
and source.dml_type in('U','D');

	--$$$
	
insert into crm_integration_anlt.t_lkp_lead
    select
      case
        when dml_type = 'I' then max_cod + new_cod
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
      crm_integration_anlt.tmp_ro_load_lead
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

-- #######################
-- ####    PASSO 6    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set cod_status = 1, -- Ok
last_processing_datetime = coalesce((select max(updated_at) from crm_integration_anlt.tmp_ro_load_lead),last_processing_datetime)
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

drop table if exists crm_integration_anlt.tmp_ro_load_lead;

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

drop table if exists crm_integration_anlt.tmp_ro_load_loss_reason;

create table crm_integration_anlt.tmp_ro_load_loss_reason 
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
    crm_integration_anlt.t_lkp_loss_reason target
  where
    coalesce(source_table.opr_loss_reason,-1) = target.opr_loss_reason(+)
	and source_table.cod_source_system = target.cod_source_system (+)
    and target.valid_to(+) = 20991231
    and coalesce(source_table.opr_base_user_creator,-1) = lkp_user_creator.opr_base_user
	and source_table.cod_source_system = lkp_user_creator.cod_source_system -- new
	and lkp_user_creator.valid_to = 20991231;

analyze crm_integration_anlt.tmp_ro_load_loss_reason;
	
	--$$$
	
delete from crm_integration_anlt.t_lkp_loss_reason
using crm_integration_anlt.tmp_ro_load_loss_reason
where 
	tmp_ro_load_loss_reason.dml_type = 'I' 
	and t_lkp_loss_reason.opr_loss_reason = tmp_ro_load_loss_reason.opr_loss_reason 
	and t_lkp_loss_reason.valid_from = (select dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_loss_reason');

	--$$$
	
update crm_integration_anlt.t_lkp_loss_reason
set valid_to = (select rel_integr_proc.dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_loss_reason') 
from crm_integration_anlt.tmp_ro_load_loss_reason source
where source.cod_loss_reason = crm_integration_anlt.t_lkp_loss_reason.cod_loss_reason
and crm_integration_anlt.t_lkp_loss_reason.valid_to = 20991231
and source.dml_type in('U','D');

insert into crm_integration_anlt.t_lkp_loss_reason
	select
	  case
		when dml_type = 'I' then max_cod + new_cod
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
	  crm_integration_anlt.tmp_ro_load_loss_reason
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

-- #######################
-- ####    PASSO 6    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set cod_status = 1, -- Ok
last_processing_datetime = coalesce((select max(updated_at) from crm_integration_anlt.tmp_ro_load_loss_reason),last_processing_datetime)
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

drop table if exists crm_integration_anlt.tmp_ro_load_loss_reason;

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

drop table if exists crm_integration_anlt.tmp_ro_load_pipeline;

create table crm_integration_anlt.tmp_ro_load_pipeline 
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
    crm_integration_anlt.t_lkp_pipeline target
  where
    coalesce(source_table.opr_pipeline,-1) = target.opr_pipeline(+)
	and source_table.cod_source_system = target.cod_source_system (+)
    and target.valid_to(+) = 20991231;

analyze crm_integration_anlt.tmp_ro_load_pipeline;
	
	--$$$
	
delete from crm_integration_anlt.t_lkp_pipeline
using crm_integration_anlt.tmp_ro_load_pipeline
where 
	tmp_ro_load_pipeline.dml_type = 'I' 
	and t_lkp_pipeline.opr_pipeline = tmp_ro_load_pipeline.opr_pipeline
	and t_lkp_pipeline.valid_from = (select dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_pipeline');

	--$$$
	
update crm_integration_anlt.t_lkp_pipeline
set valid_to = (select rel_integr_proc.dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_pipeline') 
from crm_integration_anlt.tmp_ro_load_pipeline source
where source.cod_pipeline = crm_integration_anlt.t_lkp_pipeline.cod_pipeline
and crm_integration_anlt.t_lkp_pipeline.valid_to = 20991231
and source.dml_type in('U','D');

insert into crm_integration_anlt.t_lkp_pipeline
    select
      case
        when dml_type = 'I' then max_cod + new_cod
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
      crm_integration_anlt.tmp_ro_load_pipeline
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

-- #######################
-- ####    PASSO 6    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set cod_status = 1, -- Ok
last_processing_datetime = coalesce((select max(updated_at) from crm_integration_anlt.tmp_ro_load_pipeline),last_processing_datetime)
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

drop table if exists crm_integration_anlt.tmp_ro_load_pipeline;

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

drop table if exists crm_integration_anlt.tmp_ro_load_stage;

create table crm_integration_anlt.tmp_ro_load_stage 
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
    crm_integration_anlt.t_lkp_stage target
  where
    coalesce(source_table.opr_stage,-1) = target.opr_stage(+)
	and source_table.cod_source_system = target.cod_source_system (+)
    and target.valid_to(+) = 20991231
    and coalesce(source_table.opr_pipeline,-1) = lkp_pipeline.opr_pipeline
	and source_table.cod_source_system = lkp_pipeline.cod_source_system -- new
	and lkp_pipeline.valid_to = 20991231;

analyze crm_integration_anlt.tmp_ro_load_stage;
	
	--$$$
	
delete from crm_integration_anlt.t_lkp_stage
using crm_integration_anlt.tmp_ro_load_stage
where 
	tmp_ro_load_stage.dml_type = 'I' 
	and t_lkp_stage.opr_stage = tmp_ro_load_stage.opr_stage
	and t_lkp_stage.valid_from = (select dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_stage');

	--$$$ -- 60
	
update crm_integration_anlt.t_lkp_stage
set valid_to = (select rel_integr_proc.dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_stage') 
from crm_integration_anlt.tmp_ro_load_stage source
where source.cod_stage = crm_integration_anlt.t_lkp_stage.cod_stage
and crm_integration_anlt.t_lkp_stage.valid_to = 20991231
and source.dml_type in('U','D');

	--$$$
	
insert into crm_integration_anlt.t_lkp_stage
    select
      case
        when dml_type = 'I' then max_cod + new_cod
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
      crm_integration_anlt.tmp_ro_load_stage
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

-- #######################
-- ####    PASSO 6    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set cod_status = 1, -- Ok
last_processing_datetime = coalesce((select max(updated_at) from crm_integration_anlt.tmp_ro_load_stage),last_processing_datetime)
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

drop table if exists crm_integration_anlt.tmp_ro_load_stage;

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

drop table if exists crm_integration_anlt.tmp_ro_load_deals;

create table crm_integration_anlt.tmp_ro_load_deals 
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
    crm_integration_anlt.t_lkp_deal target
  where
    source_table.opr_deal = target.opr_deal(+)
    and source_table.cod_source_system = target.cod_source_system (+)
    and target.valid_to(+) = 20991231
    and coalesce(source_table.opr_base_user_owner,-1) = lkp_base_user_owner.opr_base_user
    and source_table.cod_source_system = lkp_base_user_owner.cod_source_system -- new
    and lkp_base_user_owner.valid_to = 20991231
    and coalesce(source_table.opr_base_user_creator,-1) = lkp_base_user_creator.opr_base_user
    and source_table.cod_source_system = lkp_base_user_creator.cod_source_system -- new
    and lkp_base_user_creator.valid_to = 20991231
    and coalesce(source_table.opr_currency,'Unknown') = lkp_currency.opr_currency
    and lkp_currency.valid_to = 20991231
    and coalesce(source_table.opr_loss_reason,-1) = lkp_loss_reason.opr_loss_reason
    and lkp_currency.valid_to = 20991231
    and coalesce(source_table.opr_stage,-1) = lkp_stages.opr_stage
    and source_table.cod_source_system = lkp_stages.cod_source_system -- new
    and lkp_currency.valid_to = 20991231
    and coalesce(source_table.opr_base_source,-1) = lkp_base_source.opr_base_source
    and lkp_base_source.valid_to = 20991231
    and coalesce(source_table.opr_contact,-1) = lkp_contact.opr_contact
    and source_table.cod_source_system = lkp_contact.cod_source_system -- new
    and lkp_contact.valid_to = 20991231;

analyze crm_integration_anlt.tmp_ro_load_deals;
	
	--$$$
	
delete from crm_integration_anlt.t_lkp_deal
using crm_integration_anlt.tmp_ro_load_deals
where 
	tmp_ro_load_deals.dml_type = 'I' 
	and t_lkp_deal.opr_deal = tmp_ro_load_deals.opr_deal
	and t_lkp_deal.valid_from = (select dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_deal');

	--$$$
	
update crm_integration_anlt.t_lkp_deal
set valid_to = (select rel_integr_proc.dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_deal') 
from crm_integration_anlt.tmp_ro_load_deals source
where source.cod_deal = crm_integration_anlt.t_lkp_deal.cod_deal
and crm_integration_anlt.t_lkp_deal.valid_to = 20991231
and source.dml_type in('U','D');
	
insert into crm_integration_anlt.t_lkp_deal
    select
      case
        when dml_type = 'I' then max_cod + new_cod
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
      crm_integration_anlt.tmp_ro_load_deals
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

-- #######################
-- ####    PASSO 6    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set cod_status = 1, -- Ok
last_processing_datetime = coalesce((select max(updated_at) from crm_integration_anlt.tmp_ro_load_deals),last_processing_datetime)
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

drop table if exists crm_integration_anlt.tmp_ro_load_deals;

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

drop table if exists crm_integration_anlt.tmp_ro_load_calls;

create table crm_integration_anlt.tmp_ro_load_calls 
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
      coalesce(lkp_contact.cod_contact,-2) cod_contact,
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
        coalesce(opr_contact,0) +
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
            resource_id opr_contact,
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
      crm_integration_anlt.t_lkp_resource_type lkp_resource_type,
      (select coalesce(max(cod_call),0) max_cod from crm_integration_anlt.t_fac_call) max_cod_calls,
      crm_integration_anlt.t_fac_call target
    where
      source_table.opr_call = target.opr_call(+)
      and source_table.cod_source_system = target.cod_source_system (+)
      and coalesce(source_table.opr_base_user,-1) = lkp_base_user.opr_base_user
      and source_table.cod_source_system = lkp_base_user.cod_source_system
      and lkp_base_user.valid_to = 20991231
      and coalesce(source_table.opr_contact,-1) = lkp_contact.opr_contact(+)
      and source_table.cod_source_system = lkp_contact.cod_source_system(+)
      and lkp_contact.valid_to(+) = 20991231
      and coalesce(source_table.opr_resource_type,'') = lkp_resource_type.opr_resource_type
      and lkp_resource_type.valid_to = 20991231
      and source_table.rn = 1
  ) source,
  crm_integration_anlt.t_lkp_call_outcome lkp_call_outcome
where
  coalesce(source.opr_call_outcome,-1) = lkp_call_outcome.opr_call_outcome (+)
  and source.cod_source_system = lkp_call_outcome.cod_source_system (+)
  and lkp_call_outcome.valid_to (+) = 20991231;

analyze crm_integration_anlt.tmp_ro_load_calls;
	
	--$$$
	
insert into crm_integration_anlt.t_hst_call
    select
      target.*
    from
      crm_integration_anlt.t_fac_call target,
      crm_integration_anlt.tmp_ro_load_calls source
    where
      target.opr_call = source.opr_call
      and source.dml_type = 'U';

	--$$$
	
delete from crm_integration_anlt.t_fac_call
using crm_integration_anlt.tmp_ro_load_calls
where crm_integration_anlt.t_fac_call.opr_call = crm_integration_anlt.tmp_ro_load_calls.opr_call
and crm_integration_anlt.tmp_ro_load_calls.dml_type = 'U';

	--$$$
	
insert into crm_integration_anlt.t_fac_call
    select
      case
        when dml_type = 'I' then max_cod + new_cod
        when dml_type = 'U' then cod_call
      end cod_call,
      opr_call,
      cod_contact,
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
      crm_integration_anlt.tmp_ro_load_calls
    where
      dml_type in ('U','I');

analyze crm_integration_anlt.t_fac_call;
	  
	--$$$
	
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
			  crm_integration_anlt.tmp_ro_load_calls a,
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
			  crm_integration_anlt.tmp_ro_load_calls a,
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
			  crm_integration_anlt.tmp_ro_load_calls a,
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
			  crm_integration_anlt.tmp_ro_load_calls a,
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
			  crm_integration_anlt.tmp_ro_load_calls a,
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
			  crm_integration_anlt.tmp_ro_load_calls a,
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
		crm_integration_anlt.tmp_ro_load_calls a,
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

-- #######################
-- ####    PASSO 6    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set cod_status = 1, -- Ok
last_processing_datetime = coalesce((select max(updated_at) from crm_integration_anlt.tmp_ro_load_calls),last_processing_datetime)
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

drop table if exists crm_integration_anlt.tmp_ro_load_calls;

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

drop table if exists crm_integration_anlt.tmp_ro_load_orders;

create table crm_integration_anlt.tmp_ro_load_orders 
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
  and source_table.opr_deal = lkp_deals.opr_deal
	and source_table.cod_source_system = lkp_deals.cod_source_system
    and lkp_deals.valid_to = 20991231;

analyze crm_integration_anlt.tmp_ro_load_orders;
	
	--$$$
	
insert into crm_integration_anlt.t_hst_order
    select
      target.*
    from
      crm_integration_anlt.t_fac_order target,
      crm_integration_anlt.tmp_ro_load_orders source
    where
      target.opr_order = source.opr_order
      and source.dml_type = 'U';

	--$$$
	
delete from crm_integration_anlt.t_fac_order
using crm_integration_anlt.tmp_ro_load_orders
where crm_integration_anlt.t_fac_order.opr_order = crm_integration_anlt.tmp_ro_load_orders.opr_order
and crm_integration_anlt.tmp_ro_load_orders.dml_type = 'U';

	--$$$
	
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
      crm_integration_anlt.tmp_ro_load_orders
    where
      dml_type in ('U','I');

analyze crm_integration_anlt.t_fac_order;
	  
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

-- #######################
-- ####    PASSO 6    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set cod_status = 1, -- Ok
last_processing_datetime = coalesce((select max(updated_at) from crm_integration_anlt.tmp_ro_load_orders),last_processing_datetime)
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

drop table if exists crm_integration_anlt.tmp_ro_load_orders;

	--$$$ -- 80
	
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

drop table if exists crm_integration_anlt.tmp_ro_load_order_line_items;

create table crm_integration_anlt.tmp_ro_load_order_line_items 
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
    and coalesce(source_table.opr_sku,'Unknown') = lkp_product.opr_sku
	and source_table.cod_source_system = lkp_product.cod_source_system -- new
    and lkp_product.valid_to = 20991231
    and coalesce(source_table.opr_currency,'Unknown') = lkp_currency.opr_currency
    and lkp_currency.valid_to = 20991231
      and coalesce(source_table.opr_order,-1) = lkp_orders.opr_order -- TAMBÉM DEVEREMOS CONSIDERAR A DATA DAT_ORDER
	and source_table.cod_source_system = lkp_orders.cod_source_system
	and source_table.rn = 1; -- new

analyse crm_integration_anlt.tmp_ro_load_order_line_items;
	
	--$$$
	
insert into crm_integration_anlt.t_hst_order_line_item
    select
      target.*
    from
      crm_integration_anlt.t_fac_order_line_item target,
      crm_integration_anlt.tmp_ro_load_order_line_items source
    where
      target.opr_order_line_item = source.opr_order_line_item
      and source.dml_type = 'U';

	--$$$
	
delete from crm_integration_anlt.t_fac_order_line_item
using crm_integration_anlt.tmp_ro_load_order_line_items
where crm_integration_anlt.t_fac_order_line_item.opr_order_line_item=crm_integration_anlt.tmp_ro_load_order_line_items.opr_order_line_item
and crm_integration_anlt.tmp_ro_load_order_line_items.dml_type = 'U';

	--$$$
	
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
      crm_integration_anlt.tmp_ro_load_order_line_items
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

-- #######################
-- ####    PASSO 6    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set cod_status = 1, -- Ok
last_processing_datetime = coalesce((select max(updated_at) from crm_integration_anlt.tmp_ro_load_order_line_items),last_processing_datetime)
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

drop table if exists crm_integration_anlt.tmp_ro_load_order_line_items;

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
    where proc.dsc_process_short = 't_lkp_category'
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
	and proc.dsc_process_short = 't_lkp_category';	

	--$$$
	
-- #############################################
-- # 		     ATLAS - ROMANIA               #
-- #          LOADING t_lkp_category           #
-- #############################################

drop table if exists crm_integration_anlt.tmp_ro_load_category;

create table crm_integration_anlt.tmp_ro_load_category 
distkey(cod_source_system)
sortkey(cod_category, opr_category)
as
  select
    source_table.opr_category,
    source_table.dsc_category_pl,
    source_table.dsc_category_pt,
    source_table.dsc_category_en,
	source_table.dsc_category_ro,
	source_table.dsc_category_ru,
	source_table.dsc_category_hi,
	source_table.dsc_category_uk,
	source_table.opr_category_parent,
    source_table.category_code,
	source_table.operation_type,
	source_table.operation_timestamp,
    source_table.offer_name_pl,
    source_table.offer_name_en,
    source_table.seek_name_pl,
    source_table.seek_name_en,
    source_table.private_name_pl,
    source_table.private_name_en,
    source_table.private_name_adding_pl,
    source_table.private_name_adding_en,
    source_table.business_name_pl,
    source_table.business_name_en,
    source_table.business_name_adding_pl,
    source_table.business_name_adding_en,
    source_table.offer_name_adding_pl,
    source_table.offer_name_adding_en,
    source_table.seek_name_adding_pl,
    source_table.seek_name_adding_en,
    source_table.flg_premoderated,
    source_table.display_order,
    source_table.flg_offer_seek,
    source_table.flg_private_business,
    source_table.flg_remove_companies,
    source_table.max_photos,
	source_table.extend_days,
    source_table.filter_label_pl,
    source_table.filter_label_en,
    source_table.search_category,
    source_table.search_args,
    source_table.search_routing_params,
    source_table.flg_rmoderation_checkhistory,
    source_table.rmoderation_min_price,
    source_table.rmoderation_hotkey,
    source_table.flg_rmoderation_block_new_price,
    source_table.flg_rmoderation_can_accept_automatically,
    source_table.address_label_pl,
    source_table.address_label_en,
    source_table.cod_category_meta,
    source_table.topads_count,
    source_table.default_view,
    source_table.default_mobile_view,
    source_table.related_categories,
    source_table.hint_description,
    source_table.flg_show_map,
	source_table.title_parameters,
    source_table.flg_for_sale_category,
    source_table.flg_prioritized,
    source_table.default_price_type,
    source_table.flg_use_name_in_solr,
    source_table.flg_allow_exchange,
    source_table.legacy_code,
	source_table.default_currency,
    source_table.dsc_category_long_pl,
    source_table.dsc_category_long_en,
    source_table.dsc_category_singular_pl,
    source_table.dsc_category_singular_en,
	source_table.dsc_category_singular_pt,
    source_table.title_format,
    source_table.flg_has_free_text_search,
    source_table.title_format_description,
    source_table.path_params,
    source_table.seek_name_adding_pt,
    source_table.private_name_pt,
    source_table.private_name_adding_pt,
    source_table.offer_name_pt,
    source_table.offer_name_adding_pt,
    source_table.name_pt,
	source_table.dsc_category_long_pt,
    source_table.filter_label_pt,
    source_table.business_name_pt,
    source_table.business_name_adding_pt,
    source_table.address_label_pt,
    source_table.short_name_with_pronoun_pt,
    source_table.short_name_with_pronoun_en,
	source_table.short_name_with_pronoun_ro,
	source_table.short_name_with_pronoun_hi,
    source_table.short_name_pt,
    source_table.short_name_en,
    source_table.short_name_ro,
    source_table.short_name_hi,
    source_table.seek_name_pt,
    source_table.genitive_name_pt,
    source_table.genitive_name_en,
	source_table.genitive_name_ro,
	source_table.genitive_name_ru,
	source_table.genitive_name_hi,
	source_table.genitive_name_uk,
    source_table.dsc_category_singular_ro,
    source_table.dsc_category_singular_ru,
    source_table.dsc_category_singular_uk,
    source_table.seek_name_ro,
    source_table.seek_name_adding_ro,
    source_table.private_name_ro,
    source_table.private_name_adding_ro,
    source_table.offer_name_ro,
    source_table.offer_name_adding_ro,
    source_table.name_ro,
    source_table.dsc_category_long_ro,
	source_table.dsc_category_long_ru,
	source_table.dsc_category_long_uk,
	source_table.filter_label_ro,
    source_table.business_name_ro,
    source_table.business_name_adding_ro,
    source_table.address_label_ro,
    source_table.genitive_name_pl,
    source_table.short_name_pl,
    source_table.short_name_with_pronoun_pl,
    source_table.address_label_ru,
    source_table.address_label_uk,
    source_table.business_name_adding_ru,
    source_table.business_name_adding_uk,
    source_table.business_name_ru,
    source_table.business_name_uk,
    source_table.filter_label_ru,
    source_table.filter_label_uk,
    source_table.name_ru,
    source_table.name_uk,
    source_table.offer_name_adding_ru,
    source_table.offer_name_adding_uk,
    source_table.offer_name_ru,
    source_table.offer_name_uk,
	source_table.private_name_adding_ru,
    source_table.private_name_adding_uk,
    source_table.private_name_ru,
    source_table.private_name_uk,
    source_table.seek_name_adding_ru,
    source_table.seek_name_adding_uk,
    source_table.seek_name_ru,
    source_table.seek_name_uk,
    source_table.short_name_ru,
    source_table.short_name_uk,
    source_table.short_name_with_pronoun_ru,
    source_table.short_name_with_pronoun_uk,
    source_table.rmoderation_max_price,
    source_table.address_label_hi,
    source_table.business_name_adding_hi,
    source_table.business_name_hi,
    source_table.filter_label_hi,
    source_table.dsc_category_long_hi,
    source_table.name_hi,
    source_table.offer_name_adding_hi,
    source_table.offer_name_hi,
    source_table.private_name_adding_hi,
    source_table.private_name_hi,
    source_table.seek_name_adding_hi,
    source_table.seek_name_hi,
    source_table.dsc_category_singular_hi,
	source_table.flg_for_sale,
	source_table.paidlimits_packet_id,
	source_table.free_ads_limit,
	source_table.app_ads_fb,
	source_table.app_ads_admob,
	source_table.app_ads_admob_ad_campaign,
	source_table.app_ads_admob_ads_campaign,
	source_table.parent_level1,
	source_table.parent_level2,
	source_table.flg_leaf,
    source_table.hash_category,
	--lkp_currency.cod_currency,
    source_table.cod_source_system,
    source_table.cod_execution,
    max_cod_category.max_cod,
    row_number() over (order by source_table.opr_category desc) new_cod,
    target.cod_category,
    case
      --when target.cod_category is null then 'I'
	  when target.cod_category is null or (source_table.hash_category != target.hash_category and target.valid_from = (select dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_category')) then 'I'
	  when source_table.operation_type = 'delete' then 'D'
      when source_table.hash_category != target.hash_category then 'U'
        else 'X'
    end dml_type
  from
    (
	select
		source.*,
		lkp_source_system.cod_source_system,
		md5(
		coalesce(dsc_category_pl                                                                ,'') +
		coalesce(dsc_category_pt                                                                ,'') +
		coalesce(dsc_category_en                                                                ,'') +
		coalesce(dsc_category_ro                                                                ,'') +
		coalesce(dsc_category_ru                                                                ,'') +
		coalesce(dsc_category_hi                                                                ,'') +
		coalesce(dsc_category_uk                                                                ,'') +
		coalesce(opr_category_parent                                                            ,0) +
		coalesce(category_code                                                                  ,'') +
		coalesce(offer_name_pl                                                                  ,'') +
		coalesce(offer_name_en                                                                  ,'') +
		coalesce(seek_name_pl                                                                   ,'') +
		coalesce(seek_name_en                                                                   ,'') +
		coalesce(private_name_pl                                                                ,'') +
		coalesce(private_name_en                                                                ,'') +
		coalesce(private_name_adding_pl                                                         ,'') +
		coalesce(private_name_adding_en                                                         ,'') +
		coalesce(business_name_pl                                                               ,'') +
		coalesce(business_name_en                                                               ,'') +
		coalesce(business_name_adding_pl                                                        ,'') +
		coalesce(business_name_adding_en                                                        ,'') +
		coalesce(offer_name_adding_pl                                                           ,'') +
		coalesce(offer_name_adding_en                                                           ,'') +
		coalesce(seek_name_adding_pl                                                            ,'') +
		coalesce(seek_name_adding_en                                                            ,'') +
		coalesce(flg_premoderated                                                               ,0) +
		coalesce(display_order                                                                  ,0) +
		coalesce(flg_offer_seek                                                                 ,0) +
		coalesce(flg_private_business                                                           ,0) +
		coalesce(flg_remove_companies                                                           ,0) +
		coalesce(max_photos                                                                     ,0) +
		coalesce(extend_days                                                                    ,0) +
		coalesce(default_currency                                                               ,'') +
		coalesce(filter_label_pl                                                                ,'') +
		coalesce(filter_label_en                                                                ,'') +
		coalesce(search_category                                                                ,0) +
		coalesce(search_args                                                                    ,'') +
		coalesce(search_routing_params                                                          ,'') +
		coalesce(flg_rmoderation_checkhistory                                                   ,0) +
		cast(coalesce(rmoderation_min_price                                                      ,0) as varchar) +
		coalesce(rmoderation_hotkey                                                             ,'') +
		coalesce(flg_rmoderation_block_new_price                                                ,0) +
		coalesce(flg_rmoderation_can_accept_automatically                                       ,0) +
		coalesce(address_label_pl                                                               ,'') +
		coalesce(address_label_en                                                               ,'') +
		coalesce(cod_category_meta                                                              ,0) +
		coalesce(topads_count                                                                   ,0) +
		coalesce(default_view                                                                   ,'') +
		coalesce(default_mobile_view                                                            ,'') +
		coalesce(related_categories                                                             ,'') +
		coalesce(hint_description                                                               ,'') +
		coalesce(flg_show_map                                                                   ,0) +
		coalesce(title_parameters                                                               ,'') +
		coalesce(flg_for_sale_category                                                          ,0) +
		coalesce(flg_prioritized                                                                ,0) +
		coalesce(default_price_type                                                             ,'') +
		coalesce(flg_use_name_in_solr                                                           ,0) +
		coalesce(flg_allow_exchange                                                             ,0) +
		coalesce(legacy_code                                                                    ,'') +
		coalesce(dsc_category_long_pl                                                           ,'') +
		coalesce(dsc_category_long_en                                                           ,'') +
		coalesce(dsc_category_singular_pl                                                       ,'') +
		coalesce(dsc_category_singular_en                                                       ,'') +
		coalesce(dsc_category_singular_pt                                                       ,'') +
		coalesce(title_format                                                                   ,'') +
		coalesce(flg_has_free_text_search                                                       ,0) +
		coalesce(title_format_description                                                       ,'') +
		coalesce(path_params                                                                    ,'') +
		coalesce(seek_name_adding_pt                                                            ,'') +
		coalesce(private_name_pt                                                                ,'') +
		coalesce(private_name_adding_pt                                                         ,'') +
		coalesce(offer_name_pt                                                                  ,'') +
		coalesce(offer_name_adding_pt                                                           ,'') +
		coalesce(name_pt                                                                        ,'') +
		coalesce(dsc_category_long_pt                                                           ,'') +
		coalesce(filter_label_pt                                                                ,'') +
		coalesce(business_name_pt                                                               ,'') +
		coalesce(business_name_adding_pt                                                        ,'') +
		coalesce(address_label_pt                                                               ,'') +
		coalesce(short_name_with_pronoun_pt                                                     ,'') +
		coalesce(short_name_with_pronoun_en                                                     ,'') +
		coalesce(short_name_with_pronoun_ro                                                     ,'') +
		coalesce(short_name_with_pronoun_hi                                                     ,'') +
		coalesce(short_name_pt                                                                  ,'') +
		coalesce(short_name_en                                                                  ,'') +
		coalesce(short_name_ro                                                                  ,'') +
		coalesce(short_name_hi                                                                  ,'') +
		coalesce(seek_name_pt                                                                   ,'') +
		coalesce(genitive_name_pt                                                               ,'') +
		coalesce(genitive_name_en                                                               ,'') +
		coalesce(genitive_name_ro                                                               ,'') +
		coalesce(genitive_name_ru                                                               ,'') +
		coalesce(genitive_name_hi                                                               ,'') +
		coalesce(genitive_name_uk                                                               ,'') +
		coalesce(dsc_category_singular_ro                                                       ,'') +
		coalesce(dsc_category_singular_ru                                                       ,'') +
		coalesce(dsc_category_singular_uk                                                       ,'') +
		coalesce(seek_name_ro                                                                   ,'') +
		coalesce(seek_name_adding_ro                                                            ,'') +
		coalesce(private_name_ro                                                                ,'') +
		coalesce(private_name_adding_ro                                                         ,'') +
		coalesce(offer_name_ro                                                                  ,'') +
		coalesce(offer_name_adding_ro                                                           ,'') +
		coalesce(name_ro                                                                        ,'') +
		coalesce(dsc_category_long_ro                                                           ,'') +
		coalesce(dsc_category_long_ru                                                           ,'') +
		coalesce(dsc_category_long_uk                                                           ,'') +
		coalesce(filter_label_ro                                                                ,'') +
		coalesce(business_name_ro                                                               ,'') +
		coalesce(business_name_adding_ro                                                        ,'') +
		coalesce(address_label_ro                                                               ,'') +
		coalesce(genitive_name_pl                                                               ,'') +
		coalesce(short_name_pl                                                                  ,'') +
		coalesce(short_name_with_pronoun_pl                                                     ,'') +
		coalesce(address_label_ru                                                               ,'') +
		coalesce(address_label_uk                                                               ,'') +
		coalesce(business_name_adding_ru                                                        ,'') +
		coalesce(business_name_adding_uk                                                        ,'') +
		coalesce(business_name_ru                                                               ,'') +
		coalesce(business_name_uk                                                               ,'') +
		coalesce(filter_label_ru                                                                ,'') +
		coalesce(filter_label_uk                                                                ,'') +
		coalesce(name_ru                                                                        ,'') +
		coalesce(name_uk                                                                        ,'') +
		coalesce(offer_name_adding_ru                                                           ,'') +
		coalesce(offer_name_adding_uk                                                           ,'') +
		coalesce(offer_name_ru                                                                  ,'') +
		coalesce(offer_name_uk                                                                  ,'') +
		coalesce(private_name_adding_ru                                                         ,'') +
		coalesce(private_name_adding_uk                                                         ,'') +
		coalesce(private_name_ru                                                                ,'') +
		coalesce(private_name_uk                                                                ,'') +
		coalesce(seek_name_adding_ru                                                            ,'') +
		coalesce(seek_name_adding_uk                                                            ,'') +
		coalesce(seek_name_ru                                                                   ,'') +
		coalesce(seek_name_uk                                                                   ,'') +
		coalesce(short_name_ru                                                                  ,'') +
		coalesce(short_name_uk                                                                  ,'') +
		coalesce(short_name_with_pronoun_ru                                                     ,'') +
		coalesce(short_name_with_pronoun_uk                                                     ,'') +
		cast(coalesce(rmoderation_max_price                                                      ,0) as varchar) +
		coalesce(address_label_hi                                                               ,'') +
		coalesce(business_name_adding_hi                                                        ,'') +
		coalesce(business_name_hi                                                               ,'') +
		coalesce(filter_label_hi                                                                ,'') +
		coalesce(dsc_category_long_hi                                                           ,'') +
		coalesce(name_hi                                                                        ,'') +
		coalesce(offer_name_adding_hi                                                           ,'') +
		coalesce(offer_name_hi                                                                  ,'') +
		coalesce(private_name_adding_hi                                                         ,'') +
		coalesce(private_name_hi                                                                ,'') +
		coalesce(seek_name_adding_hi                                                            ,'') +
		coalesce(seek_name_hi                                                                   ,'') +
		coalesce(dsc_category_singular_hi                                                       ,'') +
		coalesce(flg_for_sale                                                                   ,0) +
		coalesce(paidlimits_packet_id                                                           ,0) +
		coalesce(free_ads_limit                                                                 ,0) +
		coalesce(app_ads_fb                                                                     ,0) +
		coalesce(app_ads_admob                                                                  ,0) +
		coalesce(app_ads_admob_ad_campaign                                                      ,'') +
		coalesce(app_ads_admob_ads_campaign                                                     ,'') +
		coalesce(parent_level1                                                                  ,0) +
		coalesce(parent_level2                                                                  ,0) +
		coalesce(flg_leaf                                                                       ,0)
        ) hash_category
	from
		(
      SELECT
	  	id opr_category,
		livesync_dbname opr_source_system,
		operation_type,
		operation_timestamp,
		name_pl dsc_category_pl,
		null dsc_category_pt, -- não existe de todo
		name_en dsc_category_en,
		name_ro dsc_category_ro,
		null dsc_category_ru, -- não existe de todo
		null dsc_category_hi, -- não existe de todo
		null dsc_category_uk, -- não existe de todo
		parent_id opr_category_parent,
		code category_code,
		offer_name_pl,
		offer_name_en,
		seek_name_pl,
		seek_name_en,
		private_name_pl,
		private_name_en,
		private_name_adding_pl,
		private_name_adding_en,
		business_name_pl,
		business_name_en,
		business_name_adding_pl,
		business_name_adding_en,
		offer_name_adding_pl,
		offer_name_adding_en,
		seek_name_adding_pl,
		seek_name_adding_en,
		premoderated flg_premoderated,
		display_order,
		offer_seek flg_offer_seek,
		private_business flg_private_business,
		remove_companies flg_remove_companies,
		max_photos,
		extend_days,
		--default_currency opr_currency,
		default_currency,
		filter_label_pl,
		filter_label_en,
		search_category,
		search_args,
		search_routing_params,
		rmoderation_checkhistory flg_rmoderation_checkhistory,
		rmoderation_minprice rmoderation_min_price,
		rmoderation_hotkey,
		rmoderation_block_new_price flg_rmoderation_block_new_price,
		rmoderation_can_accept_automaticly flg_rmoderation_can_accept_automatically,
		address_label_pl,
		address_label_en,
		meta_category_id cod_category_meta,
		topads_count,
		default_view,
		default_view_mobile default_mobile_view,
		related_categories,
		hint_description,
		show_map flg_show_map,
		title_parameters,
		for_sale_category flg_for_sale_category,
		is_prioritized flg_prioritized,
		default_price_type,
		use_name_in_solr flg_use_name_in_solr,
		allow_exchange flg_allow_exchange,
		legacy_code,
		long_name_pl dsc_category_long_pl,
		long_name_en dsc_category_long_en,
		singular_name_pl dsc_category_singular_pl,
		singular_name_en dsc_category_singular_en,
		null dsc_category_singular_pt, -- não existe de todo
		title_format,
		has_free_text_search flg_has_free_text_search,
		title_description_format title_format_description,
		path_params,
		seek_name_adding_pt,
		private_name_pt,
		private_name_adding_pt,
		offer_name_pt,
		offer_name_adding_pt,
		name_pt,
		long_name_pt dsc_category_long_pt,
		filter_label_pt,
		business_name_pt,
		business_name_adding_pt,
		address_label_pt,
		short_name_with_pronoun_pt,
		short_name_with_pronoun_en,
		null short_name_with_pronoun_ro, -- não existe de todo
		null short_name_with_pronoun_hi, -- não existe de todo
		short_name_pt,
		short_name_en,
		null short_name_ro, -- não existe de todo
		null short_name_hi, -- não existe de todo
		seek_name_pt,
		genitive_name_pt,
		genitive_name_en,
		null genitive_name_ro, -- não existe de todo
		null genitive_name_ru, -- não existe de todo
		null genitive_name_hi, -- não existe de todo
		null genitive_name_uk, -- não existe de todo
		singular_name_ro dsc_category_singular_ro,
		null dsc_category_singular_ru, -- não existe de todo
		null dsc_category_singular_uk, -- não existe de todo
		seek_name_ro,
		seek_name_adding_ro,
		private_name_ro,
		private_name_adding_ro,
		offer_name_ro,
		offer_name_adding_ro,
		name_ro,
		long_name_ro dsc_category_long_ro,
		null dsc_category_long_ru, -- não existe de todo
		null dsc_category_long_uk, -- não existe de todo
		filter_label_ro,
		business_name_ro,
		business_name_adding_ro,
		address_label_ro,
		genitive_name_pl,
		short_name_pl,
		short_name_with_pronoun_pl,
		address_label_ru,
		address_label_uk,
		business_name_adding_ru,
		business_name_adding_uk,
		business_name_ru,
		business_name_uk,
		filter_label_ru,
		filter_label_uk,
		name_ru,
		name_uk,
		offer_name_adding_ru,
		offer_name_adding_uk,
		offer_name_ru,
		offer_name_uk,
		private_name_adding_ru,
		private_name_adding_uk,
		private_name_ru,
		private_name_uk,
		seek_name_adding_ru,
		seek_name_adding_uk,
		seek_name_ru,
		seek_name_uk,
		short_name_ru,
		short_name_uk,
		short_name_with_pronoun_ru,
		short_name_with_pronoun_uk,
		rmoderation_maxprice rmoderation_max_price,
		address_label_hi,
		business_name_adding_hi,
		business_name_hi,
		filter_label_hi,
		long_name_hi dsc_category_long_hi,
		name_hi,
		offer_name_adding_hi,
		offer_name_hi,
		private_name_adding_hi,
		private_name_hi,
		seek_name_adding_hi,
		seek_name_hi,
		singular_name_hi dsc_category_singular_hi,
		cast(null as bigint) flg_for_sale,
		cast(null as bigint) paidlimits_packet_id,
		cast(null as bigint) free_ads_limit,
		cast(null as bigint) app_ads_fb,
		cast(null as bigint) app_ads_admob,
		cast(null as varchar) app_ads_admob_ad_campaign,
		cast(null as varchar) app_ads_admob_ads_campaign,
		cast(null as bigint) parent_level1,
		cast(null as bigint) parent_level2,
		cast(null as bigint) flg_leaf,
		scai_execution.cod_execution
     FROM
        crm_integration_stg.stg_ro_db_atlas_verticals_categories a,
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
            and proc.dsc_process_short = 't_lkp_category'
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
	  	id opr_category,
		'olxro' opr_source_system,
		operation_type,
		operation_timestamp,
		null dsc_category_pl,
		null dsc_category_pt, -- não existe de todo
		null dsc_category_en,
		name_ro dsc_category_ro,
		null dsc_category_ru, -- não existe de todo
		null dsc_category_hi, -- não existe de todo
		null dsc_category_uk, -- não existe de todo
		parent_id opr_category_parent,
		code category_code,
		null offer_name_pl,
		null offer_name_en,
		null seek_name_pl,
		null seek_name_en,
		null private_name_pl,
		null private_name_en,
		null private_name_adding_pl,
		null private_name_adding_en,
		null business_name_pl,
		null business_name_en,
		null business_name_adding_pl,
		null business_name_adding_en,
		null offer_name_adding_pl,
		null offer_name_adding_en,
		null seek_name_adding_pl,
		null seek_name_adding_en,
		premoderated flg_premoderated,
		display_order,
		offer_seek flg_offer_seek,
		private_business flg_private_business,
		remove_companies flg_remove_companies,
		max_photos,
		extend_days,
		null default_currency,
		null filter_label_pl,
		null filter_label_en,
		search_category,
		search_args,
		search_routing_params,
		rmoderation_checkhistory flg_rmoderation_checkhistory,
		rmoderation_minprice rmoderation_min_price,
		rmoderation_hotkey,
		rmoderation_block_new_price flg_rmoderation_block_new_price,
		rmoderation_can_accept_automaticly flg_rmoderation_can_accept_automatically,
		null address_label_pl,
		null address_label_en,
		meta_category_id cod_category_meta, -- SERGIO ALERTA
		topads_count,
		default_view,
		default_view_mobile default_mobile_view,
		related_categories,
		hint_description,
		show_map flg_show_map,
		title_parameters,
		for_sale_category flg_for_sale_category,
		is_prioritized flg_prioritized,
		null default_price_type,
		use_name_in_solr flg_use_name_in_solr,
		null flg_allow_exchange,
		null legacy_code,
		null dsc_category_long_pl,
		null dsc_category_long_en,
		null dsc_category_singular_pl,
		null dsc_category_singular_en,
		null dsc_category_singular_pt, -- não existe de todo
		null title_format,
		null flg_has_free_text_search,
		null title_description_format,
		path_params,
		null seek_name_adding_pt,
		null private_name_pt,
		null private_name_adding_pt,
		null offer_name_pt,
		null offer_name_adding_pt,
		null name_pt,
		null dsc_category_long_pt,
		null filter_label_pt,
		null business_name_pt,
		null business_name_adding_pt,
		null address_label_pt,
		null short_name_with_pronoun_pt,
		null short_name_with_pronoun_en,
		null short_name_with_pronoun_ro, -- não existe de todo
		null short_name_with_pronoun_hi, -- não existe de todo
		null short_name_pt,
		null short_name_en,
		null short_name_ro, -- não existe de todo
		null short_name_hi, -- não existe de todo
		null seek_name_pt,
		null genitive_name_pt,
		null genitive_name_en,
		null genitive_name_ro, -- não existe de todo
		null genitive_name_ru, -- não existe de todo
		null genitive_name_hi, -- não existe de todo
		null genitive_name_uk, -- não existe de todo
		null dsc_category_singular_ro,
		null dsc_category_singular_ru, -- não existe de todo
		null dsc_category_singular_uk, -- não existe de todo
		seek_name_ro,
		seek_name_adding_ro,
		private_name_ro,
		private_name_adding_ro,
		offer_name_ro,
		offer_name_adding_ro,
		name_ro,
		null dsc_category_long_ro,
		null dsc_category_long_ru, -- não existe de todo
		null dsc_category_long_uk, -- não existe de todo
		filter_label_ro,
		business_name_ro,
		business_name_adding_ro,
		address_label_ro,
		null genitive_name_pl,
		null short_name_pl,
		null short_name_with_pronoun_pl,
		null address_label_ru,
		null address_label_uk,
		null business_name_adding_ru,
		null business_name_adding_uk,
		null business_name_ru,
		null business_name_uk,
		null filter_label_ru,
		null filter_label_uk,
		null name_ru,
		null name_uk,
		null offer_name_adding_ru,
		null offer_name_adding_uk,
		null offer_name_ru,
		null offer_name_uk,
		null private_name_adding_ru,
		null private_name_adding_uk,
		null private_name_ru,
		null private_name_uk,
		null seek_name_adding_ru,
		null seek_name_adding_uk,
		null seek_name_ru,
		null seek_name_uk,
		null short_name_ru,
		null short_name_uk,
		null short_name_with_pronoun_ru,
		null short_name_with_pronoun_uk,
		rmoderation_maxprice rmoderation_max_price,
		null address_label_hi,
		null business_name_adding_hi,
		null business_name_hi,
		null filter_label_hi,
		null dsc_category_long_hi,
		null name_hi,
		null offer_name_adding_hi,
		null offer_name_hi,
		null private_name_adding_hi,
		null private_name_hi,
		null seek_name_adding_hi,
		null seek_name_hi,
		null dsc_category_singular_hi,
		is_for_sale flg_for_sale,
		paidlimits_packet_id,
		free_ads_limit,
		null app_ads_fb,
		null app_ads_admob,
		null app_ads_admob_ad_campaign,
		null app_ads_admob_ads_campaign,
		parent_level1,
		parent_level2,
		is_leaf flg_leaf,
		scai_execution.cod_execution
	from
		crm_integration_stg.stg_ro_db_atlas_olxro_categories,
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
          and proc.dsc_process_short = 't_lkp_category'
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
    (select coalesce(max(cod_category),0) max_cod from crm_integration_anlt.t_lkp_category) max_cod_category,
    crm_integration_anlt.t_lkp_category target
  where
    coalesce(source_table.opr_category,-1) = target.opr_category(+)
	  and source_table.cod_source_system = target.cod_source_system (+)
    and target.valid_to(+) = 20991231;

analyze crm_integration_anlt.tmp_ro_load_category;
	
	--$$$
	
delete from crm_integration_anlt.t_lkp_category
using crm_integration_anlt.tmp_ro_load_category
where 
	tmp_ro_load_category.dml_type = 'I' 
	and t_lkp_category.opr_category = tmp_ro_load_category.opr_category
	and t_lkp_category.valid_from = (select dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_category');

	--$$$
	
update crm_integration_anlt.t_lkp_category
set valid_to = (select rel_integr_proc.dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_category') 
from crm_integration_anlt.tmp_ro_load_category source
where source.cod_category = crm_integration_anlt.t_lkp_category.cod_category
and crm_integration_anlt.t_lkp_category.valid_to = 20991231
and source.dml_type in('U','D');

	--$$$
	
insert into crm_integration_anlt.t_lkp_category
    select
		case
			when dml_type = 'I' then max_cod + new_cod
			when dml_type = 'U' then cod_category
		end cod_category,
		opr_category,
		dsc_category_pt,
		dsc_category_pl,
		dsc_category_en,
		dsc_category_ro,
		dsc_category_ru,
		dsc_category_hi,
		dsc_category_uk,
		(select rel_integr_proc.dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_category') valid_from, 
		20991231 valid_to,
		cod_source_system,
		-2 cod_category_parent,
		category_code,
		offer_name_pt,
		offer_name_pl,
		offer_name_en,
		offer_name_ro,
		offer_name_ru,
		offer_name_hi,
		offer_name_uk,
		offer_name_adding_pt,
		offer_name_adding_pl,
		offer_name_adding_en,
		offer_name_adding_ro,
		offer_name_adding_ru,
		offer_name_adding_hi,
		offer_name_adding_uk,
		seek_name_pt,
		seek_name_pl,
		seek_name_en,
		seek_name_ro,
		seek_name_ru,
		seek_name_hi,
		seek_name_uk,
		seek_name_adding_pt,
		seek_name_adding_pl,
		seek_name_adding_en,
		seek_name_adding_ro,
		seek_name_adding_ru,
		seek_name_adding_hi,
		seek_name_adding_uk,
		private_name_pt,
		private_name_pl,
		private_name_en,
		private_name_ro,
		private_name_ru,
		private_name_hi,
		private_name_uk,
		private_name_adding_pt,
		private_name_adding_pl,
		private_name_adding_en,
		private_name_adding_ro,
		private_name_adding_ru,
		private_name_adding_hi,
		private_name_adding_uk,
		business_name_pt,
		business_name_pl,
		business_name_en,
		business_name_ro,
		business_name_ru,
		business_name_hi,
		business_name_uk,
		business_name_adding_pt,
		business_name_adding_pl,
		business_name_adding_en,
		business_name_adding_ro,
		business_name_adding_ru,
		business_name_adding_hi,
		business_name_adding_uk,
		flg_premoderated,
		flg_offer_seek,
		flg_private_business,
		flg_remove_companies,
		flg_rmoderation_checkhistory,
		flg_rmoderation_block_new_price,
		flg_rmoderation_can_accept_automatically,
		flg_show_map,
		flg_for_sale_category,
		flg_prioritized,
		flg_use_name_in_solr,
		flg_allow_exchange,
		flg_has_free_text_search,
		display_order,
		max_photos,
		extend_days,
		default_currency,
		filter_label_pt,
		filter_label_pl,
		filter_label_en,
		filter_label_ro,
		filter_label_ru,
		filter_label_hi,
		filter_label_uk,
		search_category,
		search_args,
		search_routing_params,
		rmoderation_min_price,
		rmoderation_max_price,
		rmoderation_hotkey,
		address_label_pt,
		address_label_pl,
		address_label_en,
		address_label_ro,
		address_label_ru,
		address_label_hi,
		address_label_uk,
		cod_category_meta,
		topads_count,
		default_view,
		default_mobile_view,
		related_categories,
		hint_description,
		title_parameters,
		default_price_type,
		legacy_code,
		dsc_category_long_pt,
		dsc_category_long_pl,
		dsc_category_long_en,
		dsc_category_long_ro,
		dsc_category_long_ru,
		dsc_category_long_hi,
		dsc_category_long_uk,
		dsc_category_singular_pt,
		dsc_category_singular_pl,
		dsc_category_singular_en,
		dsc_category_singular_ro,
		dsc_category_singular_ru,
		dsc_category_singular_hi,
		dsc_category_singular_uk,
		title_format,
		title_format_description,
		path_params,
		short_name_pt,
		short_name_pl,
		short_name_en,
		short_name_ro,
		short_name_ru,
		short_name_hi,
		short_name_uk,
		short_name_with_pronoun_pt,
		short_name_with_pronoun_pl,
		short_name_with_pronoun_en,
		short_name_with_pronoun_ro,
		short_name_with_pronoun_ru,
		short_name_with_pronoun_hi,
		short_name_with_pronoun_uk,
		genitive_name_pt,
		genitive_name_pl,
		genitive_name_en,
		genitive_name_ro,
		genitive_name_ru,
		genitive_name_hi,
		genitive_name_uk,
		cast(flg_for_sale as bigint) flg_for_sale,
		cast(paidlimits_packet_id as bigint) paidlimits_packet_id,
		cast(free_ads_limit as bigint) free_ads_limit,
		cast(app_ads_fb as bigint) app_ads_fb,
		cast(app_ads_admob as bigint) app_ads_admob,
		cast(app_ads_admob_ad_campaign as varchar) app_ads_admob_ad_campaign,
		cast(app_ads_admob_ads_campaign as varchar) app_ads_admob_ads_campaign,
		cast(parent_level1 as bigint) parent_level1,
		cast(parent_level2 as bigint) parent_level2,
		cast(flg_leaf as bigint) flg_leaf,
		hash_category,
		cod_execution
    from
      crm_integration_anlt.tmp_ro_load_category
    where
      dml_type in ('U','I');

analyze crm_integration_anlt.t_lkp_category;
	  
	--$$$
	
update crm_integration_anlt.t_lkp_category
set cod_category_parent = lkp.cod_category
from crm_integration_anlt.tmp_ro_load_category source, crm_integration_anlt.t_lkp_category lkp, crm_integration_anlt.t_lkp_source_system ss
where coalesce(source.opr_category_parent,-1) = lkp.opr_category
and source.cod_source_system = ss.cod_source_system
and ss.cod_country = 4
and lkp.cod_category_parent = -2
and lkp.valid_to = 20991231 ;

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
	and proc.dsc_process_short = 't_lkp_category';

-- #######################
-- ####    PASSO 6    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set cod_status = 1, -- Ok
last_processing_datetime = coalesce((select max(operation_timestamp) from crm_integration_anlt.tmp_ro_load_category),last_processing_datetime)
/*from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc, crm_integration_anlt.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_category'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 4
  ) source*/
from crm_integration_anlt.t_lkp_scai_process proc 
where t_rel_scai_integration_process.cod_process = proc.cod_process
and t_rel_scai_integration_process.cod_status = 2
and t_rel_scai_integration_process.cod_country = 4
and proc.dsc_process_short = 't_lkp_category'
and t_rel_scai_integration_process.ind_active = 1
/*crm_integration_anlt.t_rel_scai_integration_process.cod_process = source.cod_process
and crm_integration_anlt.t_rel_scai_integration_process.cod_country = source.cod_country
and crm_integration_anlt.t_rel_scai_integration_process.cod_integration = source.cod_integration*/;
	  
drop table if exists crm_integration_anlt.tmp_ro_load_category;

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

drop table if exists crm_integration_anlt.tmp_ro_load_paidad_index;

create table crm_integration_anlt.tmp_ro_load_paidad_index 
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
    crm_integration_anlt.t_lkp_paidad_index target
  where
    coalesce(source_table.opr_paidad_index,-1) = target.opr_paidad_index(+)
	and source_table.cod_source_system = target.cod_source_system (+)
    and target.valid_to(+) = 20991231
    and coalesce(source_table.opr_paidad_index_type,'Unknown') = lkp_paidad_index_type.opr_paidad_index_type
	and source_table.cod_source_system = lkp_paidad_index_type.cod_source_system -- new
	and lkp_paidad_index_type.valid_to = 20991231;

analyze crm_integration_anlt.tmp_ro_load_paidad_index;
	
	--$$$
	
delete from crm_integration_anlt.t_lkp_paidad_index
using crm_integration_anlt.tmp_ro_load_paidad_index
where 
	tmp_ro_load_paidad_index.dml_type = 'I' 
	and t_lkp_paidad_index.opr_paidad_index = tmp_ro_load_paidad_index.opr_paidad_index
	and t_lkp_paidad_index.valid_from = (select dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_paidad_index');

	--$$$
	
update crm_integration_anlt.t_lkp_paidad_index
set valid_to = (select rel_integr_proc.dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_paidad_index') 
from crm_integration_anlt.tmp_ro_load_paidad_index source
where source.cod_paidad_index = crm_integration_anlt.t_lkp_paidad_index.cod_paidad_index
and crm_integration_anlt.t_lkp_paidad_index.valid_to = 20991231
and source.dml_type in('U','D');

	--$$$
	
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
      cod_source_system,
      hash_paidad_index,
	  flg_aut_deal_exclude,
	  cod_execution
    from
      crm_integration_anlt.tmp_ro_load_paidad_index
    where
      dml_type in ('U','I');

analyze crm_integration_anlt.t_lkp_paidad_index;
	  
-- New -> Lookup to itself

	--$$$
	
update crm_integration_anlt.t_lkp_paidad_index
set cod_paidad_index_related = lkp.cod_paidad_index
from crm_integration_anlt.tmp_ro_load_paidad_index source, crm_integration_anlt.t_lkp_paidad_index lkp, crm_integration_anlt.t_lkp_source_system ss
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

-- #######################
-- ####    PASSO 6    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set cod_status = 1, -- Ok
last_processing_datetime = coalesce((select max(operation_timestamp) from crm_integration_anlt.tmp_ro_load_paidad_index),last_processing_datetime)
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

drop table if exists crm_integration_anlt.tmp_ro_load_paidad_index;

	--$$$ -- 100

-- #######################
-- ####    PASSO 3    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set dat_processing = source.dat_processing, execution_nbr = source.execution_nbr, cod_status = 2 -- Running
from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc, crm_integration_anlt.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_region'
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
	and proc.dsc_process_short = 't_lkp_region';	

	--$$$
	
-- #############################################
-- # 		        ATLAS - GERAL              #
-- #	       LOADING t_lkp_region     	   #
-- #############################################

drop table if exists crm_integration_anlt.tmp_ro_load_region;

create table crm_integration_anlt.tmp_ro_load_region 
distkey(cod_source_system)
sortkey(cod_region, opr_region)
as
  select
    source_table.opr_region,
    source_table.dsc_region_pt,
    source_table.dsc_region_en,
    source_table.dsc_region_pl,
    source_table.dsc_region_ro,
    source_table.dsc_region_ru,
    source_table.dsc_region_hi,
    source_table.opr_source_system,
	source_table.operation_timestamp,
    source_table.code,
    source_table.domain,
    source_table.lon,
    source_table.lat,
    source_table.seo_weight,
    source_table.zoom,
    source_table.locative_pt,
    source_table.locative_en,
    source_table.locative_pl,
    source_table.locative_ro,
    source_table.locative_ru,
    source_table.locative_hi,
    source_table.possessive_pt,
    source_table.possessive_en,
    source_table.possessive_pl,
    source_table.possessive_ro,
    source_table.possessive_ru,
    source_table.possessive_hi,
    source_table.search_combo_label_pt,
    source_table.search_combo_label_en,
    source_table.search_combo_label_pl,
    source_table.search_combo_label_ro,
    source_table.search_combo_label_ru,
    source_table.search_combo_label_hi,
    source_table.aliases_pt,
    source_table.aliases_en,
    source_table.aliases_pl,
    source_table.aliases_ro,
    source_table.aliases_ru,
    source_table.aliases_hi,
    source_table.country_id,
    source_table.hash_region,
    source_table.cod_source_system,
    source_table.cod_execution,
    max_cod_region.max_cod,
    row_number() over (order by source_table.opr_region desc) new_cod,
    target.cod_region,
    case
      --when target.cod_region is null then 'I'
      when target.cod_region is null or (source_table.hash_region != target.hash_region and target.valid_from = (select dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_region')) then 'I'
	  when source_table.operation_type = 'delete' then 'D'
      when source_table.hash_region != target.hash_region then 'U'
        else 'X'
    end dml_type
  from
    (
	select
		source.*,
		lkp_source_system.cod_source_system,
		md5(coalesce(dsc_region_pt,'') + coalesce(dsc_region_en,'') + coalesce(dsc_region_pl,'') + coalesce(dsc_region_ro,'') + coalesce(dsc_region_ru,'') + coalesce(dsc_region_hi,'')
                  + coalesce(code,'') + coalesce(domain,'')
                  + cast(coalesce(lon,0) as varchar) + cast(coalesce(lat,0) as varchar) + coalesce(seo_weight,0) + coalesce(zoom,0) + coalesce(locative_pt,'') + coalesce(locative_en,'')
                  + coalesce(locative_pl,'') + coalesce(locative_ro,'') + coalesce(locative_ru,'') + coalesce(locative_hi,'') + coalesce(possessive_pt,'') + coalesce(possessive_en,'')
                  + coalesce(possessive_pl,'') + coalesce(possessive_ro,'') + coalesce(possessive_ru,'') + coalesce(possessive_hi,'') + coalesce(search_combo_label_pt,'')
                  + coalesce(search_combo_label_en,'') + coalesce(search_combo_label_pl,'') + coalesce(search_combo_label_ro,'') + coalesce(search_combo_label_ru,'')
                  + coalesce(search_combo_label_hi,'') + coalesce(aliases_pt,'') + coalesce(aliases_en,'') + coalesce(aliases_pl,'') + coalesce(aliases_ro,'') + coalesce(aliases_ru,'')
                  + coalesce(aliases_hi,'') + coalesce(country_id,0)
              ) hash_region
	from
	(
      SELECT
        id opr_region,
        name_pt dsc_region_pt,
        name_en dsc_region_en,
        name_pl dsc_region_pl,
        name_ro dsc_region_ro,
        name_ru dsc_region_ru,
        name_hi dsc_region_hi,
        livesync_dbname opr_source_system,
		    operation_timestamp,
		    operation_type,
        code,
        domain,
        lon,
        lat,
        seo_weight,
        zoom,
        locative_pt,
        locative_en,
        locative_pl,
        locative_ro,
        locative_ru,
        locative_hi,
        possessive_pt,
        possessive_en,
        possessive_pl,
        possessive_ro,
        possessive_ru,
        possessive_hi,
        search_combo_label_pt,
        search_combo_label_en,
        search_combo_label_pl,
        search_combo_label_ro,
        search_combo_label_ru,
        search_combo_label_hi,
        aliases_pt,
        aliases_en,
        aliases_pl,
        aliases_ro,
        aliases_ru,
        aliases_hi,
        country_id,
        scai_execution.cod_execution
      FROM
        crm_integration_stg.stg_ro_db_atlas_verticals_regions a,
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
            and proc.dsc_process_short = 't_lkp_region'
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
		id opr_region,
        null dsc_region_pt,
        null dsc_region_en,
        null dsc_region_pl,
        name_ro dsc_region_ro,
        null dsc_region_ru,
        null dsc_region_hi,
        'olxro' opr_source_system,
		    operation_timestamp,
		    operation_type,
        code,
        domain,
        lon,
        lat,
        seo_weight,
        zoom,
        null locative_pt,
        null locative_en,
        null locative_pl,
        locative_ro,
        null locative_ru,
        null locative_hi,
        null possessive_pt,
        null possessive_en,
        null possessive_pl,
        possessive_ro,
        null possessive_ru,
        null possessive_hi,
        null search_combo_label_pt,
        null search_combo_label_en,
        null search_combo_label_pl,
        search_combo_label_ro,
        null search_combo_label_ru,
        null search_combo_label_hi,
        null aliases_pt,
        null aliases_en,
        null aliases_pl,
        aliases_ro,
        null aliases_ru,
        null aliases_hi,
        null country_id,
        scai_execution.cod_execution
	  from
		crm_integration_stg.stg_ro_db_atlas_olxro_regions,
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
          and proc.dsc_process_short = 't_lkp_region'
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
    (select coalesce(max(cod_region),0) max_cod from crm_integration_anlt.t_lkp_region) max_cod_region,
    crm_integration_anlt.t_lkp_region target
  where
    coalesce(source_table.opr_region,-1) = target.opr_region(+)
	and source_table.cod_source_system = target.cod_source_system (+)
    and target.valid_to(+) = 20991231;

analyze crm_integration_anlt.tmp_ro_load_region;
	
	--$$$

delete from crm_integration_anlt.t_lkp_region
using crm_integration_anlt.tmp_ro_load_region
where 
	tmp_ro_load_region.dml_type = 'I' 
	and t_lkp_region.opr_region = tmp_ro_load_region.opr_region
	and t_lkp_region.valid_from = (select dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_region');

	--$$$

update crm_integration_anlt.t_lkp_region
set valid_to = (select rel_integr_proc.dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_region') 
from crm_integration_anlt.tmp_ro_load_region source
where source.cod_region = crm_integration_anlt.t_lkp_region.cod_region
and crm_integration_anlt.t_lkp_region.valid_to = 20991231
and source.dml_type = 'U';

	--$$$

insert into crm_integration_anlt.t_lkp_region
    select
      case
        when dml_type = 'I' then max_cod + new_cod
        when dml_type = 'U' then cod_region
      end cod_region,
      opr_region,
      dsc_region_pt,
      dsc_region_en,
      dsc_region_pl,
      dsc_region_ro,
      dsc_region_ru,
      dsc_region_hi,
      (select rel_integr_proc.dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_region') valid_from, 
      20991231 valid_to,
      cod_source_system,
      code,
      domain,
      lon,
      lat,
      seo_weight,
      zoom,
      locative_pt,
      locative_en,
      locative_pl,
      locative_ro,
      locative_ru,
      locative_hi,
      possessive_pt,
      possessive_en,
      possessive_pl,
      possessive_ro,
      possessive_ru,
      possessive_hi,
      search_combo_label_pt,
      search_combo_label_en,
      search_combo_label_pl,
      search_combo_label_ro,
      search_combo_label_ru,
      search_combo_label_hi,
      aliases_pt,
      aliases_en,
      aliases_pl,
      aliases_ro,
      aliases_ru,
      aliases_hi,
      country_id,
      hash_region,
	  cod_execution
    from
      crm_integration_anlt.tmp_ro_load_region
    where
      dml_type in ('U','I');

analyze crm_integration_anlt.t_lkp_region;
	  
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
	and proc.dsc_process_short = 't_lkp_region';

-- #######################
-- ####    PASSO 6    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set cod_status = 1, -- Ok
last_processing_datetime = coalesce((select max(operation_timestamp) from crm_integration_anlt.tmp_ro_load_region),last_processing_datetime)
/*from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc, crm_integration_anlt.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_region'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 4
  ) source*/
from crm_integration_anlt.t_lkp_scai_process proc 
where t_rel_scai_integration_process.cod_process = proc.cod_process
and t_rel_scai_integration_process.cod_status = 2
and t_rel_scai_integration_process.cod_country = 4
and proc.dsc_process_short = 't_lkp_region'
and t_rel_scai_integration_process.ind_active = 1
/*crm_integration_anlt.t_rel_scai_integration_process.cod_process = source.cod_process
and crm_integration_anlt.t_rel_scai_integration_process.cod_country = source.cod_country
and crm_integration_anlt.t_rel_scai_integration_process.cod_integration = source.cod_integration*/;

drop table if exists crm_integration_anlt.tmp_ro_load_region;

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
    where proc.dsc_process_short = 't_lkp_subregion'
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
	and proc.dsc_process_short = 't_lkp_subregion';	

	--$$$
	
-- #############################################
-- # 		        ATLAS - GERAL              #
-- #	      LOADING t_lkp_subregion     	   #
-- #############################################

drop table if exists crm_integration_anlt.tmp_ro_load_subregion;

create table crm_integration_anlt.tmp_ro_load_subregion 
distkey(cod_source_system)
sortkey(cod_subregion, opr_subregion)
as
  select
    source_table.opr_subregion,
    source_table.dsc_subregion_pt,
    source_table.dsc_subregion_en,
    source_table.dsc_subregion_pl,
    source_table.dsc_subregion_ro,
    source_table.dsc_subregion_ru,
    source_table.dsc_subregion_uk,
	source_table.operation_timestamp,
    source_table.code,
    source_table.opr_region,
    source_table.dsc_subregion_normalized_pt,
    source_table.dsc_subregion_normalized_en,
    source_table.dsc_subregion_normalized_pl,
    source_table.dsc_subregion_normalized_ru,
    source_table.dsc_subregion_normalized_uk,
    source_table.lon,
    source_table.lat,
    source_table.seo_weight,
    source_table.zoom,
    source_table.locative_pt,
    source_table.locative_en,
    source_table.locative_pl,
    source_table.locative_ro,
    source_table.locative_ru,
    source_table.locative_hi,
    source_table.locative_uk,
    source_table.url_code,
    source_table.possessive_pt,
    source_table.possessive_en,
    source_table.possessive_pl,
    source_table.possessive_ro,
    source_table.possessive_ru,
    source_table.possessive_hi,
    source_table.possessive_uk,
    source_table.price_group,
    source_table.display_order,
    source_table.flg_urban,
    source_table.external_id,
    source_table.hash_subregion,
    coalesce(lkp_region.cod_region,-2) cod_region,
    source_table.cod_source_system,
    source_table.cod_execution,
    max_cod_subregion.max_cod,
    row_number() over (order by source_table.opr_subregion desc) new_cod,
    target.cod_subregion,
    case
      --when target.cod_subregion is null then 'I'
      when target.cod_subregion is null or (source_table.hash_subregion != target.hash_subregion and target.valid_from = (select dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_subregion')) then 'I'
	  when source_table.operation_type = 'delete' then 'D'
      when source_table.hash_subregion != target.hash_subregion then 'U'
        else 'X'
    end dml_type
  from
    (
	select
		*,
		md5(coalesce(dsc_subregion_pt,'') + coalesce(dsc_subregion_en,'') + coalesce(dsc_subregion_pl,'') + coalesce(dsc_subregion_ro,'') + coalesce(dsc_subregion_ru,'') + coalesce(dsc_subregion_hi,'') + coalesce(dsc_subregion_uk,'')
            + coalesce(code,'') + coalesce(opr_region,0) + coalesce(dsc_subregion_normalized_pt,'') + coalesce(dsc_subregion_normalized_en,'') + coalesce(dsc_subregion_normalized_pl,'')
            + coalesce(dsc_subregion_normalized_ru,'') + coalesce(dsc_subregion_normalized_uk,'') + cast(coalesce(lon,0) as varchar) + cast(coalesce(lat,0) as varchar) + coalesce(seo_weight,0)
            + coalesce(zoom,0) + coalesce(locative_pt,'') + coalesce(locative_en,'') + coalesce(locative_pl,'') + coalesce(locative_ro,'') + coalesce(locative_ru,'')
            + coalesce(locative_hi,'') + coalesce(locative_hi,'') + coalesce(locative_uk,'') + coalesce(url_code,'') + coalesce(possessive_pt,'') + coalesce(possessive_en,'')
            + coalesce(possessive_pl,'') + coalesce(possessive_ro,'') + coalesce(possessive_ru,'') + coalesce(possessive_hi,'') + coalesce(possessive_uk,'') + coalesce(price_group,'')
           + coalesce(display_order,0) + coalesce(flg_urban,0) + coalesce(external_id,'')) hash_subregion
	from
	(
      SELECT
        id opr_subregion,
        name_pt dsc_subregion_pt,
        name_transliterated dsc_subregion_en,
        name_pl dsc_subregion_pl,
        name_ro dsc_subregion_ro,
        name_ru dsc_subregion_ru,
		    name_hi dsc_subregion_hi,
        name_uk dsc_subregion_uk,
        livesync_dbname opr_source_system,
		    operation_timestamp,
		    operation_type,
        code,
        region_id opr_region,
        name_normalized_pt dsc_subregion_normalized_pt,
        name_normalized_en dsc_subregion_normalized_en,
        name_normalized_pl dsc_subregion_normalized_pl,
        name_normalized_ru dsc_subregion_normalized_ru,
        name_normalized_uk dsc_subregion_normalized_uk,
        lon,
        lat,
        seo_weight,
        zoom,
        locative_pt,
        locative_en,
        locative_pl,
        locative_ro,
        locative_ru,
        locative_hi,
        locative_uk,
        url_code,
        possessive_pt,
        possessive_en,
        possessive_pl,
        possessive_ro,
        possessive_ru,
        possessive_hi,
        possessive_uk,
        price_group,
        display_order,
        urban flg_urban,
        external_id,
        cod_source_system,
        scai_execution.cod_execution
      FROM
        crm_integration_stg.stg_ro_db_atlas_verticals_subregions a,
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
            and proc.dsc_process_short = 't_lkp_subregion'
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
	)
    ) source_table,
    crm_integration_anlt.t_lkp_region lkp_region,
    (select coalesce(max(cod_subregion),0) max_cod from crm_integration_anlt.t_lkp_subregion) max_cod_subregion,
    crm_integration_anlt.t_lkp_subregion target
  where
    coalesce(source_table.opr_subregion,-1) = target.opr_subregion(+)
	and source_table.cod_source_system = target.cod_source_system (+)
    and target.valid_to(+) = 20991231
    and coalesce(source_table.opr_region,-1) = lkp_region.opr_region
	and source_table.cod_source_system = lkp_region.cod_source_system -- new
	and lkp_region.valid_to = 20991231;

analyze crm_integration_anlt.tmp_ro_load_subregion;
	
	--$$$
	
delete from crm_integration_anlt.t_lkp_subregion
using crm_integration_anlt.tmp_ro_load_subregion
where 
	tmp_ro_load_subregion.dml_type = 'I' 
	and t_lkp_subregion.opr_subregion = tmp_ro_load_subregion.opr_subregion
	and t_lkp_subregion.valid_from = (select dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_subregion');

	--$$$
	
update crm_integration_anlt.t_lkp_subregion
set valid_to = (select rel_integr_proc.dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_subregion') 
from crm_integration_anlt.tmp_ro_load_subregion source
where source.cod_subregion = crm_integration_anlt.t_lkp_subregion.cod_subregion
and crm_integration_anlt.t_lkp_subregion.valid_to = 20991231
and source.dml_type = 'U';

	--$$$
	
insert into crm_integration_anlt.t_lkp_subregion
    select
      case
        when dml_type = 'I' then max_cod + new_cod
        when dml_type = 'U' then cod_subregion
      end cod_subregion,
      opr_subregion,
      dsc_subregion_pt,
      dsc_subregion_en,
      dsc_subregion_pl,
      dsc_subregion_ro,
      dsc_subregion_ru,
      null dsc_subregion_hi,
      dsc_subregion_uk,
      (select rel_integr_proc.dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_subregion') valid_from, 
      20991231 valid_to,
      cod_region,
      cod_source_system,
      dsc_subregion_normalized_pt,
      dsc_subregion_normalized_en,
      dsc_subregion_normalized_pl,
      null dsc_subregion_normalized_ro,
      dsc_subregion_normalized_ru,
      null dsc_subregion_normalized_hi,
      dsc_subregion_normalized_uk,
      code,
      lon,
      lat,
      seo_weight,
      zoom,
      locative_pt,
      locative_en,
      locative_pl,
      locative_ro,
      locative_ru,
      locative_hi,
      locative_uk,
      url_code,
      possessive_pt,
      possessive_en,
      possessive_pl,
      possessive_ro,
      possessive_ru,
      possessive_hi,
      possessive_uk,
      price_group,
      display_order,
      flg_urban,
      external_id,
      hash_subregion,
	  cod_execution
    from
      crm_integration_anlt.tmp_ro_load_subregion
    where
      dml_type in ('U','I');

analyze crm_integration_anlt.t_lkp_subregion;
	  
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
	and proc.dsc_process_short = 't_lkp_subregion';

-- #######################
-- ####    PASSO 6    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set cod_status = 1, -- Ok
last_processing_datetime = coalesce((select max(operation_timestamp) from crm_integration_anlt.tmp_ro_load_subregion),last_processing_datetime)
/*from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc, crm_integration_anlt.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_subregion'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 4
  ) source*/
from crm_integration_anlt.t_lkp_scai_process proc 
where t_rel_scai_integration_process.cod_process = proc.cod_process
and t_rel_scai_integration_process.cod_status = 2
and t_rel_scai_integration_process.cod_country = 4
and proc.dsc_process_short = 't_lkp_subregion'
and t_rel_scai_integration_process.ind_active = 1
/*crm_integration_anlt.t_rel_scai_integration_process.cod_process = source.cod_process
and crm_integration_anlt.t_rel_scai_integration_process.cod_country = source.cod_country
and crm_integration_anlt.t_rel_scai_integration_process.cod_integration = source.cod_integration*/;

drop table if exists crm_integration_anlt.tmp_ro_load_subregion;

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
    where proc.dsc_process_short = 't_lkp_city'
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
	and proc.dsc_process_short = 't_lkp_city';	

	--$$$
	
-- #############################################
-- # 		        ATLAS - GERAL              #
-- #	        LOADING t_lkp_city       	   #
-- #############################################

drop table if exists crm_integration_anlt.tmp_ro_load_city;

create table crm_integration_anlt.tmp_ro_load_city 
distkey(cod_source_system)
sortkey(cod_city, opr_city)
as
  select
    source_table.opr_city,
    source_table.dsc_city_pl,
    source_table.dsc_city_en,
	source_table.operation_timestamp,
    source_table.url,
    source_table.county,
    source_table.municipality,
    source_table.flg_unique,
    source_table.zip,
    source_table.city_id,
    source_table.lat,
    source_table.lon,
    source_table.zoom,
    source_table.citizens_count,
    source_table.citizens_weight,
    source_table.flg_main,
    source_table.flg_import_approximation,
    source_table.flg_show_on_mainpage,
    source_table.radius,
    source_table.polygon,
    source_table.group_id,
    source_table.external_id,
    source_table.external_type,
    source_table.dsc_city_normalized_pl,
    source_table.hash_city,
    source_table.cod_subregion,
	source_table.cod_region,
    source_table.cod_source_system,
    source_table.cod_execution,
    max_cod_city.max_cod,
    row_number() over (order by source_table.opr_city desc) new_cod,
    target.cod_city,
    case
      --when target.cod_city is null then 'I'
      when target.cod_city is null or (source_table.hash_city != target.hash_city and target.valid_from = (select dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_city')) then 'I'
	  when source_table.operation_type = 'delete' then 'D'
      when source_table.hash_city != target.hash_city then 'U'
        else 'X'
    end dml_type
  from
    (
	select
		*,
		md5(
		coalesce(dsc_city_pl,'') +
		coalesce(dsc_city_en,'') +
		coalesce(url,'') +
		coalesce(county,'') +
		coalesce(municipality,'') +
		coalesce(flg_unique,0) +
		coalesce(zip,'') +
		coalesce(city_id,0) +
		cast(coalesce(lat,0) as varchar) +
		cast(coalesce(lon,0) as varchar) +
		coalesce(zoom,0) +
		coalesce(citizens_count,0) +
		coalesce(citizens_weight,0) +
		coalesce(opr_region,0) +
		coalesce(opr_subregion,0) +
		coalesce(flg_main,0) +
		coalesce(flg_import_approximation,0) +
		coalesce(flg_show_on_mainpage,0) +
		cast(coalesce(radius,0) as varchar) +
		coalesce(polygon,'') +
		coalesce(group_id,0) +
		coalesce(external_id,'') +
		coalesce(external_type,'') +
		coalesce(dsc_city_normalized_pl,'')
        ) hash_city
	from
	(
  SELECT
		a.id opr_city,
		a.livesync_dbname opr_source_system,
		a.operation_timestamp,
		a.operation_type,
		a.name_pl dsc_city_pl,
		a.name_en dsc_city_en,
		a.url,
		a.county,
		a.municipality,
		a.is_unique flg_unique,
		a.zip,
		a.city_id,
		a.lat,
		a.lon,
		a.zoom,
		a.citizens_count,
		a.citizens_weight,
		d.cod_region,
		a.region_id opr_region,
		c.cod_subregion,
		a.subregion_id opr_subregion,
		a.main flg_main,
		a.import_approximation flg_import_approximation,
		a.show_on_mainpage flg_show_on_mainpage,
		a.radius,
		a.polygon,
		a.group_id,
		a.external_id,
		a.external_type,
		a.name_normalized_pl dsc_city_normalized_pl,
		b.cod_source_system,
		scai_execution.cod_execution
      FROM
        crm_integration_stg.stg_ro_db_atlas_verticals_cities a,
        crm_integration_anlt.t_lkp_source_system b,
		    crm_integration_anlt.t_lkp_subregion c,
		    crm_integration_anlt.t_lkp_region d,
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
            and proc.dsc_process_short = 't_lkp_city'
            and fac.cod_process = rel_integr_proc.cod_process
            and fac.cod_integration = rel_integr_proc.cod_integration
            and rel_integr_proc.dat_processing = fac.dat_processing
            and fac.cod_status = 2
        ) scai_execution
    where
       a.livesync_dbname = b.opr_source_system
       and a.region_id = d.opr_region
       and d.cod_source_system = b.cod_source_system
       and a.subregion_id = c.opr_subregion
       and c.cod_source_system = b.cod_source_system
       and b.cod_business_type = 1 -- Verticals
       and b.cod_country = 4 -- Romania
       --and 1 = 0
	  union all
	  select
	    a.id opr_city,
		'olxro' opr_source_system,
		a.operation_timestamp,
		a.operation_type,
		null dsc_city_pl,
		a.name_ro dsc_city_en,
		a.url,
		a.county,
		a.municipality_ro municipality,
		a.is_unique flg_unique,
		a.zip,
		a.city_id,
		a.lat,
		a.lon,
		a.zoom,
		a.citizens_count,
		a.citizens_weight,
		c.cod_region,
		region_id opr_region,
		-1 cod_subregion,
		-1 opr_subregion,
		a.main flg_main,
		a.import_approximation flg_import_approximation,
		a.show_on_mainpage flg_show_on_mainpage,
		a.radius,
		a.polygon,
		a.group_id,
		null external_id,
		null external_type,
		null dsc_city_normalized_pl,
		b.cod_source_system,
		scai_execution.cod_execution
      FROM
        crm_integration_stg.stg_ro_db_atlas_olxro_cities a,
        crm_integration_anlt.t_lkp_source_system b,
	      crm_integration_anlt.t_lkp_region c,
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
            and proc.dsc_process_short = 't_lkp_city'
            and fac.cod_process = rel_integr_proc.cod_process
            and fac.cod_integration = rel_integr_proc.cod_integration
            and rel_integr_proc.dat_processing = fac.dat_processing
            and fac.cod_status = 2
        ) scai_execution
	where
		'olxro' = b.opr_source_system
		and a.region_id = c.opr_region
		and c.cod_source_system = b.cod_source_system
		--and 1 = 0
    )
	) source_table,
    (select coalesce(max(cod_city),0) max_cod from crm_integration_anlt.t_lkp_city) max_cod_city,
    crm_integration_anlt.t_lkp_city target
  where
    coalesce(source_table.opr_city,-1) = target.opr_city(+)
	and source_table.cod_source_system = target.cod_source_system (+)
    and target.valid_to(+) = 20991231;

analyze crm_integration_anlt.tmp_ro_load_city;
	
	--$$$
	
delete from crm_integration_anlt.t_lkp_city
using crm_integration_anlt.tmp_ro_load_city
where 
	tmp_ro_load_city.dml_type = 'I' 
	and t_lkp_city.opr_city = tmp_ro_load_city.opr_city
	and t_lkp_city.valid_from = (select dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_city');

	--$$$
	
update crm_integration_anlt.t_lkp_city
set valid_to = (select rel_integr_proc.dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_city') 
from crm_integration_anlt.tmp_ro_load_city source
where source.cod_city = crm_integration_anlt.t_lkp_city.cod_city
and crm_integration_anlt.t_lkp_city.valid_to = 20991231
and source.dml_type = 'U';

	--$$$
	
insert into crm_integration_anlt.t_lkp_city
    select
		case
			when dml_type = 'I' then max_cod + new_cod
			when dml_type = 'U' then cod_city
		end cod_city,
		opr_city,
		dsc_city_en,
		dsc_city_pl,
		(select rel_integr_proc.dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_city') valid_from, 
		20991231 valid_to,
		dsc_city_normalized_pl,
		cod_subregion,
		cod_region,
		cod_source_system,
		url,
		county,
		municipality,
		flg_unique,
		zip,
		lat,
		lon,
		zoom,
		citizens_count,
		citizens_weight,
		flg_main,
		flg_import_approximation,
		flg_show_on_mainpage,
		radius,
		polygon,
		group_id,
		external_id,
		external_type,
		hash_city,
		cod_execution
    from
      crm_integration_anlt.tmp_ro_load_city
    where
      dml_type in ('U','I');

analyze crm_integration_anlt.t_lkp_city;
	  
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
	and proc.dsc_process_short = 't_lkp_city';

-- #######################
-- ####    PASSO 6    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set cod_status = 1, -- Ok
last_processing_datetime = coalesce((select max(operation_timestamp) from crm_integration_anlt.tmp_ro_load_city),last_processing_datetime)
/*from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc, crm_integration_anlt.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_city'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 4
  ) source*/
from crm_integration_anlt.t_lkp_scai_process proc 
where t_rel_scai_integration_process.cod_process = proc.cod_process
and t_rel_scai_integration_process.cod_status = 2
and t_rel_scai_integration_process.cod_country = 4
and proc.dsc_process_short = 't_lkp_city'
and t_rel_scai_integration_process.ind_active = 1
/*crm_integration_anlt.t_rel_scai_integration_process.cod_process = source.cod_process
and crm_integration_anlt.t_rel_scai_integration_process.cod_country = source.cod_country
and crm_integration_anlt.t_rel_scai_integration_process.cod_integration = source.cod_integration*/;

drop table if exists crm_integration_anlt.tmp_ro_load_city;

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

drop table if exists crm_integration_anlt.tmp_ro_load_atlas_user;

create table crm_integration_anlt.tmp_ro_load_atlas_user 
distkey(cod_source_system)
sortkey(cod_atlas_user, opr_atlas_user)
as
select a.*, coalesce(b.cod_city,-2) cod_city from (
	select
    source_table.opr_atlas_user,
	source_table.dsc_atlas_user,
	source_table.email_original,
	source_table.password,
	source_table.autologin_rev,
	source_table.operation_type,
	source_table.operation_timestamp,
	source_table.type,
	source_table.created_at,
	source_table.last_login_at,
	source_table.default_lang,
	source_table.flg_newsletter,
	source_table.flg_use_offer_limits,
	source_table.ban_reason_id,
	source_table.flg_autocomplete_defaults,
	source_table.default_skype,
	source_table.default_phone,
	source_table.default_map_address,
	source_table.default_gg,
	source_table.default_person,
	--source_table.default_region_id,
	--source_table.default_subregion_id,
	source_table.default_lat,
	source_table.default_lng,
	source_table.default_zoom,
	--source_table.default_district_id,
	--lkp_city.cod_city,
	source_table.last_login_ip,
	source_table.last_login_port,
	source_table.fraudster,
	source_table.rmoderation_moderated_by,
	source_table.rmoderation_moderated_at,
	source_table.rmoderation_moderated_days,
	source_table.rmoderation_moderated_total,
	source_table.rmoderation_moderated_last,
	source_table.credits,
	source_table.flg_app,
	source_table.flg_android_app,
	source_table.flg_apple_app,
	source_table.flg_wp_app,
	source_table.flg_spammer,
	coalesce(lkp_source.cod_source,-2) cod_source,
	source_table.flg_hide_user_ads,
	source_table.flg_email_msg_notif,
	source_table.flg_email_alarms_notif,
	source_table.police_comment,
	source_table.police_bank_account,
	source_table.flg_monitored,
	source_table.flg_hide_bank_warning,
	source_table.flg_external_login,
	source_table.flg_business,
	source_table.flg_restricted,
	source_table.trusted_started_at,
	source_table.flg_trusted_accepted,
	source_table.migration_status,
	source_table.suspend_reason,
	source_table.password_method,
	source_table.default_person_first_name,
	source_table.default_person_last_name,
	source_table.default_postcode,
	source_table.last_modification_date,
	source_table.flg_autorenew,
	source_table.quality_score,
	source_table.first_app_login_at,
	source_table.flg_email_promo_notif,
	source_table.flg_email_expired_notif,
	source_table.disabled_export_clients,
	source_table.username_legacy,
	source_table.user_legacy_id,
	source_table.bonus_credits,
	source_table.bonus_credits_expire_at,
	source_table.hermes_dirty,
	source_table.flg_uses_crm,
	source_table.sms_verification_phone,
	source_table.sms_verification_status,
	source_table.sms_verification_code,
	source_table.opr_city,
    source_table.cod_source_system,
    source_table.hash_atlas_user,
	source_table.cod_execution,
    max_cod_atlas_user.max_cod,
    row_number() over (order by source_table.opr_atlas_user desc) new_cod,
    target.cod_atlas_user,
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
			coalesce(password                                            ,'') +
			coalesce(autologin_rev                                       ,0) +
			coalesce(type                                                ,'') +
			coalesce(created_at                                          ,'2099-12-31 00:00:00.000000') +
			coalesce(last_login_at                                       ,'2099-12-31 00:00:00.000000') +
			coalesce(default_lang                                        ,'') +
			coalesce(flg_newsletter                                      ,0) +
			coalesce(flg_use_offer_limits                                ,0) +
			coalesce(ban_reason_id                                       ,0) +
			coalesce(flg_autocomplete_defaults                           ,0) +
			coalesce(default_skype                                       ,'') +
			coalesce(default_phone                                       ,'') +
			coalesce(default_map_address                                 ,'') +
			coalesce(default_gg                                          ,'') +
			coalesce(default_person                                      ,'') +
			coalesce(opr_region                                   ,0) +
			coalesce(opr_subregion                                ,0) +
			cast(coalesce(default_lat                                    ,0) as varchar) +
			cast(coalesce(default_lng                                    ,0) as varchar) +
			coalesce(default_zoom                                        ,0) +
			coalesce(default_district_id                                 ,0) +
			coalesce(opr_city                                            ,0) +
			coalesce(last_login_ip                                       ,0) +
			coalesce(last_login_port                                     ,0) +
			coalesce(fraudster                                           ,0) +
			coalesce(rmoderation_moderated_by                            ,0) +
			--coalesce(rmoderation_moderated_at                            ,'2099-12-31 00:00:00.000000') +
			coalesce(rmoderation_moderated_days                          ,0) +
			coalesce(rmoderation_moderated_total                         ,0) +
			--coalesce(rmoderation_moderated_last                          ,'2099-12-31 00:00:00.000000') +
			cast(coalesce(credits                                        ,0) as varchar) +
			coalesce(flg_app                                             ,'0') +
			coalesce(flg_android_app                                     ,0) +
			coalesce(flg_apple_app                                       ,0) +
			coalesce(flg_wp_app                                          ,0) +
			coalesce(flg_spammer                                      ,0) +
			coalesce(opr_source                                          ,'') +
			coalesce(flg_hide_user_ads                                   ,0) +
			coalesce(flg_email_msg_notif                                 ,0) +
			coalesce(flg_email_alarms_notif                              ,0) +
			coalesce(police_comment                                      ,'') +
			coalesce(police_bank_account                                 ,'') +
			coalesce(flg_monitored                                    ,0) +
			coalesce(flg_hide_bank_warning                               ,0) +
			coalesce(flg_external_login                                  ,0) +
			coalesce(flg_business                                     ,0) +
			coalesce(flg_restricted                                   ,0) +
			--coalesce(trusted_started_at                                  ,'2099-12-31 00:00:00.000000') +
			coalesce(flg_trusted_accepted                                ,0) +
			coalesce(migration_status                                    ,'') +
			coalesce(suspend_reason                                      ,'') +
			coalesce(password_method                                     ,'') +
			coalesce(default_person_first_name                           ,'') +
			coalesce(default_person_last_name                            ,'') +
			coalesce(default_postcode                                    ,'') +
			--coalesce(last_modification_date                              ,'2099-12-31 00:00:00.000000') +
			coalesce(flg_autorenew                                       ,0) +
			cast(coalesce(quality_score                                  ,0) as varchar) +
			--coalesce(first_app_login_at                                  ,'2099-12-31 00:00:00.000000') +
			coalesce(flg_email_promo_notif                               ,0) +
			coalesce(flg_email_expired_notif                             ,0) +
			coalesce(disabled_export_clients                             ,'') +
			coalesce(username_legacy                                     ,'') +
			coalesce(user_legacy_id                                      ,0) +
			cast(coalesce(bonus_credits                                  ,0) as varchar) +
			--coalesce(bonus_credits_expire_at                             ,'2099-12-31 00:00:00.000000') +
			coalesce(hermes_dirty                                        ,0) +
			coalesce(flg_uses_crm                                        ,0) +
			coalesce(sms_verification_phone                              ,'') +
			coalesce(sms_verification_status                             ,'') +
			coalesce(sms_verification_code                               ,'')
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
		password,
		autologin_rev,
		type,
		created_at,
		last_login_at,
		default_lang,
		newsletter flg_newsletter,
		use_offer_limits flg_use_offer_limits,
		ban_reason_id,
		autocomplete_defaults flg_autocomplete_defaults,
		default_skype,
		default_phone,
		default_map_address,
		default_gg,
		default_person,
		default_region_id opr_region,
		default_subregion_id opr_subregion,
		default_lat,
		default_lng,
		default_zoom,
		default_district_id,
		default_city_id opr_city,
		last_login_ip,
		last_login_port,
		fraudster,
		rmoderation_moderated_by,
		rmoderation_moderated_at,
		rmoderation_moderated_days,
		rmoderation_moderated_total,
		rmoderation_moderated_last,
		credits,
		app flg_app,
		android_app flg_android_app,
		apple_app flg_apple_app,
		wp_app flg_wp_app,
		is_spammer flg_spammer,
		source opr_source,
		hide_user_ads flg_hide_user_ads,
		email_msg_notif flg_email_msg_notif,
		email_alarms_notif flg_email_alarms_notif,
		police_comment,
		police_bank_account,
		is_monitored flg_monitored,
		hide_bank_warning flg_hide_bank_warning,
		external_login flg_external_login,
		is_business flg_business,
		is_restricted flg_restricted,
		trusted_started_at,
		trusted_accepted flg_trusted_accepted,
		migration_status,
		suspend_reason,
		password_method,
		default_person_first_name,
		default_person_last_name,
		default_postcode,
		last_modification_date,
		autorenew flg_autorenew,
		quality_score,
		first_app_login_at,
		email_promo_notif flg_email_promo_notif,
		email_expired_notif flg_email_expired_notif,
		disabled_export_clients,
		username_legacy,
		user_legacy_id,
		cast(null as numeric(10,2)) bonus_credits,
		cast(null as timestamp) bonus_credits_expire_at,
		cast(null as bigint) hermes_dirty,
		cast(null as bigint) flg_uses_crm,
		cast(null as varchar) sms_verification_phone,
		cast(null as varchar) sms_verification_status,
		cast(null as varchar) sms_verification_code,
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
		null as password,
		autologin_rev,
		type,
		created_at,
		last_login_at,
		null default_lang,
		newsletter flg_newsletter,
		use_offer_limits flg_use_offer_limits,
		ban_reason_id,
		autocomplete_defaults flg_autocomplete_defaults,
		default_skype,
		default_phone,
		default_map_address,
		default_gg,
		default_person,
		default_region_id opr_region,
		default_subregion_id opr_subregion,
		default_lat,
		default_lng,
		default_zoom,
		default_district_id,
		default_city_id opr_city,
		last_login_ip,
		null last_login_port,
		fraudster,
		rmoderation_moderated_by,
		rmoderation_moderated_at,
		null rmoderation_moderated_days,
		null rmoderation_moderated_total,
		null rmoderation_moderated_last,
		credits,
		app flg_app,
		android_app flg_android_app,
		apple_app flg_apple_app,
		wp_app flg_wp_app,
		is_spammer flg_spammer,
		source opr_source,
		hide_user_ads flg_hide_user_ads,
		null flg_email_msg_notif,
		email_alarms_notif flg_email_alarms_notif,
		police_comment,
		police_bank_account,
		is_monitored flg_monitored,
		hide_bank_warning flg_hide_bank_warning,
		external_login flg_external_login,
		is_business flg_business,
		is_restricted flg_restricted,
		trusted_started_at,
		trusted_accepted flg_trusted_accepted,
		null migration_status,
		null suspend_reason,
		null password_method,
		null default_person_first_name,
		null default_person_last_name,
		null default_postcode,
		null last_modification_date,
		null flg_autorenew,
		null quality_score,
		null first_app_login_at,
		null flg_email_promo_notif,
		null flg_email_expired_notif,
		null disabled_export_clients,
		null username_legacy,
		null user_legacy_id,
		bonus_credits,
		bonus_credits_expire_at,
		hermes_dirty,
		uses_crm flg_uses_crm,
		sms_verification_phone,
		sms_verification_status,
		sms_verification_code,
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
	crm_integration_anlt.t_lkp_source lkp_source,
    (select coalesce(max(cod_atlas_user),0) max_cod from crm_integration_anlt.t_lkp_atlas_user) max_cod_atlas_user,
    crm_integration_anlt.t_lkp_atlas_user target
  where
    coalesce(source_table.opr_atlas_user,-1) = target.opr_atlas_user(+)
	and source_table.cod_source_system = target.cod_source_system (+)
    and target.valid_to(+) = 20991231
	and coalesce(source_table.opr_source,'Unknown') = lkp_source.opr_source
	and lkp_source.valid_to = 20991231
	) a,  crm_integration_anlt.t_lkp_city b
	where
	coalesce(a.opr_city,-1) = b.opr_city (+)
	and a.cod_source_system = b.cod_source_system (+)
	and b.valid_to (+) = 20991231;

analyze crm_integration_anlt.tmp_ro_load_atlas_user;	

	--$$$ -- 120
	
delete from crm_integration_anlt.t_lkp_atlas_user
using crm_integration_anlt.tmp_ro_load_atlas_user
where 
	tmp_ro_load_atlas_user.dml_type = 'I' 
	and t_lkp_atlas_user.opr_atlas_user = tmp_ro_load_atlas_user.opr_atlas_user
	and t_lkp_atlas_user.valid_from = (select dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_atlas_user');
	
	--$$$
	
update crm_integration_anlt.t_lkp_atlas_user
set valid_to = (select rel_integr_proc.dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_atlas_user') 
from crm_integration_anlt.tmp_ro_load_atlas_user source
where source.cod_atlas_user = crm_integration_anlt.t_lkp_atlas_user.cod_atlas_user
and crm_integration_anlt.t_lkp_atlas_user.valid_to = 20991231
and source.dml_type in('U','D');

	--$$$
	
insert into crm_integration_anlt.t_lkp_atlas_user
	 select
      case
        when dml_type = 'I' then max_cod + new_cod
        when dml_type = 'U' then cod_atlas_user
      end cod_atlas_user,
	  opr_atlas_user,
	  dsc_atlas_user,
	  (select rel_integr_proc.dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_atlas_user') valid_from, 
      20991231 valid_to,
	  cod_source_system,
	  cod_source,
	  cod_city,
	  email_original,
	  password,
	  autologin_rev,
	  type,
	  created_at,
	  last_login_at,
	  default_lang,
	  flg_newsletter,
	  flg_use_offer_limits,
	  ban_reason_id,
	  flg_autocomplete_defaults,
	  default_skype,
	  default_phone,
	  default_map_address,
	  default_gg,
	  default_person,
	  --default_region_id,
	  --default_subregion_id,
	  default_lat,
	  default_lng,
	  default_zoom,
	  --default_district_id,
	  last_login_ip,
	  last_login_port,
	  fraudster,
	  rmoderation_moderated_by,
	  rmoderation_moderated_at,
	  rmoderation_moderated_days,
	  rmoderation_moderated_total,
	  rmoderation_moderated_last,
	  credits,
	  flg_app,
	  flg_android_app,
	  flg_apple_app,
	  flg_wp_app,
	  flg_spammer,
	  flg_hide_user_ads,
	  flg_email_msg_notif,
	  flg_email_alarms_notif,
	  police_comment,
	  police_bank_account,
	  flg_monitored,
	  flg_hide_bank_warning,
	  flg_external_login,
	  flg_business,
	  flg_restricted,
	  trusted_started_at,
	  flg_trusted_accepted,
	  migration_status,
	  suspend_reason,
	  password_method,
	  default_person_first_name,
	  default_person_last_name,
	  default_postcode,
	  last_modification_date,
	  flg_autorenew,
	  quality_score,
	  first_app_login_at,
	  flg_email_promo_notif,
	  flg_email_expired_notif,
	  disabled_export_clients,
	  username_legacy,
	  user_legacy_id,
	  cast(bonus_credits as numeric(10,2)) bonus_credits,
	  cast(bonus_credits_expire_at as timestamp) bonus_credits_expire_at,
	  cast(hermes_dirty as bigint) hermes_dirty,
	  cast(flg_uses_crm as bigint) flg_uses_crm,
	  cast(sms_verification_phone as varchar) sms_verification_phone,
	  cast(sms_verification_status as varchar) sms_verification_status,
	  cast(sms_verification_code as varchar) sms_verification_code,
      hash_atlas_user,
	  cod_execution
    from
      crm_integration_anlt.tmp_ro_load_atlas_user
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

-- #######################
-- ####    PASSO 6    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set cod_status = 1, -- Ok
last_processing_datetime = coalesce((select max(operation_timestamp) from crm_integration_anlt.tmp_ro_load_atlas_user),last_processing_datetime)
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

drop table if exists crm_integration_anlt.tmp_ro_load_atlas_user;


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
      and base_contact.cod_source_system = 20
      and base_contact.valid_from = scai_valid_from.dat_processing
  ) source
where
  t_lkp_contact.cod_contact = source.cod_contact
  and t_lkp_contact.valid_from = source.valid_from
  and t_lkp_contact.cod_source_system = source.cod_source_system;

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
      and base_contact.cod_source_system = 18
      and base_contact.valid_from = scai_valid_from.dat_processing
  ) source
where
  t_lkp_contact.cod_contact = source.cod_contact
  and t_lkp_contact.valid_from = source.valid_from
  and t_lkp_contact.cod_source_system = source.cod_source_system;

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
      and base_contact.cod_source_system = 19
      and base_contact.valid_from = scai_valid_from.dat_processing
  ) source
where
  t_lkp_contact.cod_contact = source.cod_contact
  and t_lkp_contact.valid_from = source.valid_from
  and t_lkp_contact.cod_source_system = source.cod_source_system;

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
    where proc.dsc_process_short = 't_lkp_ad'
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
	and proc.dsc_process_short = 't_lkp_ad';

	--$$$
		
-- #############################################
-- # 		     ATLAS - ROMANIA               #
-- #             LOADING t_lkp_ad              #
-- #############################################

drop table if exists crm_integration_anlt.tmp_ro_load_ad_aux_horz;

create table crm_integration_anlt.tmp_ro_load_ad_aux_horz
as
	(
select
			opr_ad,
			dsc_ad_title,
			dsc_ad,
			opr_source_system,
			operation_type,
			operation_timestamp,
			opr_category,
			opr_city,
			opr_atlas_user,
			last_update_date,
			created_at,
			created_at_first,
			bump_date,
			ad_valid_to,
			opr_ad_status,
			reason_id,
			remove_reason_details,
			phone,
			params,
			contact_form,
			ip,
			port,
			search_tags,
			map_address,
			opr_offer_seek,
			opr_solr_archive_status,
			opr_solr_status,
			external_partner_code,
			external_id,
			partner_offer_url,
			private_business,
			map_zoom,
			map_radius,
			skype,
			gg,
			person,
			visible_in_profile,
			riak_ring,
			riak_key,
			riak_mapping,
			riak_order,
			riak_revision,
			riak_old,
			riak_validate,
			riak_sizes,
			paidads_valid_to,
			ad_homepage_to,
			ad_bighomepage_to,
			opr_rmoderation_status,
			rmoderation_ranking,
			rmoderation_previous_description,
			rmoderation_reasons,
			rmoderation_ip_country,
			rmoderation_moderation_started_at,
			rmoderation_moderation_ended_at,
			rmoderation_removed_at,
			rmoderation_verified_by,
			rmoderation_forwarded_by,
			rmoderation_bann_reason_id,
			rmoderation_parent,
			rmoderation_duplicate_type,
			rmoderation_markprice,
			rmoderation_paid,
			rmoderation_revision,
			moderation_disable_attribute,
			opr_source,
			flg_net_ad_counted,
			flg_was_paid_for_post,
			flg_paid_for_post,
			id_legacy,
			email,
			highlight_to,
			opr_new_used,
			export_olx_to,
			olx_id,
			olx_image_collection_id,
			migration_last_updated,
			allegro_id,
			mysql_search_title,
			flg_autorenew,
			brand_program_id,
			wp_to,
			walkaround,
			user_quality_score,
			updated_at,
			street_name,
			street_id,
			reference_id,
			punish_no_image_enabled,
			parent_id,
			panorama,
			olx_last_updated,
			mysql_search_rooms_num,
			mysql_search_price_per_m,
			mysql_search_price,
			movie,
			mirror_to,
			mailing_promotion_count,
			local_plan,
			header_to,
			header_category_id,
			hash,
			flg_extend_automatically,
			agent_id,
			ad_quality_score,
			view_3d,
			stand_id,
			map_lat,
			map_lon,
			mysql_search_m,
			accurate_location,
			created_at_pushup,
			overlimit,
			net_ad_counted,
			was_paid_for_post,
			is_paid_for_post,
			hermes_dirty,
			hide_adverts,
			urgent,
			highlight,
			topads_to,
			topads_reminded_at,
			urgent_to,
			pushup_recurrencies,
			pushup_next_recurrency,
			image_upload_monetization_to,
			opr_paidad_index, -- não vai ser usado
			opr_paidad_user_payment, -- não vai ser usado
			district_id_old, -- não se usa
			opr_district, -- não se usa
			opr_region, -- não se usa
			opr_subregion, -- não se usa
			district_name, -- não se usa
			created_at_backup_20150730, -- não se usa
      cod_execution
	  from
		(
SELECT
				coalesce(id,-1) opr_ad,
				title dsc_ad_title,
				description dsc_ad,
				'olxro' opr_source_system,
				operation_type,
				operation_timestamp,
				coalesce(category_id,-1) opr_category,
				coalesce(city_id,-1) opr_city,
				coalesce(user_id,-1) opr_atlas_user,
				cast(null as timestamp) last_update_date,
				created_at,
				created_at_first,
				cast(null as timestamp) bump_date,
				valid_to ad_valid_to,
				coalesce(status,'Unknown') opr_ad_status,
				reason_id,
				remove_reason_details,
				phone,
				params,
				contactform contact_form,
				ip,
				cast(null as bigint) as port,
				search_tags,
				map_address,
				coalesce(offer_seek,'Unknown') opr_offer_seek,
				coalesce(solr_archive_status,'Unknown') opr_solr_archive_status,
				coalesce(solr_status,'Unknown') opr_solr_status,
				external_partner_code,
				external_id,
				partner_offer_url,
				private_business,
				map_zoom,
				map_radius,
				skype,
				gg,
				person,
				visible_in_profile,
				riak_ring,
				riak_key,
				riak_mapping,
				riak_order,
				riak_revision,
				riak_old,
				riak_validate,
				riak_sizes,
				paidads_valid_to,
				ad_homepage_to,
				cast(null as timestamp) ad_bighomepage_to,
				coalesce(rmoderation_status,'Unknown') opr_rmoderation_status,
				rmoderation_ranking,
				rmoderation_previous_description,
				rmoderation_reasons,
				rmoderation_ip_country,
				rmoderation_moderation_started_at,
				rmoderation_moderation_ended_at,
				rmoderation_removed_at,
				rmoderation_verified_by,
				rmoderation_forwarded_by,
				rmoderation_bann_reason_id,
				rmoderation_parent,
				rmoderation_duplicate_type,
				rmoderation_markprice,
				rmoderation_paid,
				rmoderation_revision,
				moderation_disable_attribute,
				coalesce(source,'Unknown') opr_source,
				cast(null as bigint) flg_net_ad_counted,
				cast(null as bigint) flg_was_paid_for_post,
				cast(null as bigint) flg_paid_for_post,
				null id_legacy,
				null email,
				cast(null as timestamp) highlight_to,
				'Unknown' opr_new_used,
				cast(null as timestamp) export_olx_to,
				cast(null as bigint) olx_id,
				cast(null as bigint) olx_image_collection_id,
				cast(null as timestamp) migration_last_updated,
				cast(null as bigint) allegro_id,
				null mysql_search_title,
				cast(null as bigint) flg_autorenew,
				cast(null as bigint) brand_program_id,
				cast(null as timestamp) wp_to,
				null walkaround,
				cast(null as numeric(18)) user_quality_score,
				cast(null as timestamp) updated_at,
				null street_name,
				cast(null as bigint) street_id,
				null reference_id,
				cast(null as bigint) punish_no_image_enabled,
				cast(null as bigint) parent_id,
				null panorama,
				cast(null as timestamp) olx_last_updated,
				null mysql_search_rooms_num,
				cast(null as numeric(18)) mysql_search_price_per_m,
				cast(null as numeric(18)) mysql_search_price,
				null movie,
				cast(null as timestamp) mirror_to,
				cast(null as bigint) mailing_promotion_count,
				null local_plan,
				cast(null as timestamp) header_to,
				cast(null as bigint) header_category_id,
				null hash,
				cast(null as bigint) flg_extend_automatically,
				cast(null as bigint) agent_id,
				cast(null as numeric(18)) ad_quality_score,
				null view_3d,
				cast(null as bigint) stand_id,
				map_lat,
				map_lon,
				cast(null as bigint) mysql_search_m,
				accurate_location,
				created_at_pushup,
				overlimit,
				net_ad_counted,
				cast(null as bigint) was_paid_for_post,
				cast(null as bigint) is_paid_for_post,
				hermes_dirty,
				hide_adverts,
				cast(null as bigint) urgent,
				cast(null as timestamp) highlight,
				cast(null as timestamp) topads_to,
				cast(null as timestamp) topads_reminded_at,
				cast(null as timestamp) urgent_to,
				cast(null as bigint) pushup_recurrencies,
				cast(null as timestamp) pushup_next_recurrency,
				cast(null as timestamp) image_upload_monetization_to,
				paidads_id_index opr_paidad_index, -- não vai ser usado
				paidads_id_payment opr_paidad_user_payment, -- não vai ser usado
				cast(null as bigint) district_id_old, -- não se usa
				cast(null as bigint) opr_district, -- não se usa
				cast(null as bigint) opr_region, -- não se usa
				cast(null as bigint) opr_subregion, -- não se usa
				null district_name, -- não se usa
				cast(null as timestamp) created_at_backup_20150730, -- não se usa
				row_number() over (partition by id order by operation_type desc) rn,
				scai_execution.cod_execution
			  FROM
				crm_integration_stg.stg_ro_db_atlas_olxro_ads,
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
					and proc.dsc_process_short = 't_lkp_ad'
					and fac.cod_process = rel_integr_proc.cod_process
					and fac.cod_integration = rel_integr_proc.cod_integration
					and rel_integr_proc.dat_processing = fac.dat_processing
					and fac.cod_status = 2
			   ) scai_execution
			  --where 1 = 0
		)
	where
		rn = 1
);

analyze crm_integration_anlt.tmp_ro_load_ad_aux_horz;

	--$$$

drop table if exists crm_integration_anlt.tmp_ro_load_ad_aux;

create table crm_integration_anlt.tmp_ro_load_ad_aux
distkey(opr_ad)
sortkey(opr_ad, opr_source_system, opr_category, opr_city, opr_atlas_user, opr_ad_status, opr_offer_seek, opr_solr_archive_status, opr_solr_status, opr_rmoderation_status, opr_source, opr_new_used)
as
(
SELECT
		coalesce(id,-1) opr_ad,
		title dsc_ad_title,
		description dsc_ad,
		livesync_dbname opr_source_system,
		operation_type,
		operation_timestamp,
		coalesce(category_id,-1) opr_category,
		coalesce(city_id,-1) opr_city,
		coalesce(user_id,-1) opr_atlas_user,
		last_update_date,
		created_at,
		created_at_first,
		bump_date,
		a.valid_to ad_valid_to,
		coalesce(status,'Unknown') opr_ad_status,
		reason_id,
		remove_reason_details,
		phone,
		params,
		contactform contact_form,
		ip,
		port,
		search_tags,
		map_address,
		coalesce(offer_seek,'Unknown') opr_offer_seek,
		coalesce(solr_archive_status,'Unknown') opr_solr_archive_status,
		coalesce(solr_status,'Unknown') opr_solr_status,
		external_partner_code,
		external_id,
		partner_offer_url,
		private_business,
		map_zoom,
		map_radius,
		skype,
		gg,
		person,
		visible_in_profile,
		riak_ring,
		riak_key,
		riak_mapping,
		riak_order,
		riak_revision,
		riak_old,
		riak_validate,
		riak_sizes,
		paidads_valid_to,
		ad_homepage_to,
		ad_bighomepage_to,
		coalesce(rmoderation_status,'Unknown') opr_rmoderation_status,
		rmoderation_ranking,
		rmoderation_previous_description,
		rmoderation_reasons,
		rmoderation_ip_country,
		rmoderation_moderation_started_at,
		rmoderation_moderation_ended_at,
		rmoderation_removed_at,
		rmoderation_verified_by,
		rmoderation_forwarded_by,
		rmoderation_bann_reason_id,
		rmoderation_parent,
		rmoderation_duplicate_type,
		rmoderation_markprice,
		rmoderation_paid,
		rmoderation_revision,
		moderation_disable_attribute,
		coalesce(source,'Unknown') opr_source,
		net_ad_counted flg_net_ad_counted,
		was_paid_for_post flg_was_paid_for_post,
		is_paid_for_post flg_paid_for_post,
		id_legacy,
		email,
		highlight_to,
		coalesce(new_used,'Unknown') opr_new_used,
		export_olx_to,
		olx_id,
		olx_image_collection_id,
		migration_last_updated,
		allegro_id,
		mysql_search_title,
		autorenew flg_autorenew,
		brand_program_id,
		wp_to,
		walkaround,
		user_quality_score,
		updated_at,
		street_name,
		street_id,
		reference_id,
		punish_no_image_enabled,
		parent_id,
		panorama,
		olx_last_updated,
		mysql_search_rooms_num,
		mysql_search_price_per_m,
		mysql_search_price,
		movie,
		mirror_to,
		mailing_promotion_count,
		local_plan,
		header_to,
		header_category_id,
		hash,
		extend_automatically flg_extend_automatically,
		agent_id,
		ad_quality_score,
		"3dview" view_3d,
		stand_id,
		map_lat,
		map_lon,
		mysql_search_m,
		cast(null as bigint) accurate_location,
		cast(null as timestamp) created_at_pushup,
		cast(null as varchar) overlimit,
		cast(null as bigint) net_ad_counted,
		cast(null as bigint) was_paid_for_post,
		cast(null as bigint) is_paid_for_post,
		cast(null as bigint) hermes_dirty,
		cast(null as bigint) hide_adverts,
		cast(null as bigint) urgent,
		cast(null as timestamp) highlight,
		cast(null as timestamp) topads_to,
		cast(null as timestamp) topads_reminded_at,
		cast(null as timestamp) urgent_to,
		cast(null as bigint) pushup_recurrencies,
		cast(null as timestamp) pushup_next_recurrency,
		cast(null as timestamp) image_upload_monetization_to,
		paidads_id_index opr_paidad_index, -- não vai ser usado
		paidads_id_payment opr_paidad_user_payment, -- não vai ser usado
		district_id_old, -- não se usa
		district_id opr_district, -- não se usa
		region_id opr_region, -- não se usa
		subregion_id opr_subregion, -- não se usa
		district_name, -- não se usa
		created_at_backup_20150730, -- não se usa
		scai_execution.cod_execution
      FROM
		crm_integration_stg.stg_ro_db_atlas_verticals_ads a,
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
        and proc.dsc_process_short = 't_lkp_ad'
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
			opr_ad,
			dsc_ad_title,
			dsc_ad,
			opr_source_system,
			operation_type,
			operation_timestamp,
			opr_category,
			opr_city,
			opr_atlas_user,
			last_update_date,
			created_at,
			created_at_first,
			bump_date,
			ad_valid_to,
			opr_ad_status,
			reason_id,
			remove_reason_details,
			phone,
			params,
			contact_form,
			ip,
			port,
			search_tags,
			map_address,
			opr_offer_seek,
			opr_solr_archive_status,
			opr_solr_status,
			external_partner_code,
			external_id,
			partner_offer_url,
			private_business,
			map_zoom,
			map_radius,
			skype,
			gg,
			person,
			visible_in_profile,
			riak_ring,
			riak_key,
			riak_mapping,
			riak_order,
			riak_revision,
			riak_old,
			riak_validate,
			riak_sizes,
			paidads_valid_to,
			ad_homepage_to,
			ad_bighomepage_to,
			opr_rmoderation_status,
			rmoderation_ranking,
			rmoderation_previous_description,
			rmoderation_reasons,
			rmoderation_ip_country,
			rmoderation_moderation_started_at,
			rmoderation_moderation_ended_at,
			rmoderation_removed_at,
			rmoderation_verified_by,
			rmoderation_forwarded_by,
			rmoderation_bann_reason_id,
			rmoderation_parent,
			rmoderation_duplicate_type,
			rmoderation_markprice,
			rmoderation_paid,
			rmoderation_revision,
			moderation_disable_attribute,
			opr_source,
			flg_net_ad_counted,
			flg_was_paid_for_post,
			flg_paid_for_post,
			id_legacy,
			email,
			highlight_to,
			opr_new_used,
			export_olx_to,
			olx_id,
			olx_image_collection_id,
			migration_last_updated,
			allegro_id,
			mysql_search_title,
			flg_autorenew,
			brand_program_id,
			wp_to,
			walkaround,
			user_quality_score,
			updated_at,
			street_name,
			street_id,
			reference_id,
			punish_no_image_enabled,
			parent_id,
			panorama,
			olx_last_updated,
			mysql_search_rooms_num,
			mysql_search_price_per_m,
			mysql_search_price,
			movie,
			mirror_to,
			mailing_promotion_count,
			local_plan,
			header_to,
			header_category_id,
			hash,
			flg_extend_automatically,
			agent_id,
			ad_quality_score,
			view_3d,
			stand_id,
			map_lat,
			map_lon,
			mysql_search_m,
			accurate_location,
			created_at_pushup,
			overlimit,
			net_ad_counted,
			was_paid_for_post,
			is_paid_for_post,
			hermes_dirty,
			hide_adverts,
			urgent,
			highlight,
			topads_to,
			topads_reminded_at,
			urgent_to,
			pushup_recurrencies,
			pushup_next_recurrency,
			image_upload_monetization_to,
			opr_paidad_index, -- não vai ser usado
			opr_paidad_user_payment, -- não vai ser usado
			district_id_old, -- não se usa
			opr_district, -- não se usa
			opr_region, -- não se usa
			opr_subregion, -- não se usa
			district_name, -- não se usa
			created_at_backup_20150730, -- não se usa
			cod_execution
	  from
		crm_integration_anlt.tmp_ro_load_ad_aux_horz
);

analyze crm_integration_anlt.tmp_ro_load_ad_aux;

	--$$$

drop table if exists crm_integration_anlt.tmp_ro_load_ad_md5_step1;
	
create table crm_integration_anlt.tmp_ro_load_ad_md5_step1
distkey(opr_ad)
sortkey(opr_ad, opr_source_system)
as
  SELECT
      source.*,
      lkp_source_system.cod_source_system
  FROM
      crm_integration_anlt.tmp_ro_load_ad_aux source,
      crm_integration_anlt.t_lkp_source_system lkp_source_system
  where
      source.opr_source_system = lkp_source_system.opr_source_system;	

analyze crm_integration_anlt.tmp_ro_load_ad_md5_step1;


drop table if exists crm_integration_anlt.tmp_ro_load_ad_md5_step2;

create table crm_integration_anlt.tmp_ro_load_ad_md5_step2
distkey(opr_ad)
sortkey(opr_ad, opr_source_system, opr_category, opr_city, opr_atlas_user, opr_ad_status, opr_offer_seek, opr_solr_archive_status, opr_solr_status, opr_rmoderation_status, opr_source, opr_new_used)
as
SELECT
		source.*,
		md5
		(
			coalesce(dsc_ad_title                                                            ,'') +
			coalesce(dsc_ad                                                                  ,'') +
			coalesce(opr_category                                                            ,0) +
			coalesce(opr_city                                                                ,0) +
			coalesce(opr_atlas_user                                                          ,0) +
			--coalesce(bump_date                                                               ,'2099-12-31 00:00:00.000000') +
			coalesce(ad_valid_to                                                             ,'2099-12-31 00:00:00.000000') +
			coalesce(opr_ad_status                                                           ,'') +
			coalesce(reason_id                                                               ,0) +
			coalesce(remove_reason_details                                                   ,'') +
			coalesce(phone                                                                   ,'') +
			coalesce(params                                                                  ,'') +
			coalesce(contact_form                                                            ,0) +
			coalesce(ip                                                                      ,0) +
			coalesce(port                                                                    ,0) +
			coalesce(search_tags                                                             ,'') +
			coalesce(map_address                                                             ,'') +
			coalesce(opr_offer_seek                                                          ,'') +
			coalesce(opr_solr_archive_status                                                 ,'') +
			coalesce(opr_solr_status                                                         ,'') +
			coalesce(external_partner_code                                                   ,'') +
			coalesce(external_id                                                             ,'') +
			coalesce(partner_offer_url                                                       ,'') +
			coalesce(private_business                                                        ,'') +
			coalesce(map_zoom                                                                ,0) +
			coalesce(map_radius                                                              ,0) +
			coalesce(skype                                                                   ,'') +
			coalesce(gg                                                                      ,'') +
			coalesce(person                                                                  ,'') +
			coalesce(visible_in_profile                                                      ,0) +
			coalesce(riak_ring                                                               ,0) +
			coalesce(riak_key                                                                ,0) +
			coalesce(riak_mapping                                                            ,0) +
			coalesce(riak_order                                                              ,'') +
			coalesce(riak_revision                                                           ,0) +
			coalesce(riak_old                                                                ,0) +
			coalesce(riak_validate                                                           ,0) +
			coalesce(riak_sizes                                                              ,'') +
			--coalesce(paidads_valid_to                                                        ,'2099-12-31 00:00:00.000000') +
			--coalesce(ad_homepage_to                                                          ,'2099-12-31 00:00:00.000000') +
			--coalesce(ad_bighomepage_to                                                       ,'2099-12-31 00:00:00.000000') +
			coalesce(opr_rmoderation_status                                                  ,'') +
			coalesce(rmoderation_ranking                                                     ,0) +
			coalesce(rmoderation_previous_description                                        ,'') +
			coalesce(rmoderation_reasons                                                     ,'') +
			coalesce(rmoderation_ip_country                                                  ,'') +
			--coalesce(rmoderation_moderation_started_at                                       ,'2099-12-31 00:00:00.000000') +
			--coalesce(rmoderation_moderation_ended_at                                         ,'2099-12-31 00:00:00.000000') +
			--coalesce(rmoderation_removed_at                                                  ,'2099-12-31 00:00:00.000000') +
			coalesce(rmoderation_verified_by                                                 ,0) +
			coalesce(rmoderation_forwarded_by                                                ,0) +
			coalesce(rmoderation_bann_reason_id                                              ,0) +
			coalesce(rmoderation_parent                                                      ,0) +
			coalesce(rmoderation_duplicate_type                                              ,'') +
			coalesce(rmoderation_markprice                                                   ,0) +
			coalesce(rmoderation_paid                                                        ,0) +
			coalesce(rmoderation_revision                                                    ,0) +
			coalesce(moderation_disable_attribute                                            ,'') +
			coalesce(opr_source                                                              ,'') +
			coalesce(flg_net_ad_counted                                                      ,0) +
			coalesce(flg_was_paid_for_post                                                   ,0) +
			coalesce(flg_paid_for_post                                                    ,0) +
			coalesce(id_legacy                                                               ,'') +
			coalesce(email                                                                   ,'') +
			--coalesce(highlight_to                                                            ,'2099-12-31 00:00:00.000000') +
			coalesce(opr_new_used                                                            ,'') +
			--coalesce(export_olx_to                                                           ,'2099-12-31 00:00:00.000000') +
			coalesce(olx_id                                                                  ,0) +
			coalesce(olx_image_collection_id                                                 ,0) +
			--coalesce(migration_last_updated                                                  ,'2099-12-31 00:00:00.000000') +
			coalesce(allegro_id                                                              ,0) +
			coalesce(mysql_search_title                                                      ,'') +
			coalesce(flg_autorenew                                                           ,0) +
			coalesce(brand_program_id                                                        ,0) +
			--coalesce(wp_to                                                                   ,'2099-12-31 00:00:00.000000') +
			coalesce(walkaround                                                              ,'') +
			cast(coalesce(user_quality_score                                                 ,0) as varchar) +
			--coalesce(updated_at                                                              ,'2099-12-31 00:00:00.000000') +
			coalesce(street_name                                                             ,'') +
			coalesce(street_id                                                               ,0) +
			coalesce(reference_id                                                            ,'') +
			coalesce(punish_no_image_enabled                                                 ,0) +
			coalesce(parent_id                                                               ,0) +
			coalesce(panorama                                                                ,'') +
			--coalesce(olx_last_updated                                                        ,'2099-12-31 00:00:00.000000') +
			coalesce(mysql_search_rooms_num                                                  ,'') +
			cast(coalesce(mysql_search_price_per_m                                           ,0) as varchar) +
			cast(coalesce(mysql_search_price                                                 ,0) as varchar) +
			coalesce(movie                                                                   ,'') +
			--coalesce(mirror_to                                                               ,'2099-12-31 00:00:00.000000') +
			coalesce(mailing_promotion_count                                                 ,0) +
			coalesce(local_plan                                                              ,'') +
			--coalesce(header_to                                                               ,'2099-12-31 00:00:00.000000') +
			coalesce(header_category_id                                                      ,0) +
			coalesce(hash                                                                    ,'') +
			coalesce(flg_extend_automatically                                                ,0) +
			coalesce(agent_id                                                                ,0) +
			cast(coalesce(ad_quality_score                                                   ,0) as varchar) +
			coalesce(view_3d                                                                 ,'') +
			coalesce(stand_id                                                                ,0) +
			cast(coalesce(map_lat                                                            ,0) as varchar) +
			cast(coalesce(map_lon                                                            ,0) as varchar) +
			cast(coalesce(mysql_search_m                                                     ,0) as varchar) +
			coalesce(accurate_location                                                       ,0) +
			--coalesce(created_at_pushup                                                       ,'2099-12-31 00:00:00.000000') +
			coalesce(overlimit                                                               ,'') +
			coalesce(net_ad_counted                                                          ,0) +
			coalesce(was_paid_for_post                                                       ,0) +
			coalesce(is_paid_for_post                                                        ,0) +
			coalesce(hermes_dirty                                                            ,0) +
			coalesce(hide_adverts                                                            ,0) +
			coalesce(urgent                                                                  ,0) +
			--coalesce(highlight                                                               ,'2099-12-31 00:00:00.000000') +
			--coalesce(topads_to                                                               ,'2099-12-31 00:00:00.000000') +
			--coalesce(topads_reminded_at                                                      ,'2099-12-31 00:00:00.000000') +
			--coalesce(urgent_to                                                               ,'2099-12-31 00:00:00.000000') +
			coalesce(pushup_recurrencies                                                     ,0)
			--coalesce(pushup_next_recurrency                                                  ,'2099-12-31 00:00:00.000000') +
			--coalesce(image_upload_monetization_to                                            ,'2099-12-31 00:00:00.000000')
	    ) hash_ad
	  FROM
		crm_integration_anlt.tmp_ro_load_ad_md5_step1 source;

analyze crm_integration_anlt.tmp_ro_load_ad_md5_step2;
	
	--$$$
	
drop table if exists crm_integration_anlt.tmp_ro_load_ad;

create table crm_integration_anlt.tmp_ro_load_ad 
distkey(cod_ad)
sortkey(cod_ad, opr_ad, dml_type, cod_source_system)
as
  select source.*, coalesce(lkp_city.cod_city,-1) cod_city, coalesce(lkp_atlas_user.cod_atlas_user,-1) cod_atlas_user, coalesce(lkp_category.cod_category,-1) cod_category
		from
	(
 select
  source_table.opr_ad,
	source_table.dsc_ad_title,
	source_table.dsc_ad,
	--lkp_category.cod_category,
	source_table.opr_category,
	--lkp_city.cod_city,
	source_table.opr_city,
	--lkp_atlas_user.cod_atlas_user,
	source_table.opr_atlas_user,
	source_table.operation_type,
	source_table.operation_timestamp,
	source_table.last_update_date,
	source_table.created_at,
	source_table.created_at_first,
	source_table.bump_date,
	source_table.ad_valid_to,
	coalesce(lkp_ad_status.cod_ad_status,-2) cod_ad_status,
	source_table.reason_id,
	source_table.remove_reason_details,
	source_table.phone,
	source_table.params,
	source_table.contact_form,
	source_table.ip,
	source_table.port,
	source_table.search_tags,
	source_table.map_address,
	coalesce(lkp_offer_seek.cod_offer_seek,-2) cod_offer_seek,
	coalesce(lkp_solr_archive_status.cod_solr_archive_status,-2) cod_solr_archive_status,
	coalesce(lkp_solr_status.cod_solr_status,-2) cod_solr_status,
	source_table.external_partner_code,
	source_table.external_id,
	source_table.partner_offer_url,
	source_table.private_business,
	source_table.map_zoom,
	source_table.map_radius,
	source_table.skype,
	source_table.gg,
	source_table.person,
	source_table.visible_in_profile,
	source_table.riak_ring,
	source_table.riak_key,
	source_table.riak_mapping,
	source_table.riak_order,
	source_table.riak_revision,
	source_table.riak_old,
	source_table.riak_validate,
	source_table.riak_sizes,
	source_table.paidads_valid_to,
	source_table.ad_homepage_to,
	source_table.ad_bighomepage_to,
	coalesce(lkp_rmoderation_status.cod_rmoderation_status,-2) cod_rmoderation_status,
	source_table.rmoderation_ranking,
	source_table.rmoderation_previous_description,
	source_table.rmoderation_reasons,
	source_table.rmoderation_ip_country,
	source_table.rmoderation_moderation_started_at,
	source_table.rmoderation_moderation_ended_at,
	source_table.rmoderation_removed_at,
	source_table.rmoderation_verified_by,
	source_table.rmoderation_forwarded_by,
	source_table.rmoderation_bann_reason_id,
	source_table.rmoderation_parent,
	source_table.rmoderation_duplicate_type,
	source_table.rmoderation_markprice,
	source_table.rmoderation_paid,
	source_table.rmoderation_revision,
	source_table.moderation_disable_attribute,
	coalesce(lkp_source.cod_source,-2) cod_source,
	source_table.flg_net_ad_counted,
	source_table.flg_was_paid_for_post,
	source_table.flg_paid_for_post,
	source_table.id_legacy,
	source_table.email,
	source_table.highlight_to,
	coalesce(lkp_new_used.cod_new_used,-2) cod_new_used,
	source_table.export_olx_to,
	source_table.olx_id,
	source_table.olx_image_collection_id,
	source_table.migration_last_updated,
	source_table.allegro_id,
	source_table.mysql_search_title,
	source_table.flg_autorenew,
	source_table.brand_program_id,
	source_table.wp_to,
	source_table.walkaround,
	source_table.user_quality_score,
	source_table.updated_at,
	source_table.street_name,
	source_table.street_id,
	source_table.reference_id,
	source_table.punish_no_image_enabled,
	source_table.parent_id,
	source_table.panorama,
	source_table.olx_last_updated,
	source_table.mysql_search_rooms_num,
	source_table.mysql_search_price_per_m,
	source_table.mysql_search_price,
	source_table.movie,
	source_table.mirror_to,
	source_table.mailing_promotion_count,
	source_table.local_plan,
	source_table.header_to,
	source_table.header_category_id,
	source_table.hash_ad,
	source_table.flg_extend_automatically,
	source_table.agent_id,
	source_table.ad_quality_score,
	source_table.view_3d,
	source_table.stand_id,
	source_table.map_lat,
	source_table.map_lon,
	source_table.mysql_search_m,
	source_table.accurate_location,
	source_table.created_at_pushup,
	source_table.overlimit,
	source_table.net_ad_counted,
	source_table.was_paid_for_post,
	source_table.is_paid_for_post,
	source_table.hermes_dirty,
	source_table.hide_adverts,
	source_table.urgent,
	source_table.highlight,
	source_table.topads_to,
	source_table.topads_reminded_at,
	source_table.urgent_to,
	source_table.pushup_recurrencies,
	source_table.pushup_next_recurrency,
	source_table.image_upload_monetization_to,
	source_table.hash,
    source_table.cod_source_system,
	source_table.cod_execution,
    max_cod_ad.max_cod,
    row_number() over (order by source_table.opr_ad desc) new_cod,
    target.cod_ad,
    case
      --when target.cod_ad is null then 'I'
	  when target.cod_ad is null or (source_table.hash_ad != target.hash_ad and target.valid_from = (select dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_ad')) then 'I'
	  when source_table.operation_type = 'delete' then 'D'
      when source_table.hash_ad != target.hash_ad then 'U'
        else 'X'
    end dml_type
  from
	crm_integration_anlt.tmp_ro_load_ad_md5_step2 source_table,
	crm_integration_anlt.t_lkp_ad_status lkp_ad_status,
	crm_integration_anlt.t_lkp_offer_seek lkp_offer_seek,
	crm_integration_anlt.t_lkp_solr_archive_status lkp_solr_archive_status,
	crm_integration_anlt.t_lkp_solr_status lkp_solr_status,
	crm_integration_anlt.t_lkp_rmoderation_status lkp_rmoderation_status,
	crm_integration_anlt.t_lkp_source lkp_source,
	crm_integration_anlt.t_lkp_new_used lkp_new_used,
    (select coalesce(max(cod_ad),0) max_cod from crm_integration_anlt.t_lkp_ad) max_cod_ad,
    crm_integration_anlt.t_lkp_ad target
  where
    source_table.opr_ad = target.opr_ad(+)
	and source_table.cod_source_system = target.cod_source_system (+)
    and target.valid_to(+) = 20991231
	and source_table.opr_ad_status = lkp_ad_status.opr_ad_status
	and lkp_ad_status.valid_to = 20991231
	and source_table.opr_offer_seek = lkp_offer_seek.opr_offer_seek
	and lkp_offer_seek.valid_to = 20991231
	and source_table.opr_solr_archive_status = lkp_solr_archive_status.opr_solr_archive_status
	and lkp_solr_archive_status.valid_to = 20991231
	and source_table.opr_solr_status = lkp_solr_status.opr_solr_status
	and lkp_solr_status.valid_to = 20991231
	and source_table.opr_rmoderation_status = lkp_rmoderation_status.opr_rmoderation_status
	and lkp_rmoderation_status.valid_to = 20991231
	and source_table.opr_source = lkp_source.opr_source
	and lkp_source.valid_to = 20991231
	and source_table.opr_new_used = lkp_new_used.opr_new_used
	and lkp_new_used.valid_to = 20991231
) source,
	crm_integration_anlt.t_lkp_city lkp_city,
	crm_integration_anlt.t_lkp_atlas_user lkp_atlas_user,
	crm_integration_anlt.t_lkp_category lkp_category
where 
	coalesce(source.opr_city,-1) = lkp_city.opr_city (+)
	and source.cod_source_system = lkp_city.cod_source_system (+) -- new
	and lkp_city.valid_to (+) = 20991231
	and coalesce(source.opr_atlas_user,-1) = lkp_atlas_user.opr_atlas_user (+)
	and source.cod_source_system = lkp_atlas_user.cod_source_system (+) -- new
	and lkp_atlas_user.valid_to (+) = 20991231
	and coalesce(source.opr_category,-1) = lkp_category.opr_category (+)
	and source.cod_source_system = lkp_category.cod_source_system (+) -- new
	and lkp_category.valid_to (+) = 20991231;

analyze crm_integration_anlt.tmp_ro_load_ad;
	
	--$$$ 
	
delete from crm_integration_anlt.t_lkp_ad
using crm_integration_anlt.tmp_ro_load_ad
where 
	tmp_ro_load_ad.dml_type = 'I' 
	and t_lkp_ad.cod_ad = tmp_ro_load_ad.cod_ad
	and t_lkp_ad.valid_from = (select dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_ad');
	
	--$$$
	
update crm_integration_anlt.t_lkp_ad
set valid_to = (select rel_integr_proc.dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_ad')
from crm_integration_anlt.tmp_ro_load_ad source
where source.cod_ad = crm_integration_anlt.t_lkp_ad.cod_ad
and crm_integration_anlt.t_lkp_ad.valid_to = 20991231
and source.dml_type in('U','D');

insert into crm_integration_anlt.t_lkp_ad
    select
		case
			when dml_type = 'I' then max_cod + new_cod
			when dml_type = 'U' then cod_ad
		end cod_ad,
		opr_ad,
		dsc_ad_title,
		dsc_ad,
		(select rel_integr_proc.dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 4 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_ad') valid_from, 
		20991231 valid_to,
		cod_source_system,
		cod_category,
		cod_city,
		cod_atlas_user,
		last_update_date,
		created_at,
		created_at_first,
		bump_date,
		ad_valid_to,
		cod_ad_status,
		reason_id,
		remove_reason_details,
		phone,
		params,
		contact_form,
		ip,
		port,
		search_tags,
		map_address,
		cod_offer_seek,
		cod_solr_archive_status,
		cod_solr_status,
		external_partner_code,
		external_id,
		partner_offer_url,
		private_business,
		map_zoom,
		map_radius,
		skype,
		gg,
		person,
		visible_in_profile,
		riak_ring,
		riak_key,
		riak_mapping,
		riak_order,
		riak_revision,
		riak_old,
		riak_validate,
		riak_sizes,
		paidads_valid_to,
		ad_homepage_to,
		ad_bighomepage_to,
		cod_rmoderation_status,
		rmoderation_ranking,
		rmoderation_previous_description,
		rmoderation_reasons,
		rmoderation_ip_country,
		rmoderation_moderation_started_at,
		rmoderation_moderation_ended_at,
		rmoderation_removed_at,
		rmoderation_verified_by,
		rmoderation_forwarded_by,
		rmoderation_bann_reason_id,
		rmoderation_parent,
		rmoderation_duplicate_type,
		rmoderation_markprice,
		rmoderation_paid,
		rmoderation_revision,
		moderation_disable_attribute,
		cod_source,
		flg_net_ad_counted,
		flg_was_paid_for_post,
		flg_paid_for_post,
		id_legacy,
		email,
		highlight_to,
		cod_new_used,
		export_olx_to,
		olx_id,
		olx_image_collection_id,
		migration_last_updated,
		allegro_id,
		mysql_search_title,
		flg_autorenew,
		brand_program_id,
		wp_to,
		walkaround,
		user_quality_score,
		updated_at,
		street_name,
		street_id,
		reference_id,
		punish_no_image_enabled,
		parent_id,
		panorama,
		olx_last_updated,
		mysql_search_rooms_num,
		mysql_search_price_per_m,
		mysql_search_price,
		movie,
		mirror_to,
		mailing_promotion_count,
		local_plan,
		header_to,
		header_category_id,
		hash,
		flg_extend_automatically,
		agent_id,
		ad_quality_score,
		view_3d,
		stand_id,
		map_lat,
		map_lon,
		mysql_search_m,
		accurate_location,
		created_at_pushup,
		overlimit,
		net_ad_counted,
		was_paid_for_post,
		is_paid_for_post,
		hermes_dirty,
		hide_adverts,
		urgent,
		highlight,
		topads_to,
		topads_reminded_at,
		urgent_to,
		pushup_recurrencies,
		pushup_next_recurrency,
		image_upload_monetization_to,
		hash_ad,
		cod_execution
    from
      crm_integration_anlt.tmp_ro_load_ad
    where
      dml_type in ('U','I');

analyze crm_integration_anlt.t_lkp_ad;
	  
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
	and proc.dsc_process_short = 't_lkp_ad';

-- #######################
-- ####    PASSO 6    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set cod_status = 1, -- Ok
last_processing_datetime = coalesce((select max(operation_timestamp) from crm_integration_anlt.tmp_ro_load_ad),last_processing_datetime)
/*from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc, crm_integration_anlt.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_ad'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 4
  ) source*/
from crm_integration_anlt.t_lkp_scai_process proc 
where t_rel_scai_integration_process.cod_process = proc.cod_process
and t_rel_scai_integration_process.cod_status = 2
and t_rel_scai_integration_process.cod_country = 4
and proc.dsc_process_short = 't_lkp_ad'
and t_rel_scai_integration_process.ind_active = 1
/*crm_integration_anlt.t_rel_scai_integration_process.cod_process = source.cod_process
and crm_integration_anlt.t_rel_scai_integration_process.cod_country = source.cod_country
and crm_integration_anlt.t_rel_scai_integration_process.cod_integration = source.cod_integration*/;

drop table if exists crm_integration_anlt.tmp_ro_load_ad;
drop table if exists crm_integration_anlt.tmp_ro_load_ad_md5_step2;
drop table if exists crm_integration_anlt.tmp_ro_load_ad_md5_step1;
drop table if exists crm_integration_anlt.tmp_ro_load_ad_aux;
drop table if exists crm_integration_anlt.tmp_ro_load_ad_aux_horz;

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
    where proc.dsc_process_short = 't_fac_answer_outgoing'
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
	and proc.dsc_process_short = 't_fac_answer_outgoing';

	--$$$
	
-- #############################################
-- # 		   ATLAS - ROMANIA                 #
-- #       LOADING t_fac_answer_outgoing       #
-- #############################################

create table crm_integration_anlt.tmp_ro_load_answer_step1_outgoing
distkey(opr_atlas_user_receiver)
sortkey(opr_atlas_user_receiver, opr_source, cod_source_system)
as
(
  select
    cast(coalesce(id,-1) as bigint) opr_answer,
    livesync_dbname opr_source_system,
	operation_timestamp,
    parent_id opr_answer_parent,
    cast(coalesce(ad_id,-1)  as bigint) opr_ad,
    cast(coalesce(reciever_id,-1)  as bigint) opr_atlas_user_receiver,
    sender_phone,
    readed flg_readed,
    star flg_star,
    posted dat_posted,
    readed_at dat_readed_at,
    number,
    last_posted_in dat_last_posted_in,
    last_posted_out dat_last_posted_out,
    last_posted_id opr_answer_last_posted,
    ip,
    port,
    coalesce(source,'Unknown') opr_source,
    embrace_user_id,
    olx_conversation_id,
	  cod_source_system,
    scai_execution.cod_execution
  from
    crm_integration_stg.stg_ro_db_atlas_verticals_answers a,
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
		  and proc.dsc_process_short = 't_fac_answer_outgoing'
		  and fac.cod_process = rel_integr_proc.cod_process
		  and fac.cod_integration = rel_integr_proc.cod_integration
		  and rel_integr_proc.dat_processing = fac.dat_processing
		  and fac.cod_status = 2
	 ) scai_execution
  where
    a.livesync_dbname = b.opr_source_system
    and b.cod_business_type = 1 -- Verticals
    and b.cod_country = 4 -- Romania
    and a.seller_id = a.sender_id
	--and 1 = 0
  union all
  select
    cast(coalesce(id,-1) as bigint) opr_answer,
    'olxro' opr_source_system,
	operation_timestamp,
    parent_id opr_answer_parent,
    cast(coalesce(ad_id,-1)  as bigint) opr_ad,
    cast(coalesce(reciever_id,-1)  as bigint) opr_atlas_user_receiver,
    null sender_phone,
    readed flg_readed,
    star flg_star,
    posted dat_posted,
    readed_at dat_readed_at,
    number,
    last_posted_in dat_last_posted_in,
    last_posted_out dat_last_posted_out,
    last_posted_id opr_answer_last_posted,
    ip,
    cast(null as bigint) as port,
    coalesce(source,'Unknown') opr_source,
    cast(null as bigint) embrace_user_id,
    cast(null as bigint) olx_conversation_id,
	  cod_source_system,
    scai_execution.cod_execution
  from
    crm_integration_stg.stg_ro_db_atlas_olxro_answers,
    (select cod_source_system from crm_integration_anlt.t_lkp_source_system
      where opr_source_system = 'olxro'),
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
		  and proc.dsc_process_short = 't_fac_answer_outgoing'
		  and fac.cod_process = rel_integr_proc.cod_process
		  and fac.cod_integration = rel_integr_proc.cod_integration
		  and rel_integr_proc.dat_processing = fac.dat_processing
		  and fac.cod_status = 2
	 ) scai_execution
  where
    seller_id = sender_id
	--and 1 = 0
);

analyze crm_integration_anlt.tmp_ro_load_answer_step1_outgoing;

--$$$

create table crm_integration_anlt.tmp_ro_load_answer_step2_outgoing
distkey(opr_ad)
sortkey(opr_ad, cod_source_system)
as
  select
    source_table.opr_answer,
	source_table.operation_timestamp,
    source_table.sender_phone,
    source_table.flg_readed,
    source_table.flg_star,
    source_table.dat_posted,
    source_table.dat_readed_at,
    source_table.number,
    source_table.dat_last_posted_in,
    source_table.dat_last_posted_out,
    source_table.opr_answer_last_posted,
    source_table.ip,
    source_table.port,
    source_table.embrace_user_id,
    source_table.olx_conversation_id,
    source_table.opr_answer_parent,
    source_table.cod_source_system,
    source_table.opr_atlas_user_receiver,
    source_table.opr_ad,
	source_table.cod_execution,
    coalesce(lkp_atlas_receiver.cod_atlas_user,-2) cod_atlas_user_receiver,
    coalesce(lkp_source.cod_source,-2) cod_source,
    max_cod_answer.max_cod,
    row_number() over (order by source_table.opr_answer desc) new_cod
  from
    crm_integration_anlt.tmp_ro_load_answer_step1_outgoing source_table,
    crm_integration_anlt.t_lkp_atlas_user lkp_atlas_receiver,
    crm_integration_anlt.t_lkp_source lkp_source,
    (select coalesce(max(cod_answer),0) max_cod from crm_integration_anlt.t_fac_answer_outgoing) max_cod_answer
  where
    coalesce(source_table.opr_atlas_user_receiver,-1) = lkp_atlas_receiver.opr_atlas_user
    and source_table.cod_source_system = lkp_atlas_receiver.cod_source_system -- new
    and lkp_atlas_receiver.valid_to = 20991231
  and coalesce(source_table.opr_source,'Unknown') = lkp_source.opr_source
  and lkp_source.valid_to = 20991231;
 
analyze crm_integration_anlt.tmp_ro_load_answer_step2_outgoing;
 
--$$$
 
create table crm_integration_anlt.tmp_ro_load_answer_step3_outgoing
distkey(opr_answer)
sortkey(opr_answer, cod_source_system)
as
  select
    source_table.opr_answer,
	source_table.operation_timestamp,
    source_table.sender_phone,
    source_table.flg_readed,
    source_table.flg_star,
    source_table.dat_posted,
    source_table.dat_readed_at,
    source_table.number,
    source_table.dat_last_posted_in,
    source_table.dat_last_posted_out,
    source_table.opr_answer_last_posted,
    source_table.ip,
    source_table.port,
    source_table.embrace_user_id,
    source_table.olx_conversation_id,
    source_table.opr_answer_parent,
    source_table.cod_source_system,
    coalesce(lkp_ad.cod_ad,-2) cod_ad,
    source_table.cod_atlas_user_receiver,
    source_table.cod_source,
    source_table.max_cod,
    source_table.new_cod,
	source_table.cod_execution
  from
    crm_integration_anlt.tmp_ro_load_answer_step2_outgoing source_table,
    crm_integration_anlt.t_lkp_ad lkp_ad
  where
    coalesce(source_table.opr_ad,-1) = lkp_ad.opr_ad
    and source_table.cod_source_system = lkp_ad.cod_source_system -- new
    and lkp_ad.valid_to = 20991231;

analyze crm_integration_anlt.tmp_ro_load_answer_step3_outgoing;
	
	--$$$
	
insert into crm_integration_anlt.t_hst_answer_outgoing
select * from crm_integration_anlt.t_fac_answer_outgoing
where crm_integration_anlt.t_fac_answer_outgoing.opr_answer in (select distinct opr_answer from crm_integration_anlt.tmp_ro_load_answer_step3_outgoing)
and crm_integration_anlt.t_fac_answer_outgoing.cod_source_system in (select distinct cod_source_system from crm_integration_anlt.tmp_ro_load_answer_step3_outgoing);

delete from crm_integration_anlt.t_fac_answer_outgoing
where crm_integration_anlt.t_fac_answer_outgoing.opr_answer in (select distinct opr_answer from crm_integration_anlt.tmp_ro_load_answer_step3_outgoing)
and crm_integration_anlt.t_fac_answer_outgoing.cod_source_system in (select distinct cod_source_system from crm_integration_anlt.tmp_ro_load_answer_step3_outgoing);

--$$$ -- 140

insert into crm_integration_anlt.t_fac_answer_outgoing
    select
      max_cod + new_cod cod_answer,
      opr_answer,
      opr_answer_parent,
      cod_ad,
      cod_atlas_user_receiver,
      cod_source_system,
      sender_phone,
      flg_readed,
      flg_star,
      dat_posted,
      dat_readed_at,
      number,
      dat_last_posted_in,
      dat_last_posted_out,
      opr_answer_last_posted,
      ip,
      port,
      cod_source,
      embrace_user_id,
      olx_conversation_id,
      null hash_answer,
	  cod_execution
    from
      crm_integration_anlt.tmp_ro_load_answer_step3_outgoing;

analyze crm_integration_anlt.t_fac_answer_outgoing;
	  
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
	and proc.dsc_process_short = 't_fac_answer_outgoing';

-- #######################
-- ####    PASSO 6    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set cod_status = 1, -- Ok
last_processing_datetime = coalesce((select max(operation_timestamp) from crm_integration_anlt.tmp_ro_load_answer_step2_outgoing),last_processing_datetime)
/*from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc, crm_integration_anlt.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_ad'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 4
  ) source*/
from crm_integration_anlt.t_lkp_scai_process proc 
where t_rel_scai_integration_process.cod_process = proc.cod_process
and t_rel_scai_integration_process.cod_status = 2
and t_rel_scai_integration_process.cod_country = 4
and proc.dsc_process_short = 't_fac_answer_outgoing'
and t_rel_scai_integration_process.ind_active = 1
/*crm_integration_anlt.t_rel_scai_integration_process.cod_process = source.cod_process
and crm_integration_anlt.t_rel_scai_integration_process.cod_country = source.cod_country
and crm_integration_anlt.t_rel_scai_integration_process.cod_integration = source.cod_integration*/;

drop table if exists crm_integration_anlt.tmp_ro_load_answer_step1_outgoing;
drop table if exists crm_integration_anlt.tmp_ro_load_answer_step2_outgoing;
drop table if exists crm_integration_anlt.tmp_ro_load_answer_step3_outgoing;

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
    where proc.dsc_process_short = 't_fac_answer_incoming'
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
	and proc.dsc_process_short = 't_fac_answer_incoming';

	--$$$

-- #############################################
-- # 		   ATLAS - ROMANIA                 #
-- #       LOADING t_fac_answer_incoming       #
-- #############################################

create table crm_integration_anlt.tmp_ro_load_answer_step1_incoming
distkey(opr_atlas_user_sender)
sortkey(opr_atlas_user_sender, opr_source, cod_source_system)
as
(
  select
    cast(coalesce(id,-1) as bigint) opr_answer,
    livesync_dbname opr_source_system,
	  operation_timestamp,
    parent_id opr_answer_parent,
    cast(coalesce(ad_id,-1)  as bigint) opr_ad,
    cast(coalesce(sender_id,-1)  as bigint) opr_atlas_user_sender,
    sender_phone,
    readed flg_readed,
    star flg_star,
    posted dat_posted,
    readed_at dat_readed_at,
    number,
    last_posted_in dat_last_posted_in,
    last_posted_out dat_last_posted_out,
    last_posted_id opr_answer_last_posted,
    ip,
    port,
    coalesce(source,'Unknown') opr_source,
    embrace_user_id,
    olx_conversation_id,
	  cod_source_system,
    scai_execution.cod_execution
  from
    crm_integration_stg.stg_ro_db_atlas_verticals_answers a,
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
		  and proc.dsc_process_short = 't_fac_answer_incoming'
		  and fac.cod_process = rel_integr_proc.cod_process
		  and fac.cod_integration = rel_integr_proc.cod_integration
		  and rel_integr_proc.dat_processing = fac.dat_processing
		  and fac.cod_status = 2
	) scai_execution
  where
    a.livesync_dbname = b.opr_source_system
    and b.cod_business_type = 1 -- Verticals
    and b.cod_country = 4 -- Romania
    and a.seller_id = a.reciever_id
	--and 1 = 0
  union all
  select
    cast(coalesce(id,-1) as bigint) opr_answer,
    'olxro' opr_source_system,
	  operation_timestamp,
    parent_id opr_answer_parent,
    cast(coalesce(ad_id,-1)  as bigint) opr_ad,
    cast(coalesce(sender_id,-1)  as bigint) opr_atlas_user_sender,
    null sender_phone,
    readed flg_readed,
    star flg_star,
    posted dat_posted,
    readed_at dat_readed_at,
    number,
    last_posted_in dat_last_posted_in,
    last_posted_out dat_last_posted_out,
    last_posted_id opr_answer_last_posted,
    ip,
    cast(null as bigint) as port,
    coalesce(source,'Unknown') opr_source,
    cast(null as bigint) embrace_user_id,
    cast(null as bigint) olx_conversation_id,
	  cod_source_system,
    scai_execution.cod_execution
  from
    crm_integration_stg.stg_ro_db_atlas_olxro_answers,
    (select cod_source_system from crm_integration_anlt.t_lkp_source_system
      where opr_source_system = 'olxro'),
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
		  and proc.dsc_process_short = 't_fac_answer_incoming'
		  and fac.cod_process = rel_integr_proc.cod_process
		  and fac.cod_integration = rel_integr_proc.cod_integration
		  and rel_integr_proc.dat_processing = fac.dat_processing
		  and fac.cod_status = 2
	) scai_execution
  where
    seller_id = reciever_id
	--and 1 = 0
);

analyze crm_integration_anlt.tmp_ro_load_answer_step1_incoming;

--$$$

create table crm_integration_anlt.tmp_ro_load_answer_step2_incoming
distkey(opr_ad)
sortkey(opr_ad, cod_source_system)
as
  select
    source_table.opr_answer,
	source_table.operation_timestamp,
    source_table.sender_phone,
    source_table.flg_readed,
    source_table.flg_star,
    source_table.dat_posted,
    source_table.dat_readed_at,
    source_table.number,
    source_table.dat_last_posted_in,
    source_table.dat_last_posted_out,
    source_table.opr_answer_last_posted,
    source_table.ip,
    source_table.port,
    source_table.embrace_user_id,
    source_table.olx_conversation_id,
    source_table.opr_answer_parent,
    source_table.cod_source_system,
    source_table.opr_ad,
	source_table.cod_execution,
    coalesce(lkp_atlas_sender.cod_atlas_user,-2) cod_atlas_user_sender,
    coalesce(lkp_source.cod_source,-2) cod_source,
    max_cod_answer.max_cod,
    row_number() over (order by source_table.opr_answer desc) new_cod
  from
    crm_integration_anlt.tmp_ro_load_answer_step1_incoming source_table,
    crm_integration_anlt.t_lkp_atlas_user lkp_atlas_sender,
    crm_integration_anlt.t_lkp_source lkp_source,
    (select coalesce(max(cod_answer),0) max_cod from crm_integration_anlt.t_fac_answer_incoming) max_cod_answer
  where
    coalesce(source_table.opr_atlas_user_sender,-1) = lkp_atlas_sender.opr_atlas_user
    and source_table.cod_source_system = lkp_atlas_sender.cod_source_system -- new
    and lkp_atlas_sender.valid_to = 20991231
    and coalesce(source_table.opr_source,'Unknown') = lkp_source.opr_source
    and lkp_source.valid_to = 20991231;

analyze crm_integration_anlt.tmp_ro_load_answer_step2_incoming;
	
	--$$$

create table crm_integration_anlt.tmp_ro_load_answer_step3_incoming
distkey(opr_answer)
sortkey(opr_answer, cod_source_system)
as
  select
    source_table.opr_answer,
	  source_table.operation_timestamp,
    source_table.sender_phone,
    source_table.flg_readed,
    source_table.flg_star,
    source_table.dat_posted,
    source_table.dat_readed_at,
    source_table.number,
    source_table.dat_last_posted_in,
    source_table.dat_last_posted_out,
    source_table.opr_answer_last_posted,
    source_table.ip,
    source_table.port,
    source_table.embrace_user_id,
    source_table.olx_conversation_id,
    source_table.opr_answer_parent,
    source_table.cod_source_system,
    coalesce(lkp_ad.cod_ad,-2) cod_ad,
    source_table.cod_atlas_user_sender,
    source_table.cod_source,
    source_table.max_cod,
    source_table.new_cod,
	source_table.cod_execution
  from
    crm_integration_anlt.tmp_ro_load_answer_step2_incoming source_table,
    crm_integration_anlt.t_lkp_ad lkp_ad
  where
    coalesce(source_table.opr_ad,-1) = lkp_ad.opr_ad
    and source_table.cod_source_system = lkp_ad.cod_source_system -- new
    and lkp_ad.valid_to = 20991231;

analyze crm_integration_anlt.tmp_ro_load_answer_step3_incoming;
	
	--$$$

insert into crm_integration_anlt.t_hst_answer_incoming
select * from crm_integration_anlt.t_fac_answer_incoming
where crm_integration_anlt.t_fac_answer_incoming.opr_answer in (select distinct opr_answer from crm_integration_anlt.tmp_ro_load_answer_step2_incoming)
and crm_integration_anlt.t_fac_answer_incoming.cod_source_system in (select distinct cod_source_system from crm_integration_anlt.tmp_ro_load_answer_step2_incoming);

delete from crm_integration_anlt.t_fac_answer_incoming
where crm_integration_anlt.t_fac_answer_incoming.opr_answer in (select distinct opr_answer from crm_integration_anlt.tmp_ro_load_answer_step2_incoming)
and crm_integration_anlt.t_fac_answer_incoming.cod_source_system in (select distinct cod_source_system from crm_integration_anlt.tmp_ro_load_answer_step2_incoming);

--$$$

insert into crm_integration_anlt.t_fac_answer_incoming
    select
      max_cod + new_cod cod_answer,
      opr_answer,
      opr_answer_parent,
      cod_ad,
      cod_atlas_user_sender,
      cod_source_system,
      sender_phone,
      flg_readed,
      flg_star,
      dat_posted,
      dat_readed_at,
      number,
      dat_last_posted_in,
      dat_last_posted_out,
      opr_answer_last_posted,
      ip,
      port,
      cod_source,
      embrace_user_id,
      olx_conversation_id,
      null hash_answer,
	  cod_execution
    from
      crm_integration_anlt.tmp_ro_load_answer_step3_incoming;

analyze crm_integration_anlt.t_fac_answer_incoming;
	  
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
	and proc.dsc_process_short = 't_fac_answer_incoming';

-- #######################
-- ####    PASSO 6    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set cod_status = 1, -- Ok
last_processing_datetime = coalesce((select max(operation_timestamp) from crm_integration_anlt.tmp_ro_load_answer_step2_incoming),last_processing_datetime)
/*from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc, crm_integration_anlt.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_ad'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 4
  ) source*/
from crm_integration_anlt.t_lkp_scai_process proc
where t_rel_scai_integration_process.cod_process = proc.cod_process
and t_rel_scai_integration_process.cod_status = 2
and t_rel_scai_integration_process.cod_country = 4
and proc.dsc_process_short = 't_fac_answer_incoming'
and t_rel_scai_integration_process.ind_active = 1
/*crm_integration_anlt.t_rel_scai_integration_process.cod_process = source.cod_process
and crm_integration_anlt.t_rel_scai_integration_process.cod_country = source.cod_country
and crm_integration_anlt.t_rel_scai_integration_process.cod_integration = source.cod_integration*/;

drop table if exists crm_integration_anlt.tmp_ro_load_answer_step1_incoming;
drop table if exists crm_integration_anlt.tmp_ro_load_answer_step2_incoming;
drop table if exists crm_integration_anlt.tmp_ro_load_answer_step3_incoming;

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
    where proc.dsc_process_short = 't_fac_paidad_user_payment'
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
	and proc.dsc_process_short = 't_fac_paidad_user_payment';

	--$$$
	
-- #############################################
-- # 		     ATLAS - ROMANIA               #
-- #    LOADING t_fac_paidad_user_payment      #
-- #############################################

drop table if exists crm_integration_anlt.tmp_ro_load_paidad_user_payment_step1;

create table crm_integration_anlt.tmp_ro_load_paidad_user_payment_step1 
distkey(opr_atlas_user)
sortkey(opr_atlas_user, opr_paidad_user_payment, dml_type, cod_source_system)
as
  select source.*, coalesce(lkp_paidad_index.cod_paidad_index,-2) cod_paidad_index, coalesce(lkp_ad.cod_ad,-2) cod_ad
	from
		(
	  select
		source_table.opr_paidad_user_payment,
		source_table.dsc_paidad_user_payment,
		source_table.operation_timestamp,
		source_table.opr_payment_session,
		cast(to_char(trunc(source_table.dat_paidad_user_payment) , 'YYYYMMDD') as bigint) dat_paidad_user_payment, -- yyyymmdd int
		source_table.dat_paidad_user_payment dat_payment, -- timestamp
		source_table.val_price,
		source_table.dat_valid_to,
		source_table.flg_renewed,
		source_table.id_newsletter,
		source_table.val_current_credits,
		source_table.flg_invoice_sent,
		source_table.flg_money_back_on_bank_account,
		source_table.id_invoice,
		source_table.id_invoice_sap,
		source_table.flg_removed_from_invoice,
		source_table.flg_invalid_item,
		source_table.flg_vas,
		source_table.sap_id_invoice,
		source_table.migration_data,
		source_table.flg_migrated,
		source_table.hash_paidad_user_payment,
		source_table.cod_source_system,
		coalesce(lkp_payment_provider.cod_payment_provider,-2) cod_payment_provider,
		--lkp_paidad_index.cod_paidad_index,
		source_table.opr_paidad_index,
		--lkp_ad.cod_ad,
		source_table.opr_ad,
		--lkp_atlas_user.cod_atlas_user,
		source_table.opr_atlas_user,
		source_table.cod_execution,
		max_cod_paidad_user_payment.max_cod,
		row_number() over (order by source_table.opr_paidad_user_payment desc) new_cod,
		target.cod_paidad_user_payment,
		case
			when target.cod_paidad_user_payment is null then 'I'
			when source_table.hash_paidad_user_payment != target.hash_paidad_user_payment then 'U'
			else 'X'
		end dml_type
  from
    (
      SELECT
		source.*,
		lkp_source_system.cod_source_system,
		md5
		(
			coalesce(opr_atlas_user                          ,0) +
			coalesce(opr_payment_session   				,0) +
			coalesce(opr_ad                                  ,0) +
			coalesce(dsc_paidad_user_payment             ,'') +
			cast(coalesce(val_price                          ,0) as varchar) +
			coalesce(dat_paidad_user_payment               ,'2099-12-31 00:00:00.000000') +
			coalesce(dat_valid_to                            ,'2099-12-31 00:00:00.000000') +
			coalesce(opr_payment_provider                    ,'') +
			coalesce(opr_paidad_index                       ,0) +
			coalesce(flg_renewed                             ,0) +
			coalesce(id_newsletter                           ,0) +
			cast(coalesce(val_current_credits                ,0) as varchar) +
			coalesce(flg_invoice_sent                        ,0) +
			coalesce(flg_money_back_on_bank_account          ,0) +
			coalesce(id_invoice                              ,0) +
			coalesce(id_invoice_sap                          ,0) +
			coalesce(flg_removed_from_invoice                ,0) +
			coalesce(flg_invalid_item                        ,0) +
			coalesce(flg_vas                                 ,0) +
			coalesce(sap_id_invoice                          ,0) +
			coalesce(migration_data                          ,'') +
			coalesce(flg_migrated                            ,0)
	    ) hash_paidad_user_payment
	  FROM
	  (
      SELECT
        id opr_paidad_user_payment,
			  livesync_dbname opr_source_system,
			  operation_timestamp,
			  id_user opr_atlas_user,
			  id_transaction opr_payment_session,
			  id_ad opr_ad,
			  name dsc_paidad_user_payment,
			  price val_price,
			  date dat_paidad_user_payment,
			  paidads_valid_to dat_valid_to,
			  payment_provider opr_payment_provider,
			  id_index opr_paidad_index,
			  is_renewed flg_renewed,
			  id_newsletter,
			  current_credits val_current_credits,
			  invoice_sent flg_invoice_sent,
			  money_back_on_bank_account flg_money_back_on_bank_account,
			  id_invoice,
			  id_invoice_sap,
			  is_removed_from_invoice flg_removed_from_invoice,
			  is_invalid_item flg_invalid_item,
			  is_vas flg_vas,
			  sap_id_invoice,
			  migration_data,
			  migrated flg_migrated,
			  row_number() over (partition by id order by operation_type desc) rn,
        scai_execution.cod_execution
            FROM
              crm_integration_stg.stg_ro_db_atlas_verticals_paidads_user_payments a,
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
                and proc.dsc_process_short = 't_fac_paidad_user_payment'
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
        id opr_paidad_user_payment,
			  'olxro' opr_source_system,
			  operation_timestamp,
			  id_user opr_atlas_user,
			  id_transaction opr_payment_session,
			  id_ad opr_ad,
			  name dsc_paidad_user_payment,
			  price val_price,
			  date dat_paidad_user_payment,
			  paidads_valid_to dat_valid_to,
			  payment_provider opr_payment_provider,
			  id_index opr_paidad_index,
			  is_renewed flg_renewed,
			  cast(null as bigint) id_newsletter,
			  current_credits val_current_credits,
			  null flg_invoice_sent,
			  null flg_money_back_on_bank_account,
			  null id_invoice,
			  null id_invoice_sap,
			  null flg_removed_from_invoice,
			  null flg_invalid_item,
			  null flg_vas,
			  null sap_id_invoice,
			  null migration_data,
			  null flg_migrated,
			  row_number() over (partition by id order by operation_type desc) rn,
        scai_execution.cod_execution
            FROM
              crm_integration_stg.stg_ro_db_atlas_olxro_paidads_user_payments,
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
                  and proc.dsc_process_short = 't_fac_paidad_user_payment'
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
	  crm_integration_anlt.t_lkp_payment_provider lkp_payment_provider,
    (select coalesce(max(cod_paidad_user_payment),0) max_cod from crm_integration_anlt.t_fac_paidad_user_payment) max_cod_paidad_user_payment,
    crm_integration_anlt.t_fac_paidad_user_payment target
  where
    coalesce(source_table.opr_paidad_user_payment,-1) = target.opr_paidad_user_payment(+)
	and source_table.cod_source_system = target.cod_source_system (+)
    and coalesce(source_table.opr_payment_provider,'Unknown') = lkp_payment_provider.opr_payment_provider
    and lkp_payment_provider.valid_to = 20991231
    and source_table.rn = 1
) source,
	crm_integration_anlt.t_lkp_paidad_index lkp_paidad_index,
	crm_integration_anlt.t_lkp_ad lkp_ad
where
	coalesce(source.opr_paidad_index,-1) = lkp_paidad_index.opr_paidad_index(+)
	and source.cod_source_system = lkp_paidad_index.cod_source_system(+) -- new
    and lkp_paidad_index.valid_to(+) = 20991231
    and coalesce(source.opr_ad,-1) = lkp_ad.opr_ad(+)
	and source.cod_source_system = lkp_ad.cod_source_system(+) -- new
    and lkp_ad.valid_to(+) = 20991231;

analyze crm_integration_anlt.tmp_ro_load_paidad_user_payment_step1;

drop table if exists crm_integration_anlt.tmp_ro_load_paidad_user_payment_step2;

create table crm_integration_anlt.tmp_ro_load_paidad_user_payment_step2
distkey(opr_paidad_user_payment)
sortkey(opr_paidad_user_payment, dml_type, cod_source_system)
as
	select
		source.*, coalesce(lkp_atlas_user.cod_atlas_user,-2) cod_atlas_user
	from
		crm_integration_anlt.tmp_ro_load_paidad_user_payment_step1 source,
		crm_integration_anlt.t_lkp_atlas_user lkp_atlas_user
	where
    coalesce(source.opr_atlas_user,-1) = lkp_atlas_user.opr_atlas_user (+)
		and source.cod_source_system = lkp_atlas_user.cod_source_system (+) -- new
    and lkp_atlas_user.valid_to(+) = 20991231;

analyze crm_integration_anlt.tmp_ro_load_paidad_user_payment_step2;
	
	--$$$
	
insert into crm_integration_anlt.t_hst_paidad_user_payment
    select
      target.*
    from
      crm_integration_anlt.t_fac_paidad_user_payment target,
      crm_integration_anlt.tmp_ro_load_paidad_user_payment_step2 source
    where
      target.cod_paidad_user_payment = source.cod_paidad_user_payment
      and source.dml_type = 'U';

	--$$$
	
delete from crm_integration_anlt.t_fac_paidad_user_payment
using crm_integration_anlt.tmp_ro_load_paidad_user_payment_step2
where crm_integration_anlt.t_fac_paidad_user_payment.cod_paidad_user_payment=crm_integration_anlt.tmp_ro_load_paidad_user_payment_step2.cod_paidad_user_payment
and crm_integration_anlt.tmp_ro_load_paidad_user_payment_step2.dml_type = 'U';

	--$$$
	
insert into crm_integration_anlt.t_fac_paidad_user_payment
    select
      case
        when dml_type = 'I' then max_cod + new_cod
        when dml_type = 'U' then cod_paidad_user_payment
      end cod_paidad_user_payment,
	  dat_paidad_user_payment,
	  opr_paidad_user_payment,
	  dsc_paidad_user_payment,
	  opr_payment_session,
	  cod_source_system,
	  cod_atlas_user,
	  cod_ad,
	  val_price,
	  dat_payment,
	  dat_valid_to,
	  cod_paidad_index,
	  flg_renewed,
	  cod_payment_provider,
	  id_newsletter,
	  val_current_credits,
	  flg_invoice_sent,
	  flg_money_back_on_bank_account,
	  id_invoice,
	  id_invoice_sap,
	  flg_removed_from_invoice,
	  flg_invalid_item,
	  flg_vas,
	  sap_id_invoice,
	  flg_migrated,
	  migration_data,
	  hash_paidad_user_payment,
	  cod_execution
    from
      crm_integration_anlt.tmp_ro_load_paidad_user_payment_step2
    where
      dml_type in ('U','I');

analyze crm_integration_anlt.t_fac_paidad_user_payment;
	  
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
	and proc.dsc_process_short = 't_fac_paidad_user_payment';

-- #######################
-- ####    PASSO 6    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set cod_status = 1, -- Ok
last_processing_datetime = coalesce((select max(operation_timestamp) from crm_integration_anlt.tmp_ro_load_paidad_user_payment_step2),last_processing_datetime)
/*from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc, crm_integration_anlt.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_fac_paidad_user_payment'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 4
  ) source*/
from crm_integration_anlt.t_lkp_scai_process proc 
where t_rel_scai_integration_process.cod_process = proc.cod_process
and t_rel_scai_integration_process.cod_status = 2
and t_rel_scai_integration_process.cod_country = 4
and proc.dsc_process_short = 't_fac_paidad_user_payment'
and t_rel_scai_integration_process.ind_active = 1
/*crm_integration_anlt.t_rel_scai_integration_process.cod_process = source.cod_process
and crm_integration_anlt.t_rel_scai_integration_process.cod_country = source.cod_country
and crm_integration_anlt.t_rel_scai_integration_process.cod_integration = source.cod_integration*/;

drop table if exists crm_integration_anlt.tmp_ro_load_paidad_user_payment_step1;
drop table if exists crm_integration_anlt.tmp_ro_load_paidad_user_payment_step2;

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
    where proc.dsc_process_short = 't_fac_payment_session'
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
	and proc.dsc_process_short = 't_fac_payment_session';

	--$$$
	
-- #############################################
-- # 		     ATLAS - ROMANIA               #
-- #       LOADING t_fac_payment_session       #
-- #############################################

drop table if exists crm_integration_anlt.tmp_ro_load_payment_session;

create table crm_integration_anlt.tmp_ro_load_payment_session 
distkey(cod_payment_session)
sortkey(cod_payment_session, dml_type, cod_source_system)
as
  select
    source_table.opr_payment_session,
	source_table.operation_timestamp,
	source_table.created_at,
	source_table.last_status_date,
	source_table.ip,
	source_table.external_id,
	source_table.request,
	source_table.message,
    source_table.additional_data,
    source_table.migration_data,
    source_table.flg_migrated,
    source_table.hash_payment_session,
    source_table.cod_source_system,
    source_table.cod_execution,
    coalesce(lkp_payment_provider.cod_payment_provider,-2) cod_payment_provider,
    coalesce(lkp_payment_status.cod_payment_status,-2) cod_payment_status,
    coalesce(lkp_source.cod_source,-2) cod_source,
    max_cod_payment_session.max_cod,
    row_number() over (order by source_table.opr_payment_session desc) new_cod,
    target.cod_payment_session,
    case
      when target.cod_payment_session is null then 'I'
      when source_table.hash_payment_session != target.hash_payment_session then 'U'
        else 'X'
    end dml_type
  from
    (
      SELECT
		source.*,
		lkp_source_system.cod_source_system,
		md5
		(
			--coalesce(created_at                            ,'2099-12-31 00:00:00.000000') +
			--coalesce(last_status_date                      ,'2099-12-31 00:00:00.000000') +
			coalesce(ip                                      ,0) +
			coalesce(opr_payment_status                      ,'') +
			coalesce(opr_payment_provider                    ,'') +
			coalesce(external_id                             ,0) +
			coalesce(request                                 ,'')+
			coalesce(message                                 ,'') +
			coalesce(opr_source                              ,'') +
			coalesce(additional_data                         ,'') +
			coalesce(migration_data                          ,'') +
			coalesce(flg_migrated                            ,0)
	    ) hash_payment_session
	  FROM
	  (
            SELECT
				id opr_payment_session,
				livesync_dbname opr_source_system,
				operation_timestamp,
				created_at,
				last_status_date,
				ip,
				status opr_payment_status,
				provider opr_payment_provider,
				external_id,
				request,
				message,
				source opr_source,
				additional_data,
				migration_data,
				migrated flg_migrated,
				row_number() over (partition by id order by operation_type desc) rn,
				scai_execution.cod_execution
            FROM
            crm_integration_stg.stg_ro_db_atlas_verticals_payment_session a,
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
                and proc.dsc_process_short = 't_fac_payment_session'
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
				id opr_payment_session,
				'olxro' opr_source_system,
				operation_timestamp,
				created_at,
				last_status_date,
				ip,
				status opr_payment_status,
				provider opr_payment_provider,
				external_id,
				request,
				message,
				source opr_source,
				null additional_data,
				null migration_data,
				null flg_migrated,
				row_number() over (partition by id order by operation_type desc) rn,
				scai_execution.cod_execution
			FROM
				crm_integration_stg.stg_ro_db_atlas_olxro_payment_session,
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
            and proc.dsc_process_short = 't_fac_payment_session'
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
    crm_integration_anlt.t_lkp_source lkp_source,
    crm_integration_anlt.t_lkp_payment_status lkp_payment_status,
	crm_integration_anlt.t_lkp_payment_provider lkp_payment_provider,
    (select coalesce(max(cod_payment_session),0) max_cod from crm_integration_anlt.t_fac_payment_session) max_cod_payment_session,
    crm_integration_anlt.t_fac_payment_session target
  where
    coalesce(source_table.opr_payment_session,-1) = target.opr_payment_session(+)
	and source_table.cod_source_system = target.cod_source_system (+)
    and coalesce(source_table.opr_payment_provider,'Unknown') = lkp_payment_provider.opr_payment_provider
	and lkp_payment_provider.valid_to = 20991231
    and coalesce(source_table.opr_payment_status,'Unknown') = lkp_payment_status.opr_payment_status
    and lkp_payment_status.valid_to = 20991231
    and coalesce(source_table.opr_source,'Unknown') = lkp_source.opr_source
    and lkp_source.valid_to = 20991231
	and source_table.rn = 1;

analyze crm_integration_anlt.tmp_ro_load_payment_session;
	
	--$$$
	
insert into crm_integration_anlt.t_hst_payment_session
    select
      target.*
    from
      crm_integration_anlt.t_fac_payment_session target,
      crm_integration_anlt.tmp_ro_load_payment_session source
    where
      target.cod_payment_session = source.cod_payment_session
      and source.dml_type = 'U';

	--$$$
	
delete from crm_integration_anlt.t_fac_payment_session
using crm_integration_anlt.tmp_ro_load_payment_session
where crm_integration_anlt.t_fac_payment_session.cod_payment_session=crm_integration_anlt.tmp_ro_load_payment_session.cod_payment_session
and crm_integration_anlt.tmp_ro_load_payment_session.dml_type = 'U';

	--$$$
	
insert into crm_integration_anlt.t_fac_payment_session
    select
      case
        when dml_type = 'I' then max_cod + new_cod
        when dml_type = 'U' then cod_payment_session
      end cod_payment_session,
	  opr_payment_session,
	  created_at,
	  last_status_date,
	  ip,
	  cod_payment_status,
	  cod_payment_provider,
	  external_id,
	  request,
	  message,
	  cod_source,
	  additional_data,
	  migration_data,
	  flg_migrated,
	  cod_source_system,
	  hash_payment_session,
	  cod_execution
    from
      crm_integration_anlt.tmp_ro_load_payment_session
    where
      dml_type in ('U','I');

analyze crm_integration_anlt.t_fac_payment_session;
	  
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
	and proc.dsc_process_short = 't_fac_payment_session';

-- #######################
-- ####    PASSO 6    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set cod_status = 1, -- Ok
last_processing_datetime = coalesce((select max(operation_timestamp) from crm_integration_anlt.tmp_ro_load_payment_session),last_processing_datetime)
/*from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc, crm_integration_anlt.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_fac_payment_session'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 4
  ) source*/
from crm_integration_anlt.t_lkp_scai_process proc 
where t_rel_scai_integration_process.cod_process = proc.cod_process
and t_rel_scai_integration_process.cod_status = 2
and t_rel_scai_integration_process.cod_country = 4
and proc.dsc_process_short = 't_fac_payment_session'
and t_rel_scai_integration_process.ind_active = 1
/*crm_integration_anlt.t_rel_scai_integration_process.cod_process = source.cod_process
and crm_integration_anlt.t_rel_scai_integration_process.cod_country = source.cod_country
and crm_integration_anlt.t_rel_scai_integration_process.cod_integration = source.cod_integration*/;

drop table if exists crm_integration_anlt.tmp_ro_load_payment_session;

	--$$$ -- 160
	
-- #######################
-- ####    PASSO 3    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set dat_processing = source.dat_processing, execution_nbr = source.execution_nbr, cod_status = 2 -- Running
from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc, crm_integration_anlt.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_fac_payment_basket'
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
	and proc.dsc_process_short = 't_fac_payment_basket';

	--$$$
	
-- #############################################
-- # 		     ATLAS - ROMANIA               #
-- #       LOADING t_fac_payment_basket        #
-- #############################################

drop table if exists crm_integration_anlt.tmp_ro_load_payment_basket_step1;

create table crm_integration_anlt.tmp_ro_load_payment_basket_step1 
distkey(opr_atlas_user)
sortkey(opr_atlas_user, cod_payment_basket, dml_type, cod_source_system)
as
  select source.*,
    coalesce(lkp_payment_session.cod_payment_session,-2) cod_payment_session,
    coalesce(lkp_paidad_index.cod_paidad_index,-2) cod_paidad_index,
    coalesce(lkp_ad.cod_ad,-2) cod_ad
  from
    (
	select
    source_table.opr_payment_basket,
    --source_table.opr_source_system,
    --source_table.opr_payment_session,
    --source_table.opr_payment_index,
    --source_table.opr_ad,
    --source_table.opr_atlas_user,
	source_table.operation_timestamp,
    source_table.test_id,
    source_table.test_group_id,
    source_table.price,
    source_table.from_account,
    source_table.flg_refunded,
    source_table.flg_used,
    source_table.flg_cleared,
    source_table.extra_params,
    source_table.update_at,
    source_table.viewed_test_id,
    source_table.viewed_test_group_id,
    source_table.migration_data,
    source_table.flg_migrated,
    source_table.from_bonus_credits,
    source_table.from_refund_credits,
    source_table.hash_payment_basket,
    source_table.cod_source_system,
    --lkp_payment_session.cod_payment_session,
    source_table.opr_payment_session,
    --lkp_paidad_index.cod_paidad_index,
    source_table.opr_payment_index,
    --lkp_ad.cod_ad,
    source_table.opr_ad,
    --lkp_atlas_user.cod_atlas_user,
    source_table.opr_atlas_user,
    source_table.cod_execution,
    max_cod_payment_basket.max_cod,
    row_number() over (order by source_table.opr_payment_basket desc) new_cod,
    target.cod_payment_basket,
    case
      when target.cod_payment_basket is null then 'I'
      when source_table.hash_payment_basket != target.hash_payment_basket then 'U'
        else 'X'
    end dml_type
  from
    (
      select
        source.*,
		lkp_source_system.cod_source_system,
		md5
        (coalesce(opr_payment_session,0) + coalesce(opr_payment_index,0) + coalesce(opr_ad,0) + coalesce(opr_atlas_user,0)) hash_payment_basket
    from
      (
            SELECT
              id opr_payment_basket,
              livesync_dbname opr_source_system,
			  operation_timestamp,
              session_id opr_payment_session,
              index_id opr_payment_index,
              ad_id opr_ad,
              user_id opr_atlas_user,
              test_id,
              test_group_id,
              price,
              from_account,
              refunded flg_refunded,
              used flg_used,
              cleared flg_cleared,
              extra_params,
              update_at,
              viewed_test_id,
              viewed_test_group_id,
              migration_data,
              migrated flg_migrated,
              cast(null as numeric(8,2)) from_bonus_credits,
              cast(null as numeric(8,2)) from_refund_credits,
              row_number() over (partition by id order by operation_type desc) rn,
              scai_execution.cod_execution
            FROM
              crm_integration_stg.stg_ro_db_atlas_verticals_payment_basket a,
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
                  and proc.dsc_process_short = 't_fac_payment_basket'
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
        id opr_payment_basket,
        'olxro' opr_source_system,
		operation_timestamp,
        session_id opr_payment_session,
        index_id opr_payment_index,
        ad_id opr_ad,
        user_id opr_atlas_user,
        test_id,
        test_group_id,
        price,
        from_account,
        refunded flg_refunded,
        used flg_used,
        cleared flg_cleared,
        extra_params,
        null update_at,
        null viewed_test_id,
        null viewed_test_group_id,
        null migration_data,
        null flg_migrated,
        from_bonus_credits,
        from_refund_credits,
        row_number() over (partition by id order by operation_type desc) rn,
        scai_execution.cod_execution
    FROM
        crm_integration_stg.stg_ro_db_atlas_olxro_payment_basket,
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
            and proc.dsc_process_short = 't_fac_payment_basket'
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
    (select coalesce(max(cod_payment_basket),0) max_cod from crm_integration_anlt.t_fac_payment_basket) max_cod_payment_basket,
    crm_integration_anlt.t_fac_payment_basket target
  where
    coalesce(source_table.opr_payment_basket,-1) = target.opr_payment_basket(+)
	and source_table.cod_source_system = target.cod_source_system (+)
	and source_table.rn = 1
) source,
    crm_integration_anlt.t_fac_payment_session lkp_payment_session,
    crm_integration_anlt.t_lkp_paidad_index lkp_paidad_index,
    crm_integration_anlt.t_lkp_ad lkp_ad
  where
    coalesce(source.opr_payment_session,-1) = lkp_payment_session.opr_payment_session (+)
	and source.cod_source_system = lkp_payment_session.cod_source_system (+) -- new
    and coalesce(source.opr_payment_index,-1) = lkp_paidad_index.opr_paidad_index (+)
	and source.cod_source_system = lkp_paidad_index.cod_source_system (+) -- new
    and lkp_paidad_index.valid_to (+) = 20991231
    and coalesce(source.opr_ad,-1) = lkp_ad.opr_ad (+)
	and source.cod_source_system = lkp_ad.cod_source_system (+) -- new
    and lkp_ad.valid_to (+) = 20991231;

analyze crm_integration_anlt.tmp_ro_load_payment_basket_step1;

drop table if exists crm_integration_anlt.tmp_ro_load_payment_basket_step2;

create table crm_integration_anlt.tmp_ro_load_payment_basket_step2
distkey(cod_payment_basket)
sortkey(cod_payment_basket, dml_type, cod_source_system)
as
select
	source.*, coalesce(lkp_atlas_user.cod_atlas_user,-2) cod_atlas_user
from
	crm_integration_anlt.tmp_ro_load_payment_basket_step1 source,
    crm_integration_anlt.t_lkp_atlas_user lkp_atlas_user
where
	coalesce(source.opr_atlas_user,-1) = lkp_atlas_user.opr_atlas_user (+)
	and source.cod_source_system = lkp_atlas_user.cod_source_system (+) -- new
    and lkp_atlas_user.valid_to (+) = 20991231;
	
analyze crm_integration_anlt.tmp_ro_load_payment_basket_step2;
	
	--$$$
	
insert into crm_integration_anlt.t_hst_payment_basket
    select
      target.*
    from
      crm_integration_anlt.t_fac_payment_basket target,
      crm_integration_anlt.tmp_ro_load_payment_basket_step2 source
    where
      target.cod_payment_basket = source.cod_payment_basket
      and source.dml_type = 'U';

	--$$$
	
delete from crm_integration_anlt.t_fac_payment_basket
using crm_integration_anlt.tmp_ro_load_payment_basket_step2
where crm_integration_anlt.t_fac_payment_basket.cod_payment_basket=crm_integration_anlt.tmp_ro_load_payment_basket_step2.cod_payment_basket
and crm_integration_anlt.tmp_ro_load_payment_basket_step2.dml_type = 'U';

	--$$$
	
insert into crm_integration_anlt.t_fac_payment_basket
    select
      case
        when dml_type = 'I' then max_cod + new_cod
        when dml_type = 'U' then cod_payment_basket
      end cod_payment_basket,
      opr_payment_basket,
      cod_payment_session,
      cod_paidad_index,
      cod_ad,
      cod_source_system,
      cod_atlas_user,
      test_id,
      test_group_id,
      price,
      from_account,
      flg_refunded,
      flg_used,
      flg_cleared,
      extra_params,
      update_at,
      viewed_test_id,
      viewed_test_group_id,
      migration_data,
      flg_migrated,
      from_bonus_credits,
      from_refund_credits,
      hash_payment_basket,
	  cod_execution
    from
      crm_integration_anlt.tmp_ro_load_payment_basket_step2
    where
      dml_type in ('U','I');

analyze crm_integration_anlt.t_fac_payment_basket;
	  
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
	and proc.dsc_process_short = 't_fac_payment_basket';

-- #######################
-- ####    PASSO 6    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set cod_status = 1, -- Ok
last_processing_datetime = coalesce((select max(operation_timestamp) from crm_integration_anlt.tmp_ro_load_payment_basket_step2),last_processing_datetime)
/*from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc, crm_integration_anlt.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_fac_payment_basket'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 4
  ) source*/
from crm_integration_anlt.t_lkp_scai_process proc 
where t_rel_scai_integration_process.cod_process = proc.cod_process
and t_rel_scai_integration_process.cod_status = 2
and t_rel_scai_integration_process.cod_country = 4
and proc.dsc_process_short = 't_fac_payment_basket'
and t_rel_scai_integration_process.ind_active = 1
/*crm_integration_anlt.t_rel_scai_integration_process.cod_process = source.cod_process
and crm_integration_anlt.t_rel_scai_integration_process.cod_country = source.cod_country
and crm_integration_anlt.t_rel_scai_integration_process.cod_integration = source.cod_integration*/;

drop table if exists crm_integration_anlt.tmp_ro_load_payment_basket_step1;
drop table if exists crm_integration_anlt.tmp_ro_load_payment_basket_step2;

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
    where proc.dsc_process_short = 't_fac_web'
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
	and proc.dsc_process_short = 't_fac_web';

	--$$$
	
-- #############################################
-- #     HYDRA - ROMANIA                       #
-- #       LOADING t_fac_web                   #
-- #############################################

create table crm_integration_anlt.tmp_ro_load_web_step1
distkey(opr_ad)
sortkey(opr_ad, opr_event, cod_source_system)
  as
      select
        *,
        null hash_web
      from
        (
          SELECT
            cast(to_char(a.server_date_day,'yyyymmdd') as int) server_date_day,
            server_date_day server_date_day_datetime,
            a.ad_id opr_ad,
            a.trackname opr_event,
            a.occurrences,
            a.distinct_occurrences,
            b.cod_source_system,
            scai_execution.cod_execution
          FROM
            crm_integration_stg.stg_ro_hydra_verticals_web a,
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
                and proc.dsc_process_short = 't_fac_web'
                and fac.cod_process = rel_integr_proc.cod_process
                and fac.cod_integration = rel_integr_proc.cod_integration
                and rel_integr_proc.dat_processing = fac.dat_processing
                and fac.cod_status = 2
            ) scai_execution
          where
            a.source = b.opr_source_system
            and b.cod_business_type = 1 -- Verticals
            and b.cod_country = 4 -- Romania
            --and 1 = 0
          union all
          SELECT
            cast(to_char(a.server_date_day,'yyyymmdd') as int) server_date_day,
            server_date_day server_date_day_datetime,
            a.ad_id opr_ad,
            a.action_type opr_event,
            a.occurrences,
            a.distinct_occurrences,
            b.cod_source_system,
            scai_execution.cod_execution
          FROM
            crm_integration_stg.stg_ro_hydra_web a,
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
                and proc.dsc_process_short = 't_fac_web'
                and fac.cod_process = rel_integr_proc.cod_process
                and fac.cod_integration = rel_integr_proc.cod_integration
                and rel_integr_proc.dat_processing = fac.dat_processing
                and fac.cod_status = 2
            ) scai_execution
          where
            a.source = b.opr_source_system
            and b.cod_business_type = 2 -- Horizontal
            and b.cod_country = 4 -- Romania
           --and 1 = 0
        );

analyze crm_integration_anlt.tmp_ro_load_web_step1;
		
create table crm_integration_anlt.tmp_ro_load_web
distkey(dat_event)
sortkey(dat_event, cod_source_system)
as
  select
    source_table.server_date_day dat_event,
    source_table.server_date_day_datetime,
    source_table.cod_source_system,
    coalesce(lkp_ad.cod_ad,-2) cod_ad,
    coalesce(lkp_event.cod_event,-2) cod_event,
    source_table.occurrences,
    source_table.distinct_occurrences,
    source_table.hash_web,
	source_table.cod_execution
  from
    crm_integration_anlt.tmp_ro_load_web_step1 source_table,
    crm_integration_anlt.t_lkp_event lkp_event,
    crm_integration_anlt.t_lkp_ad lkp_ad
  where
    coalesce(source_table.opr_ad,-1) = lkp_ad.opr_ad(+)
    and source_table.cod_source_system = lkp_ad.cod_source_system(+)
    and lkp_ad.valid_to(+) = 20991231
    and coalesce(source_table.opr_event,'Unknown') = lkp_event.opr_event(+)
    and lkp_event.valid_to(+) = 20991231;

analyze crm_integration_anlt.tmp_ro_load_web;
	
delete from crm_integration_anlt.t_fac_web
where crm_integration_anlt.t_fac_web.dat_event in (select distinct dat_event from crm_integration_anlt.tmp_ro_load_web)
and crm_integration_anlt.t_fac_web.cod_source_system in (select distinct cod_source_system from crm_integration_anlt.tmp_ro_load_web);

insert into crm_integration_anlt.t_fac_web
    select
      cod_ad,
      dat_event,
      cod_event,
      cod_source_system,
      occurrences,
      distinct_occurrences,
      hash_web,
	  cod_execution
    from
      crm_integration_anlt.tmp_ro_load_web;

analyze crm_integration_anlt.t_fac_web;

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
	and proc.dsc_process_short = 't_fac_web';

	
-- #######################
-- ####    PASSO 6    ####
-- #######################
update crm_integration_anlt.t_rel_scai_integration_process
set cod_status = 1, -- Ok
last_processing_datetime = coalesce((select max(server_date_day_datetime) from crm_integration_anlt.tmp_ro_load_web),last_processing_datetime)
/*from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc, crm_integration_anlt.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_fac_payment_basket'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 4
  ) source*/
from crm_integration_anlt.t_lkp_scai_process proc 
where t_rel_scai_integration_process.cod_process = proc.cod_process
and t_rel_scai_integration_process.cod_status = 2
and t_rel_scai_integration_process.cod_country = 4
and proc.dsc_process_short = 't_fac_web'
and t_rel_scai_integration_process.ind_active = 1
/*crm_integration_anlt.t_rel_scai_integration_process.cod_process = source.cod_process
and crm_integration_anlt.t_rel_scai_integration_process.cod_country = source.cod_country
and crm_integration_anlt.t_rel_scai_integration_process.cod_integration = source.cod_integration*/;

drop table if exists crm_integration_anlt.tmp_ro_load_web_step1;

drop table if exists crm_integration_anlt.tmp_ro_load_web;

	--$$$
	
-- #######################
-- ####    PASSO 7    ####
-- #######################
insert into crm_integration_anlt.t_fac_scai_execution
  select
    max_cod_exec + 1 cod_execution,
    cod_country,
    cod_integration,
    -1 cod_process,
    1 cod_status, -- Ok
    2 cod_execution_type, -- End
    dat_processing,
    execution_nbr,
    sysdate
  from
    crm_integration_anlt.t_rel_scai_country_integration,
    (select coalesce(max(cod_execution),0) max_cod_exec from crm_integration_anlt.t_fac_scai_execution)
  where
    cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and cod_country = 4 -- Romania
	and ind_active = 1;

-- #######################
-- ####    PASSO 8    ####
-- #######################
update crm_integration_anlt.t_rel_scai_country_integration
    set
      cod_status = 1 -- Ok
where
    cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and cod_country = 4 -- Romania
	and ind_active = 1; 