-- #######################
-- ####    PASSO 1    ####
-- #######################
update sandbox_andre_matias.t_rel_scai_country_integration
    set dat_processing = cast(to_char(trunc(sysdate),'yyyymmdd') as int),
      execution_nbr = case
                        when trunc(sysdate) - to_date(dat_processing,'yyyymmdd') > 1 then 1
                          else execution_nbr + 1
                      end,
      cod_status = 2 -- Running
where
    cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and cod_country = 1 -- Portugal
	and ind_active = 1; 

-- #######################
-- ####    PASSO 2    ####
-- #######################
insert into sandbox_andre_matias.t_fac_scai_execution
  select
    max_cod_exec + 1 cod_execution,
    cod_integration,
    -1 cod_process,
    2 cod_status, -- Running
    1 cod_execution_type, -- Begin
    dat_processing,
    execution_nbr,
    sysdate
  from
    sandbox_andre_matias.t_rel_scai_country_integration,
    (select isnull(max(cod_execution),0) max_cod_exec from sandbox_andre_matias.t_fac_scai_execution)
  where
    cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and cod_country = 1 -- Portugal
	and ind_active = 1;

$$$
	
-- #######################
-- ####    PASSO 3    ####
-- #######################
update sandbox_andre_matias.t_rel_scai_integration_process
set dat_processing = source.dat_processing, execution_nbr = source.execution_nbr, cod_status = 2 -- Running
from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc, sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_base_source'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 1
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
  ) source
where sandbox_andre_matias.t_rel_scai_integration_process.cod_process = source.cod_process
and sandbox_andre_matias.t_rel_scai_integration_process.cod_country = source.cod_country
and sandbox_andre_matias.t_rel_scai_integration_process.cod_integration = source.cod_integration;

-- #######################
-- ####    PASSO 4    ####
-- #######################
insert into sandbox_andre_matias.t_fac_scai_execution
  select
    max_cod_exec + 1 cod_execution,
    rel_integr_proc.cod_integration,
    rel_integr_proc.cod_process,
    rel_integr_proc.cod_status,
    1 cod_execution_type, -- Begin
    rel_integr_proc.dat_processing,
    rel_integr_proc.execution_nbr,
    sysdate
  from
    sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr,
    (select isnull(max(cod_execution),0) max_cod_exec from sandbox_andre_matias.t_fac_scai_execution),
    sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc,
    sandbox_andre_matias.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 1 -- Portugal
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_lkp_base_source';		

$$$	

-- #############################################
-- # 			 BASE - Portugal               #
-- #		 LOADING t_lkp_base_source         #
-- #############################################

drop table if exists sandbox_andre_matias.tmp_load_base_source;

create table sandbox_andre_matias.tmp_load_base_source 
distkey(cod_source_system)
sortkey(cod_base_source, opr_base_source)
as
  select
    source_table.opr_base_source,
    source_table.dsc_base_source,
    lkp_resource_type.cod_resource_type,
    source_table.cod_source_system,
	source_table.meta_event_type,
	source_table.meta_event_time,
    source_table.created_at,
    source_table.updated_at,
    source_table.hash_base_source,
    max_cod_base_source.max_cod,
    row_number() over (order by source_table.opr_base_source desc) new_cod,
    target.cod_base_source,
    case
      --when target.cod_base_source is null then 'I'
	  when target.cod_base_source is null or (source_table.hash_base_source != target.hash_base_source and target.valid_from = (select dat_processing from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_base_source')) then 'I'
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
		(isnull(dsc_base_source,'') + isnull(opr_resource_type,'')) hash_base_source
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
        updated_at
      FROM
        rdl_basecrm_v2.stg_d_base_sources
	  WHERE
		updated_at >= (select isnull(rel_integr_proc.last_processing_datetime,'1900-01-01 00:00:00.000000') from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_base_source')
		--and 1 = 0
	) source,
    sandbox_andre_matias.t_lkp_source_system lkp_source_system
	where source.opr_source_system = lkp_source_system.opr_source_system
	and lkp_source_system.cod_country = 1 -- Portugal
	) source_table,
    sandbox_andre_matias.t_lkp_resource_type lkp_resource_type,
    (select isnull(max(cod_base_source),0) max_cod from sandbox_andre_matias.t_lkp_base_source) max_cod_base_source,
    sandbox_andre_matias.t_lkp_base_source target
  where
    isnull(source_table.opr_base_source,-1) = target.opr_base_source(+)
	and source_table.cod_source_system = target.cod_source_system (+)
    and target.valid_to(+) = 20991231
    and isnull(source_table.opr_resource_type,'') = lkp_resource_type.opr_resource_type
	and lkp_resource_type.valid_to = 20991231;

$$$
	
delete from sandbox_andre_matias.t_lkp_base_source
using sandbox_andre_matias.tmp_load_base_source
where 
	tmp_load_base_source.dml_type = 'I' 
	and t_lkp_base_source.opr_base_source = tmp_load_base_source.opr_base_source 
	and t_lkp_base_source.valid_from = (select dat_processing from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_base_source');

$$$
	
update sandbox_andre_matias.t_lkp_base_source
set valid_to = (select rel_integr_proc.dat_processing from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_base_source')
from sandbox_andre_matias.tmp_load_base_source source
where source.cod_base_source = sandbox_andre_matias.t_lkp_base_source.cod_base_source
and sandbox_andre_matias.t_lkp_base_source.valid_to = 20991231
and source.dml_type in('U','D');

$$$

insert into sandbox_andre_matias.t_lkp_base_source
    select
      case
        when dml_type = 'I' then max_cod + new_cod
        when dml_type = 'U' then cod_base_source
      end cod_base_source,
      opr_base_source,
      dsc_base_source,
      cod_source_system,
      (select rel_integr_proc.dat_processing from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_base_source') valid_from, 
      20991231 valid_to,
      created_at,
      updated_at,
      cod_resource_type,
      hash_base_source
    from
      sandbox_andre_matias.tmp_load_base_source
    where
      dml_type in ('U','I');

$$$
	  
-- #######################
-- ####    PASSO 5    ####
-- #######################
insert into sandbox_andre_matias.t_fac_scai_execution
  select
    max_cod_exec + 1 cod_execution,
    rel_integr_proc.cod_integration,
    rel_integr_proc.cod_process,
    1 cod_status,
    2 cod_execution_type, -- End
    rel_integr_proc.dat_processing,
    rel_integr_proc.execution_nbr,
    sysdate
  from
    sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr,
    (select isnull(max(cod_execution),0) max_cod_exec from sandbox_andre_matias.t_fac_scai_execution),
    sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc,
    sandbox_andre_matias.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 1 -- Portugal
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
update sandbox_andre_matias.t_rel_scai_integration_process
set cod_status = 1, -- Ok
last_processing_datetime = isnull((select max(updated_at) from sandbox_andre_matias.tmp_load_base_source),last_processing_datetime)
/*from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc, sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_base_source'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 1
  ) source*/
from sandbox_andre_matias.t_lkp_scai_process proc 
where t_rel_scai_integration_process.cod_process = proc.cod_process
and t_rel_scai_integration_process.cod_status = 2
and t_rel_scai_integration_process.cod_country = 1
and proc.dsc_process_short = 't_lkp_base_source'
and t_rel_scai_integration_process.ind_active = 1
/*sandbox_andre_matias.t_rel_scai_integration_process.cod_process = source.cod_process
and sandbox_andre_matias.t_rel_scai_integration_process.cod_country = source.cod_country
and sandbox_andre_matias.t_rel_scai_integration_process.cod_integration = source.cod_integration*/;

drop table if exists sandbox_andre_matias.tmp_load_base_source;

$$$

-- #######################
-- ####    PASSO 3    ####
-- #######################
update sandbox_andre_matias.t_rel_scai_integration_process
set dat_processing = source.dat_processing, execution_nbr = source.execution_nbr, cod_status = 2 -- Running
from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc, sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_base_user'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 1
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
  ) source
where sandbox_andre_matias.t_rel_scai_integration_process.cod_process = source.cod_process
and sandbox_andre_matias.t_rel_scai_integration_process.cod_country = source.cod_country
and sandbox_andre_matias.t_rel_scai_integration_process.cod_integration = source.cod_integration;

-- #######################
-- ####    PASSO 4    ####
-- #######################
insert into sandbox_andre_matias.t_fac_scai_execution
  select
    max_cod_exec + 1 cod_execution,
    rel_integr_proc.cod_integration,
    rel_integr_proc.cod_process,
    rel_integr_proc.cod_status,
    1 cod_execution_type, -- Begin
    rel_integr_proc.dat_processing,
    rel_integr_proc.execution_nbr,
    sysdate
  from
    sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr,
    (select isnull(max(cod_execution),0) max_cod_exec from sandbox_andre_matias.t_fac_scai_execution),
    sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc,
    sandbox_andre_matias.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 1 -- Portugal
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_lkp_base_user';	

$$$
	
-- #############################################
-- # 			 BASE - Portugal               #
-- #		 LOADING t_lkp_base_user          #
-- #############################################

drop table if exists sandbox_andre_matias.tmp_load_base_user;

create table sandbox_andre_matias.tmp_load_base_user 
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
    max_cod_base_user.max_cod,
    row_number() over (order by source_table.opr_base_user desc) new_cod,
    target.cod_base_user,
    case
      --when target.cod_base_user is null then 'I'
	  when target.cod_base_user is null or (source_table.hash_base_user != target.hash_base_user and target.valid_from = (select dat_processing from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_base_user')) then 'I'
	  when source_table.meta_event_type = 'deleted' then 'D'
      when source_table.hash_base_user != target.hash_base_user then 'U'
        else 'X'
    end dml_type
  from
    (
	select
		 source.*,
		lkp_source_system.cod_source_system,
		md5(isnull(dsc_base_user,'') + isnull(email,'') + isnull(role,'') + isnull(status,'') + decode(flg_confirmed, 1, 1, 0)) hash_base_user
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
        deleted_at
      FROM
        rdl_basecrm_v2.stg_d_base_users
	  WHERE
		updated_at >= (select isnull(rel_integr_proc.last_processing_datetime,'1900-01-01 00:00:00.000000') from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_base_user')
		--and 1 = 0
	) source,
    sandbox_andre_matias.t_lkp_source_system lkp_source_system
	where source.opr_source_system = lkp_source_system.opr_source_system
	and lkp_source_system.cod_country = 1
	) source_table,
    (select isnull(max(cod_base_user),0) max_cod from sandbox_andre_matias.t_lkp_base_user) max_cod_base_user,
    sandbox_andre_matias.t_lkp_base_user target
  where
    isnull(source_table.opr_base_user,-1) = target.opr_base_user(+)
	and source_table.cod_source_system = target.cod_source_system (+)
    and target.valid_to(+) = 20991231; -- Portugal

$$$
	
delete from sandbox_andre_matias.t_lkp_base_user
using sandbox_andre_matias.tmp_load_base_user
where 
	tmp_load_base_user.dml_type = 'I' 
	and t_lkp_base_user.opr_base_user = tmp_load_base_user.opr_base_user 
	and t_lkp_base_user.valid_from = (select dat_processing from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_base_user');

$$$

-- update valid_to in the updated/deleted records on source	
update sandbox_andre_matias.t_lkp_base_user
set valid_to = (select rel_integr_proc.dat_processing from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_base_user') 
from sandbox_andre_matias.tmp_load_base_user source
where source.cod_base_user = sandbox_andre_matias.t_lkp_base_user.cod_base_user
and sandbox_andre_matias.t_lkp_base_user.valid_to = 20991231
and source.dml_type in('U','D');

$$$

insert into sandbox_andre_matias.t_lkp_base_user
    select
      case
        when dml_type = 'I' then max_cod + new_cod
        when dml_type = 'U' then cod_base_user
      end cod_base_user,
      opr_base_user,
      dsc_base_user,
      cod_source_system,
      (select rel_integr_proc.dat_processing from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_base_user') valid_from, 
      20991231 valid_to,
      email,
      role,
      status,
      decode(flg_confirmed,1,1,0) flg_confirmed,
      created_at,
      updated_at,
      deleted_at,
      hash_base_user
    from
      sandbox_andre_matias.tmp_load_base_user
    where
      dml_type in ('U','I');

$$$

-- #######################
-- ####    PASSO 5    ####
-- #######################
insert into sandbox_andre_matias.t_fac_scai_execution
  select
    max_cod_exec + 1 cod_execution,
    rel_integr_proc.cod_integration,
    rel_integr_proc.cod_process,
    1 cod_status,
    2 cod_execution_type, -- End
    rel_integr_proc.dat_processing,
    rel_integr_proc.execution_nbr,
    sysdate
  from
    sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr,
    (select isnull(max(cod_execution),0) max_cod_exec from sandbox_andre_matias.t_fac_scai_execution),
    sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc,
    sandbox_andre_matias.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 1 -- Portugal
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
update sandbox_andre_matias.t_rel_scai_integration_process
set cod_status = 1, -- Ok
last_processing_datetime = isnull((select max(updated_at) from sandbox_andre_matias.tmp_load_base_user),last_processing_datetime)
/*from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc, sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_base_user'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 1
  ) source*/
from sandbox_andre_matias.t_lkp_scai_process proc 
where t_rel_scai_integration_process.cod_process = proc.cod_process
and t_rel_scai_integration_process.cod_status = 2
and t_rel_scai_integration_process.cod_country = 1
and proc.dsc_process_short = 't_lkp_base_user'
and t_rel_scai_integration_process.ind_active = 1
/*sandbox_andre_matias.t_rel_scai_integration_process.cod_process = source.cod_process
and sandbox_andre_matias.t_rel_scai_integration_process.cod_country = source.cod_country
and sandbox_andre_matias.t_rel_scai_integration_process.cod_integration = source.cod_integration*/;

drop table if exists sandbox_andre_matias.tmp_load_base_user;

$$$

-- #######################
-- ####    PASSO 3    ####
-- #######################
update sandbox_andre_matias.t_rel_scai_integration_process
set dat_processing = source.dat_processing, execution_nbr = source.execution_nbr, cod_status = 2 -- Running
from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc, sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_task'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 1
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
  ) source
where sandbox_andre_matias.t_rel_scai_integration_process.cod_process = source.cod_process
and sandbox_andre_matias.t_rel_scai_integration_process.cod_country = source.cod_country
and sandbox_andre_matias.t_rel_scai_integration_process.cod_integration = source.cod_integration;

-- #######################
-- ####    PASSO 4    ####
-- #######################
insert into sandbox_andre_matias.t_fac_scai_execution
  select
    max_cod_exec + 1 cod_execution,
    rel_integr_proc.cod_integration,
    rel_integr_proc.cod_process,
    rel_integr_proc.cod_status,
    1 cod_execution_type, -- Begin
    rel_integr_proc.dat_processing,
    rel_integr_proc.execution_nbr,
    sysdate
  from
    sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr,
    (select isnull(max(cod_execution),0) max_cod_exec from sandbox_andre_matias.t_fac_scai_execution),
    sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc,
    sandbox_andre_matias.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 1 -- Portugal
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_lkp_task';		

$$$
	
-- #############################################
-- # 			  BASE - Portugal              #
-- #		    LOADING t_lkp_task             #
-- #############################################

drop table if exists sandbox_andre_matias.tmp_load_task;

create table sandbox_andre_matias.tmp_load_task 
distkey(cod_source_system)
sortkey(cod_task, opr_task)
as
  select
    source_table.opr_task,
    lkp_base_user_owner.cod_base_user cod_base_user_owner,
    lkp_base_user_creator.cod_base_user cod_base_user_creator,
    lkp_resource_type.cod_resource_type,
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
    max_cod_task.max_cod,
    row_number() over (order by source_table.opr_task desc) new_cod,
    target.cod_task,
    case
      --when target.cod_task is null then 'I'
	  when target.cod_task is null or (source_table.hash_task != target.hash_task and target.valid_from = (select dat_processing from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_task')) then 'I'
	  when source_table.meta_event_type = 'deleted' then 'D'
      when source_table.hash_task != target.hash_task then 'U'
        else 'X'
    end dml_type
  from
    (
	select
		 source.*,
		lkp_source_system.cod_source_system,
		md5(isnull(opr_base_user_creator,-1) + isnull(opr_base_user_owner,-1) + isnull(opr_resource_type,'') + isnull(resource_id,-1) + decode(flg_completed, 1, 1, 0) + decode(flg_overdue, 1, 1, 0) + isnull(content,'')) hash_task
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
        updated_at
      FROM
        rdl_basecrm_v2.stg_d_base_tasks
	  WHERE
		updated_at >= (select isnull(rel_integr_proc.last_processing_datetime,'1900-01-01 00:00:00.000000') from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_task')
		--and 1 = 0
	) source,
    sandbox_andre_matias.t_lkp_source_system lkp_source_system
	where source.opr_source_system = lkp_source_system.opr_source_system
	and lkp_source_system.cod_country = 1 -- Portugal
	) source_table,
	sandbox_andre_matias.t_lkp_base_user lkp_base_user_creator,
	sandbox_andre_matias.t_lkp_base_user lkp_base_user_owner,
    sandbox_andre_matias.t_lkp_resource_type lkp_resource_type,
    (select isnull(max(cod_task),0) max_cod from sandbox_andre_matias.t_lkp_task) max_cod_task,
    sandbox_andre_matias.t_lkp_task target
  where
    isnull(source_table.opr_task,-1) = target.opr_task(+)
	and source_table.cod_source_system = target.cod_source_system (+)
    and target.valid_to(+) = 20991231
	and isnull(source_table.opr_base_user_owner,'-1') = lkp_base_user_owner.opr_base_user
	and source_table.cod_source_system = lkp_base_user_owner.cod_source_system -- new
	and lkp_base_user_owner.valid_to = 20991231
    and isnull(source_table.opr_base_user_creator,-1) = lkp_base_user_creator.opr_base_user
	and source_table.cod_source_system = lkp_base_user_creator.cod_source_system -- new
	and lkp_base_user_creator.valid_to = 20991231
    and isnull(source_table.opr_resource_type,'') = lkp_resource_type.opr_resource_type
	and lkp_resource_type.valid_to = 20991231;

	$$$
	
delete from sandbox_andre_matias.t_lkp_task
using sandbox_andre_matias.tmp_load_task
where 
	tmp_load_task.dml_type = 'I' 
	and t_lkp_task.opr_task = tmp_load_task.opr_task 
	and t_lkp_task.valid_from = (select dat_processing from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_task');

	$$$
	
update sandbox_andre_matias.t_lkp_task
set valid_to = (select rel_integr_proc.dat_processing from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_task')
from sandbox_andre_matias.tmp_load_task source
where source.cod_task = sandbox_andre_matias.t_lkp_task.cod_task
and sandbox_andre_matias.t_lkp_task.valid_to = 20991231
and source.dml_type in('U','D');

$$$

insert into sandbox_andre_matias.t_lkp_task
    select
      case
        when dml_type = 'I' then max_cod + new_cod
        when dml_type = 'U' then cod_task
      end cod_task,
      opr_task,
      (select rel_integr_proc.dat_processing from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_task') valid_from, 
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
	  hash_task
    from
      sandbox_andre_matias.tmp_load_task
    where
      dml_type in ('U','I');

	  $$$
	  
-- #######################
-- ####    PASSO 5    ####
-- #######################
insert into sandbox_andre_matias.t_fac_scai_execution
  select
    max_cod_exec + 1 cod_execution,
    rel_integr_proc.cod_integration,
    rel_integr_proc.cod_process,
    1 cod_status,
    2 cod_execution_type, -- End
    rel_integr_proc.dat_processing,
    rel_integr_proc.execution_nbr,
    sysdate
  from
    sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr,
    (select isnull(max(cod_execution),0) max_cod_exec from sandbox_andre_matias.t_fac_scai_execution),
    sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc,
    sandbox_andre_matias.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 1 -- Portugal
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
update sandbox_andre_matias.t_rel_scai_integration_process
set cod_status = 1, -- Ok
last_processing_datetime = isnull((select max(updated_at) from sandbox_andre_matias.tmp_load_task),last_processing_datetime)
/*from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc, sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_task'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 1
  ) source*/
from sandbox_andre_matias.t_lkp_scai_process proc 
where t_rel_scai_integration_process.cod_process = proc.cod_process
and t_rel_scai_integration_process.cod_status = 2
and t_rel_scai_integration_process.cod_country = 1
and proc.dsc_process_short = 't_lkp_task'
and t_rel_scai_integration_process.ind_active = 1
/*sandbox_andre_matias.t_rel_scai_integration_process.cod_process = source.cod_process
and sandbox_andre_matias.t_rel_scai_integration_process.cod_country = source.cod_country
and sandbox_andre_matias.t_rel_scai_integration_process.cod_integration = source.cod_integration*/;

drop table if exists sandbox_andre_matias.tmp_load_task;

$$$

-- #######################
-- ####    PASSO 3    ####
-- #######################
update sandbox_andre_matias.t_rel_scai_integration_process
set dat_processing = source.dat_processing, execution_nbr = source.execution_nbr, cod_status = 2 -- Running
from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc, sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_call_outcome'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 1
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
  ) source
where sandbox_andre_matias.t_rel_scai_integration_process.cod_process = source.cod_process
and sandbox_andre_matias.t_rel_scai_integration_process.cod_country = source.cod_country
and sandbox_andre_matias.t_rel_scai_integration_process.cod_integration = source.cod_integration;

-- #######################
-- ####    PASSO 4    ####
-- #######################
insert into sandbox_andre_matias.t_fac_scai_execution
  select
    max_cod_exec + 1 cod_execution,
    rel_integr_proc.cod_integration,
    rel_integr_proc.cod_process,
    rel_integr_proc.cod_status,
    1 cod_execution_type, -- Begin
    rel_integr_proc.dat_processing,
    rel_integr_proc.execution_nbr,
    sysdate
  from
    sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr,
    (select isnull(max(cod_execution),0) max_cod_exec from sandbox_andre_matias.t_fac_scai_execution),
    sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc,
    sandbox_andre_matias.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 1 -- Portugal
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_lkp_call_outcome';

	$$$
	
-- #############################################
-- # 	          BASE - Portugal              #
-- #        LOADING t_lkp_call_outcome         #
-- #############################################

drop table if exists sandbox_andre_matias.tmp_load_call_outcome;

create table sandbox_andre_matias.tmp_load_call_outcome 
distkey(cod_source_system)
sortkey(cod_call_outcome, opr_call_outcome)
as
  select source.*, isnull(lkp_user_creator.cod_base_user,-1) cod_base_user_creator from(
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
    max_cod_call_outcome.max_cod,
    row_number() over (order by source_table.opr_call_outcome desc) new_cod,
    target.cod_call_outcome,
    case
      --when target.cod_call_outcome is null then 'I'
	  when target.cod_call_outcome is null or (source_table.hash_call_outcome != target.hash_call_outcome and target.valid_from = (select dat_processing from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_call_outcome')) then 'I'
	  when source_table.meta_event_type = 'deleted' then 'D'
      when source_table.hash_call_outcome != target.hash_call_outcome then 'U'
        else 'X'
    end dml_type
  from
    (
	select 
		source.*,
		lkp_source_system.cod_source_system,
		md5(isnull(dsc_call_outcome,'') + isnull(opr_base_user,0)) hash_call_outcome
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
        updated_at
      FROM
        rdl_basecrm_v2.stg_d_base_call_outcomes
	  WHERE
		updated_at >= (select isnull(rel_integr_proc.last_processing_datetime,'1900-01-01 00:00:00.000000') from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_call_outcome')
		--and 1 = 0
	) source,
    sandbox_andre_matias.t_lkp_source_system lkp_source_system
	where source.opr_source_system = lkp_source_system.opr_source_system
	and lkp_source_system.cod_country = 1 -- Portugal
	) source_table,
    (select isnull(max(cod_call_outcome),0) max_cod from sandbox_andre_matias.t_lkp_call_outcome) max_cod_call_outcome,
    sandbox_andre_matias.t_lkp_call_outcome target
  where
    isnull(source_table.opr_call_outcome,-1) = target.opr_call_outcome(+)
	and source_table.cod_source_system = target.cod_source_system (+)
    and target.valid_to(+) = 20991231
    ) source, sandbox_andre_matias.t_lkp_base_user lkp_user_creator
    where isnull(source.opr_base_user,-1) = lkp_user_creator.opr_base_user (+)
	and source.cod_source_system = lkp_user_creator.cod_source_system (+) -- new
	and lkp_user_creator.valid_to (+) = 20991231;

	$$$
	
delete from sandbox_andre_matias.t_lkp_call_outcome
using sandbox_andre_matias.tmp_load_call_outcome
where 
	tmp_load_call_outcome.dml_type = 'I' 
	and t_lkp_call_outcome.opr_call_outcome = tmp_load_call_outcome.opr_call_outcome 
	and t_lkp_call_outcome.valid_from = (select dat_processing from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_call_outcome');

	$$$
	
update sandbox_andre_matias.t_lkp_call_outcome
set valid_to = (select rel_integr_proc.dat_processing from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_call_outcome') 
from sandbox_andre_matias.tmp_load_call_outcome source
where source.cod_call_outcome = sandbox_andre_matias.t_lkp_call_outcome.cod_call_outcome
and sandbox_andre_matias.t_lkp_call_outcome.valid_to = 20991231
and source.dml_type in('U','D');

$$$

insert into sandbox_andre_matias.t_lkp_call_outcome
    select
      case
        when dml_type = 'I' then max_cod + new_cod
        when dml_type = 'U' then cod_call_outcome
      end cod_call_outcome,
      opr_call_outcome,
      dsc_call_outcome,
      cod_source_system,
      (select rel_integr_proc.dat_processing from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_call_outcome') valid_from, 
      20991231 valid_to,
      cod_base_user_creator,
      created_at,
      updated_at,
      hash_call_outcome
    from
      sandbox_andre_matias.tmp_load_call_outcome
    where
      dml_type in ('U','I');

	  $$$
	  
-- #######################
-- ####    PASSO 5    ####
-- #######################
insert into sandbox_andre_matias.t_fac_scai_execution
  select
    max_cod_exec + 1 cod_execution,
    rel_integr_proc.cod_integration,
    rel_integr_proc.cod_process,
    1 cod_status,
    2 cod_execution_type, -- End
    rel_integr_proc.dat_processing,
    rel_integr_proc.execution_nbr,
    sysdate
  from
    sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr,
    (select isnull(max(cod_execution),0) max_cod_exec from sandbox_andre_matias.t_fac_scai_execution),
    sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc,
    sandbox_andre_matias.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 1 -- Portugal
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
update sandbox_andre_matias.t_rel_scai_integration_process
set cod_status = 1, -- Ok
last_processing_datetime = isnull((select max(updated_at) from sandbox_andre_matias.tmp_load_call_outcome),last_processing_datetime)
/*from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc, sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_call_outcome'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 1
  ) source*/
from sandbox_andre_matias.t_lkp_scai_process proc 
where t_rel_scai_integration_process.cod_process = proc.cod_process
and t_rel_scai_integration_process.cod_status = 2
and t_rel_scai_integration_process.cod_country = 1
and proc.dsc_process_short = 't_lkp_call_outcome'
and t_rel_scai_integration_process.ind_active = 1
/*sandbox_andre_matias.t_rel_scai_integration_process.cod_process = source.cod_process
and sandbox_andre_matias.t_rel_scai_integration_process.cod_country = source.cod_country
and sandbox_andre_matias.t_rel_scai_integration_process.cod_integration = source.cod_integration*/;

drop table if exists sandbox_andre_matias.tmp_load_call_outcome;

$$$

-- #######################
-- ####    PASSO 3    ####
-- #######################
update sandbox_andre_matias.t_rel_scai_integration_process
set dat_processing = source.dat_processing, execution_nbr = source.execution_nbr, cod_status = 2 -- Running
from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc, sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_contact'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 1
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
  ) source
where sandbox_andre_matias.t_rel_scai_integration_process.cod_process = source.cod_process
and sandbox_andre_matias.t_rel_scai_integration_process.cod_country = source.cod_country
and sandbox_andre_matias.t_rel_scai_integration_process.cod_integration = source.cod_integration;

-- #######################
-- ####    PASSO 4    ####
-- #######################
insert into sandbox_andre_matias.t_fac_scai_execution
  select
    max_cod_exec + 1 cod_execution,
    rel_integr_proc.cod_integration,
    rel_integr_proc.cod_process,
    rel_integr_proc.cod_status,
    1 cod_execution_type, -- Begin
    rel_integr_proc.dat_processing,
    rel_integr_proc.execution_nbr,
    sysdate
  from
    sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr,
    (select isnull(max(cod_execution),0) max_cod_exec from sandbox_andre_matias.t_fac_scai_execution),
    sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc,
    sandbox_andre_matias.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 1 -- Portugal
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_lkp_contact';	

	$$$
	
-- #############################################
-- # 		     BASE - PORTUGAL               #
-- #           LOADING t_lkp_contact           #
-- #############################################

drop table if exists sandbox_andre_matias.tmp_load_contact;

create table sandbox_andre_matias.tmp_load_contact 
distkey(cod_source_system)
sortkey(cod_contact, opr_contact)
as
  select
    source_table.opr_contact,
    source_table.dsc_contact,
    source_table.cod_source_system,
	source_table.meta_event_type,
	source_table.meta_event_time,
	lkp_base_user_creator.cod_base_user cod_base_user_creator,
	source_table.contact_id,
	source_table.created_at,
	source_table.updated_at,
	source_table.title,
	source_table.first_name,
	source_table.last_name,
	source_table.description,
	lkp_industry.cod_industry,
	source_table.website,
	source_table.email,
	source_table.phone,
	source_table.mobile,
	source_table.fax,
	source_table.twitter,
	source_table.facebook,
	source_table.linkedin,
	source_table.skype,
	lkp_base_user_owner.cod_base_user cod_base_user_owner,
	source_table.flg_organization,
	source_table.address,
	source_table.custom_fields,
	source_table.customer_status,
	source_table.prospect_status,
	source_table.tags,
    source_table.hash_contact,
    max_cod_contacts.max_cod,
    row_number() over (order by source_table.opr_contact desc) new_cod,
    target.cod_contact,
    case
      --when target.cod_contact is null then 'I'
	  when target.cod_contact is null or (source_table.hash_contact != target.hash_contact and target.valid_from = (select dat_processing from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_contact')) then 'I'
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
		isnull(dsc_contact                                                       ,'') +
		isnull(meta_event_type                                                   ,'') +
		--isnull(meta_event_time                                                   ,'2099-12-31 00:00:00.000000') +
		isnull(opr_base_user_creator                                             ,-1) +
		isnull(contact_id                                                        ,0) +
		--isnull(created_at                                                        ,'2099-12-31') +
		--isnull(updated_at                                                        ,'2099-12-31') +
		isnull(title                                                             ,'') +
		isnull(first_name                                                        ,'') +
		isnull(last_name                                                         ,'') +
		isnull(description                                                       ,'') +
		isnull(opr_industry                                                      ,'') +
		isnull(website                                                           ,'') +
		isnull(email                                                             ,'') +
		isnull(phone                                                             ,'') +
		isnull(mobile                                                            ,'') +
		isnull(fax                                                               ,'') +
		isnull(twitter                                                           ,'') +
		isnull(facebook                                                          ,'') +
		isnull(linkedin                                                          ,'') +
		isnull(skype                                                             ,'') +
		isnull(opr_base_user_owner                                               ,'-1') +
		decode(flg_organization,1,1,0)                                           +
		isnull(address                                                           ,'') +
		isnull(custom_fields                                                     ,'') +
		isnull(customer_status                                                   ,'') +
		isnull(prospect_status                                                   ,'') +
		isnull(tags                                                              ,'')
		) hash_contact
	from
	(
      SELECT
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
		tags
      FROM
        rdl_basecrm_v2.stg_d_base_contacts
	  WHERE
		updated_at >= (select isnull(rel_integr_proc.last_processing_datetime,'1900-01-01 00:00:00.000000') from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_contact')
		--and 1 = 0
	) source,
    sandbox_andre_matias.t_lkp_source_system lkp_source_system
	where source.opr_source_system = lkp_source_system.opr_source_system
	and lkp_source_system.cod_country = 1 -- Portugal
	) source_table,
    (select isnull(max(cod_contact),0) max_cod from sandbox_andre_matias.t_lkp_contact) max_cod_contacts,
	sandbox_andre_matias.t_lkp_base_user lkp_base_user_creator,
	sandbox_andre_matias.t_lkp_base_user lkp_base_user_owner,
	sandbox_andre_matias.t_lkp_industry lkp_industry,
    sandbox_andre_matias.t_lkp_contact target
  where
	source_table.opr_contact = target.opr_contact(+)
	and source_table.cod_source_system = target.cod_source_system (+)
    and target.valid_to(+) = 20991231
	and isnull(source_table.opr_base_user_owner,'-1') = lkp_base_user_owner.opr_base_user
	and source_table.cod_source_system = lkp_base_user_owner.cod_source_system -- new
	and lkp_base_user_owner.valid_to = 20991231
    and isnull(source_table.opr_base_user_creator,-1) = lkp_base_user_creator.opr_base_user
	and source_table.cod_source_system = lkp_base_user_creator.cod_source_system -- new
	and lkp_base_user_creator.valid_to = 20991231
    and isnull(source_table.opr_industry,'') = lkp_industry.opr_industry
	and source_table.cod_source_system = lkp_industry.cod_source_system -- new
	and lkp_industry.valid_to = 20991231;

	$$$
	
delete from sandbox_andre_matias.t_lkp_contact
using sandbox_andre_matias.tmp_load_contact
where 
	tmp_load_contact.dml_type = 'I' 
	and t_lkp_contact.opr_contact = tmp_load_contact.opr_contact 
	and t_lkp_contact.valid_from = (select dat_processing from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_contact');

	$$$
	
-- update valid_to in the updated/deleted records on source	
update sandbox_andre_matias.t_lkp_contact
set valid_to = (select rel_integr_proc.dat_processing from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_contact') 
from sandbox_andre_matias.tmp_load_contact source
where source.cod_contact = sandbox_andre_matias.t_lkp_contact.cod_contact
and sandbox_andre_matias.t_lkp_contact.valid_to = 20991231
and source.dml_type in('U','D');

	$$$
	
insert into sandbox_andre_matias.t_lkp_contact
    select
      case
        when dml_type = 'I' then max_cod + new_cod
        when dml_type = 'U' then cod_contact
      end cod_contact,
      opr_contact,
      dsc_contact,
      cod_source_system,
      (select rel_integr_proc.dat_processing from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_contact') valid_from, 
      20991231 valid_to,
	  cod_base_user_creator cod_base_user,
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
      hash_contact
    from
      sandbox_andre_matias.tmp_load_contact
    where
      dml_type in ('U','I');

	$$$
	
-- #######################
-- ####    PASSO 5    ####
-- #######################
insert into sandbox_andre_matias.t_fac_scai_execution
  select
    max_cod_exec + 1 cod_execution,
    rel_integr_proc.cod_integration,
    rel_integr_proc.cod_process,
    1 cod_status,
    2 cod_execution_type, -- End
    rel_integr_proc.dat_processing,
    rel_integr_proc.execution_nbr,
    sysdate
  from
    sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr,
    (select isnull(max(cod_execution),0) max_cod_exec from sandbox_andre_matias.t_fac_scai_execution),
    sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc,
    sandbox_andre_matias.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 1 -- Portugal
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
update sandbox_andre_matias.t_rel_scai_integration_process
set cod_status = 1, -- Ok
last_processing_datetime = isnull((select max(updated_at) from sandbox_andre_matias.tmp_load_contact),last_processing_datetime)
/*from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc, sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_contact'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 1
  ) source*/
from sandbox_andre_matias.t_lkp_scai_process proc 
where t_rel_scai_integration_process.cod_process = proc.cod_process
and t_rel_scai_integration_process.cod_status = 2
and t_rel_scai_integration_process.cod_country = 1
and proc.dsc_process_short = 't_lkp_contact'
and t_rel_scai_integration_process.ind_active = 1
/*sandbox_andre_matias.t_rel_scai_integration_process.cod_process = source.cod_process
and sandbox_andre_matias.t_rel_scai_integration_process.cod_country = source.cod_country
and sandbox_andre_matias.t_rel_scai_integration_process.cod_integration = source.cod_integration*/;

	$$$
	
-- #######################
-- ####    PASSO 3    ####
-- #######################
update sandbox_andre_matias.t_rel_scai_integration_process
set dat_processing = source.dat_processing, execution_nbr = source.execution_nbr, cod_status = 2 -- Running
from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc, sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_custom_field'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 1
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
  ) source
where sandbox_andre_matias.t_rel_scai_integration_process.cod_process = source.cod_process
and sandbox_andre_matias.t_rel_scai_integration_process.cod_country = source.cod_country
and sandbox_andre_matias.t_rel_scai_integration_process.cod_integration = source.cod_integration;


-- #######################
-- ####    PASSO 4    ####
-- #######################
insert into sandbox_andre_matias.t_fac_scai_execution
  select
    max_cod_exec + 1 cod_execution,
    rel_integr_proc.cod_integration,
    rel_integr_proc.cod_process,
    rel_integr_proc.cod_status,
    1 cod_execution_type, -- Begin
    rel_integr_proc.dat_processing,
    rel_integr_proc.execution_nbr,
    sysdate
  from
    sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr,
    (select isnull(max(cod_execution),0) max_cod_exec from sandbox_andre_matias.t_fac_scai_execution),
    sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc,
    sandbox_andre_matias.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 1 -- Portugal
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_lkp_custom_field';	

	$$$
	
-- #############################################
-- # 	     BASE - Portugal                   #
-- #       LOADING t_lkp_custom_field          #
-- #############################################

drop table if exists sandbox_andre_matias.tmp_contact_custom_field;

create table sandbox_andre_matias.tmp_contact_custom_field 
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
        sandbox_andre_matias.tmp_load_contact ts,
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
            gen_num between 1 and (select max(regexp_count(custom_fields, '\\","') + 1) from sandbox_andre_matias.tmp_load_contact)
        ) s
      where
        split_part(custom_fields, '","', s.gen_num) != ''
        and custom_fields != '{}'
    )
;

drop table if exists sandbox_andre_matias.tmp_load_custom_field;

	$$$
	
create table sandbox_andre_matias.tmp_load_custom_field as
   select
    source_table.opr_custom_field,
    source_table.opr_custom_field dsc_custom_field,
    source_table.cod_source_system,
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
        cod_source_system
      from
        sandbox_andre_matias.tmp_contact_custom_field
    ) source_table,
    (select isnull(max(cod_custom_field),0) max_cod from sandbox_andre_matias.t_lkp_custom_field) max_cod_custom_field,
    sandbox_andre_matias.t_lkp_custom_field target,
    sandbox_andre_matias.t_lkp_custom_field_context cf_context
  where
    isnull(source_table.opr_custom_field,'-1') = target.opr_custom_field(+)
    and target.valid_to(+) = 20991231
    and cf_context.opr_custom_field_context = 'Contacts';

	$$$
	
insert into sandbox_andre_matias.t_lkp_custom_field
    select
      max_cod + new_cod cod_custom_field,
      opr_custom_field,
      dsc_custom_field,
      cod_source_system,
      cod_custom_field_context,
      (select rel_integr_proc.dat_processing from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_custom_field') valid_from, 
      20991231 valid_to
    from
      sandbox_andre_matias.tmp_load_custom_field
    where
      dml_type = 'I';

	$$$
	
-- #######################
-- ####    PASSO 5    ####
-- #######################
insert into sandbox_andre_matias.t_fac_scai_execution
  select
    max_cod_exec + 1 cod_execution,
    rel_integr_proc.cod_integration,
    rel_integr_proc.cod_process,
    1 cod_status,
    2 cod_execution_type, -- End
    rel_integr_proc.dat_processing,
    rel_integr_proc.execution_nbr,
    sysdate
  from
    sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr,
    (select isnull(max(cod_execution),0) max_cod_exec from sandbox_andre_matias.t_fac_scai_execution),
    sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc,
    sandbox_andre_matias.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 1 -- Portugal
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
update sandbox_andre_matias.t_rel_scai_integration_process
set cod_status = 1, -- Ok
last_processing_datetime = isnull((select max(updated_at) from sandbox_andre_matias.tmp_load_contact),last_processing_datetime)
/*from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc, sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_stage'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 1
  ) source*/
from sandbox_andre_matias.t_lkp_scai_process proc 
where t_rel_scai_integration_process.cod_process = proc.cod_process
and t_rel_scai_integration_process.cod_status = 2
and t_rel_scai_integration_process.cod_country = 1
and proc.dsc_process_short = 't_lkp_custom_field'
and t_rel_scai_integration_process.ind_active = 1
/*sandbox_andre_matias.t_rel_scai_integration_process.cod_process = source.cod_process
and sandbox_andre_matias.t_rel_scai_integration_process.cod_country = source.cod_country
and sandbox_andre_matias.t_rel_scai_integration_process.cod_integration = source.cod_integration*/;

--drop table if exists sandbox_andre_matias.tmp_contact_custom_field; ######### ESTA TABELA É ELIMINADA NO PROCESSO SEGUINTE
drop table if exists sandbox_andre_matias.tmp_load_custom_fields;

	$$$
	
-- #######################
-- ####    PASSO 3    ####
-- #######################
update sandbox_andre_matias.t_rel_scai_integration_process
set dat_processing = source.dat_processing, execution_nbr = source.execution_nbr, cod_status = 2 -- Running
from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc, sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_rel_contact_custom_field'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 1
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
  ) source
where sandbox_andre_matias.t_rel_scai_integration_process.cod_process = source.cod_process
and sandbox_andre_matias.t_rel_scai_integration_process.cod_country = source.cod_country
and sandbox_andre_matias.t_rel_scai_integration_process.cod_integration = source.cod_integration;


-- #######################
-- ####    PASSO 4    ####
-- #######################
insert into sandbox_andre_matias.t_fac_scai_execution
  select
    max_cod_exec + 1 cod_execution,
    rel_integr_proc.cod_integration,
    rel_integr_proc.cod_process,
    rel_integr_proc.cod_status,
    1 cod_execution_type, -- Begin
    rel_integr_proc.dat_processing,
    rel_integr_proc.execution_nbr,
    sysdate
  from
    sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr,
    (select isnull(max(cod_execution),0) max_cod_exec from sandbox_andre_matias.t_fac_scai_execution),
    sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc,
    sandbox_andre_matias.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 1 -- Portugal
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_rel_contact_custom_field';	

	$$$
	
-- #############################################
-- # 	  BASE - Portugal                      #
-- #    LOADING t_rel_contact_custom_field     #
-- #############################################

drop table if exists sandbox_andre_matias.tmp_rel_contact_custom_field;

create table sandbox_andre_matias.tmp_rel_contact_custom_field as
  select
    source.cod_contact,
    source.cod_custom_field,
    source.cod_source_system,
    source.custom_field_value,
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
        tmp_cf.custom_field_value
      from
        sandbox_andre_matias.tmp_contact_custom_field tmp_cf,
        sandbox_andre_matias.t_lkp_contact contact,
        sandbox_andre_matias.t_lkp_custom_field cf
      where
        tmp_cf.opr_contact = contact.opr_contact
        and tmp_cf.custom_field_name = cf.opr_custom_field
    ) source,
    sandbox_andre_matias.t_rel_contact_custom_field target
  where
    source.cod_contact = target.cod_contact(+)
    and source.cod_custom_field = target.cod_custom_field(+)
    and source.cod_source_system = target.cod_source_system(+)
    and target.valid_to(+) = 20991231;

	$$$
	
update sandbox_andre_matias.t_rel_contact_custom_field
set valid_to = (select rel_integr_proc.dat_processing from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_rel_contact_custom_field') 
from sandbox_andre_matias.tmp_rel_contact_custom_field source
where source.cod_contact = sandbox_andre_matias.t_rel_contact_custom_field.cod_contact
and source.cod_custom_field = sandbox_andre_matias.t_rel_contact_custom_field.cod_custom_field
and source.cod_source_system = sandbox_andre_matias.t_rel_contact_custom_field.cod_source_system
and sandbox_andre_matias.t_rel_contact_custom_field.valid_to = 20991231
and source.dml_type = 'U';

	$$$
	
insert into sandbox_andre_matias.t_rel_contact_custom_field
  select
    cod_contact,
    cod_custom_field,
    cod_source_system,
    custom_field_value,
    (select rel_integr_proc.dat_processing from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_rel_contact_custom_field') valid_from, 
    20991231 valid_to
  from
    sandbox_andre_matias.tmp_rel_contact_custom_field
  where
    dml_type in ('I','U');

	$$$
	
-- #######################
-- ####    PASSO 5    ####
-- #######################
insert into sandbox_andre_matias.t_fac_scai_execution
  select
    max_cod_exec + 1 cod_execution,
    rel_integr_proc.cod_integration,
    rel_integr_proc.cod_process,
    1 cod_status,
    2 cod_execution_type, -- End
    rel_integr_proc.dat_processing,
    rel_integr_proc.execution_nbr,
    sysdate
  from
    sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr,
    (select isnull(max(cod_execution),0) max_cod_exec from sandbox_andre_matias.t_fac_scai_execution),
    sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc,
    sandbox_andre_matias.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 1 -- Portugal
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
update sandbox_andre_matias.t_rel_scai_integration_process
set cod_status = 1, -- Ok
last_processing_datetime = isnull((select max(updated_at) from sandbox_andre_matias.tmp_load_contact),last_processing_datetime)
/*from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc, sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_stage'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 1
  ) source*/
from sandbox_andre_matias.t_lkp_scai_process proc 
where t_rel_scai_integration_process.cod_process = proc.cod_process
and t_rel_scai_integration_process.cod_status = 2
and t_rel_scai_integration_process.cod_country = 1
and proc.dsc_process_short = 't_rel_contact_custom_field'
and t_rel_scai_integration_process.ind_active = 1
/*sandbox_andre_matias.t_rel_scai_integration_process.cod_process = source.cod_process
and sandbox_andre_matias.t_rel_scai_integration_process.cod_country = source.cod_country
and sandbox_andre_matias.t_rel_scai_integration_process.cod_integration = source.cod_integration*/;

drop table if exists sandbox_andre_matias.tmp_rel_contact_custom_field;
drop table if exists sandbox_andre_matias.tmp_contact_custom_field;
drop table if exists sandbox_andre_matias.tmp_load_contact;

	$$$
	
-- #######################
-- ####    PASSO 3    ####
-- #######################
update sandbox_andre_matias.t_rel_scai_integration_process
set dat_processing = source.dat_processing, execution_nbr = source.execution_nbr, cod_status = 2 -- Running
from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc, sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_lead'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 1
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
  ) source
where sandbox_andre_matias.t_rel_scai_integration_process.cod_process = source.cod_process
and sandbox_andre_matias.t_rel_scai_integration_process.cod_country = source.cod_country
and sandbox_andre_matias.t_rel_scai_integration_process.cod_integration = source.cod_integration;

-- #######################
-- ####    PASSO 4    ####
-- #######################
insert into sandbox_andre_matias.t_fac_scai_execution
  select
    max_cod_exec + 1 cod_execution,
    rel_integr_proc.cod_integration,
    rel_integr_proc.cod_process,
    rel_integr_proc.cod_status,
    1 cod_execution_type, -- Begin
    rel_integr_proc.dat_processing,
    rel_integr_proc.execution_nbr,
    sysdate
  from
    sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr,
    (select isnull(max(cod_execution),0) max_cod_exec from sandbox_andre_matias.t_fac_scai_execution),
    sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc,
    sandbox_andre_matias.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 1 -- Portugal
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_lkp_lead';	

	$$$
	
-- #############################################
-- # 	          BASE - Portugal              #
-- #           LOADING t_lkp_lead             #
-- #############################################

drop table if exists sandbox_andre_matias.tmp_load_lead;

create table sandbox_andre_matias.tmp_load_lead 
distkey(cod_source_system)
sortkey(cod_lead, opr_lead)
as
   select
    source_table.opr_lead,
    source_table.dsc_lead,
    source_table.cod_source_system,
    lkp_base_user_owner.cod_base_user cod_base_user_owner,
    lkp_base_user_creator.cod_base_user cod_base_user_creator,
    --lkp_base_source.cod_base_source,
    lkp_industry.cod_industry,
    lkp_lead_status.cod_lead_status,
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
    case
      --when target.cod_lead is null then 'I'
	  when target.cod_lead is null or (source_table.hash_lead != target.hash_lead and target.valid_from = (select dat_processing from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_lead')) then 'I'
	  when source_table.meta_event_type = 'deleted' then 'D'
      when source_table.hash_lead != target.hash_lead then 'U'
        else 'X'
    end dml_type
  from
    (
	select
		source.*,
		lkp_source_system.cod_source_system,
        md5(isnull(dsc_lead,'') + isnull(first_name,'') + isnull(last_name,'') + isnull(opr_base_user_owner,0) /*+ isnull(source_id,0)*/ + isnull(twitter,'') + isnull(phone,'')
          + isnull(mobile,'') + isnull(facebook,'') + isnull(email,'') + isnull(title,'') + isnull(skype,'') + isnull(linkedin,'') + isnull(opr_industry,'')
          + isnull(fax,'') + isnull(website,'') + isnull(address,'') + isnull(opr_lead_status,'') + isnull(opr_base_user_creator,0) + isnull(organization_name,'')
          + isnull(custom_fields,'') + isnull(tags,'')) hash_lead
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
        updated_at
      FROM
        rdl_basecrm_v2.stg_d_base_leads
	  WHERE
		updated_at >= (select isnull(rel_integr_proc.last_processing_datetime,'1900-01-01 00:00:00.000000') from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_lead')
		--and 1 = 0
	) source,
    sandbox_andre_matias.t_lkp_source_system lkp_source_system
	where source.opr_source_system = lkp_source_system.opr_source_system
	and lkp_source_system.cod_country = 1 -- Portugal
	) source_table,
    sandbox_andre_matias.t_lkp_base_user lkp_base_user_owner,
    sandbox_andre_matias.t_lkp_base_user lkp_base_user_creator,
    --sandbox_andre_matias.t_lkp_base_source lkp_base_source,
    sandbox_andre_matias.t_lkp_industry lkp_industry,
    sandbox_andre_matias.t_lkp_lead_status lkp_lead_status,
    (select isnull(max(cod_lead),0) max_cod from sandbox_andre_matias.t_lkp_lead) max_cod_lead,
    sandbox_andre_matias.t_lkp_lead target
  where
    isnull(source_table.opr_lead,-1) = target.opr_lead(+)
	and source_table.cod_source_system = target.cod_source_system (+)
    and target.valid_to(+) = 20991231
    and isnull(source_table.opr_base_user_owner,-1) = lkp_base_user_owner.opr_base_user
	and source_table.cod_source_system = lkp_base_user_owner.cod_source_system -- new
	and lkp_base_user_owner.valid_to = 20991231
    and isnull(source_table.opr_base_user_creator,-1) = lkp_base_user_creator.opr_base_user
	and source_table.cod_source_system = lkp_base_user_creator.cod_source_system -- new
	and lkp_base_user_creator.valid_to = 20991231
    and isnull(source_table.opr_industry,'') = lkp_industry.opr_industry -- LOST DATA
	and source_table.cod_source_system = lkp_industry.cod_source_system -- new
	and lkp_industry.valid_to = 20991231
    and isnull(source_table.opr_lead_status,'') = lkp_lead_status.opr_lead_status
	and source_table.cod_source_system = lkp_lead_status.cod_source_system -- new
	and lkp_lead_status.valid_to = 20991231;

	$$$
	
delete from sandbox_andre_matias.t_lkp_lead
using sandbox_andre_matias.tmp_load_lead
where 
	tmp_load_lead.dml_type = 'I' 
	and t_lkp_lead.opr_lead = tmp_load_lead.opr_lead 
	and t_lkp_lead.valid_from = (select dat_processing from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_lead');

	$$$
	
-- update valid_to in the updated/deleted records on source	
update sandbox_andre_matias.t_lkp_lead
set valid_to = (select rel_integr_proc.dat_processing from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_lead') 
from sandbox_andre_matias.tmp_load_lead source
where source.cod_lead = sandbox_andre_matias.t_lkp_lead.cod_lead
and sandbox_andre_matias.t_lkp_lead.valid_to = 20991231
and source.dml_type in('U','D');

	$$$
	
insert into sandbox_andre_matias.t_lkp_lead
    select
      case
        when dml_type = 'I' then max_cod + new_cod
        when dml_type = 'U' then cod_lead
      end cod_lead,
      opr_lead,
      dsc_lead,
      cod_source_system,
      (select rel_integr_proc.dat_processing from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_lead') valid_from, 
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
      hash_lead
    from
      sandbox_andre_matias.tmp_load_lead
    where
      dml_type in ('U','I');

	$$$
	
-- #######################
-- ####    PASSO 5    ####
-- #######################
insert into sandbox_andre_matias.t_fac_scai_execution
  select
    max_cod_exec + 1 cod_execution,
    rel_integr_proc.cod_integration,
    rel_integr_proc.cod_process,
    1 cod_status,
    2 cod_execution_type, -- End
    rel_integr_proc.dat_processing,
    rel_integr_proc.execution_nbr,
    sysdate
  from
    sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr,
    (select isnull(max(cod_execution),0) max_cod_exec from sandbox_andre_matias.t_fac_scai_execution),
    sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc,
    sandbox_andre_matias.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 1 -- Portugal
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
update sandbox_andre_matias.t_rel_scai_integration_process
set cod_status = 1, -- Ok
last_processing_datetime = isnull((select max(updated_at) from sandbox_andre_matias.tmp_load_lead),last_processing_datetime)
/*from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc, sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_lead'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 1
  ) source*/
from sandbox_andre_matias.t_lkp_scai_process proc 
where t_rel_scai_integration_process.cod_process = proc.cod_process
and t_rel_scai_integration_process.cod_status = 2
and t_rel_scai_integration_process.cod_country = 1
and proc.dsc_process_short = 't_lkp_lead'
and t_rel_scai_integration_process.ind_active = 1
/*sandbox_andre_matias.t_rel_scai_integration_process.cod_process = source.cod_process
and sandbox_andre_matias.t_rel_scai_integration_process.cod_country = source.cod_country
and sandbox_andre_matias.t_rel_scai_integration_process.cod_integration = source.cod_integration*/;

drop table if exists sandbox_andre_matias.tmp_load_lead;

	$$$
	
-- #######################
-- ####    PASSO 3    ####
-- #######################
update sandbox_andre_matias.t_rel_scai_integration_process
set dat_processing = source.dat_processing, execution_nbr = source.execution_nbr, cod_status = 2 -- Running
from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc, sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_loss_reason'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 1
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
  ) source
where sandbox_andre_matias.t_rel_scai_integration_process.cod_process = source.cod_process
and sandbox_andre_matias.t_rel_scai_integration_process.cod_country = source.cod_country
and sandbox_andre_matias.t_rel_scai_integration_process.cod_integration = source.cod_integration;

-- #######################
-- ####    PASSO 4    ####
-- #######################
insert into sandbox_andre_matias.t_fac_scai_execution
  select
    max_cod_exec + 1 cod_execution,
    rel_integr_proc.cod_integration,
    rel_integr_proc.cod_process,
    rel_integr_proc.cod_status,
    1 cod_execution_type, -- Begin
    rel_integr_proc.dat_processing,
    rel_integr_proc.execution_nbr,
    sysdate
  from
    sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr,
    (select isnull(max(cod_execution),0) max_cod_exec from sandbox_andre_matias.t_fac_scai_execution),
    sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc,
    sandbox_andre_matias.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 1 -- Portugal
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_lkp_loss_reason';	

	$$$
	
-- #############################################
-- # 	          BASE - Portugal              #
-- #        LOADING t_lkp_loss_reason          #
-- #############################################

drop table if exists sandbox_andre_matias.tmp_load_loss_reason;

create table sandbox_andre_matias.tmp_load_loss_reason 
distkey(cod_source_system)
sortkey(cod_loss_reason, opr_loss_reason)
as
  select
    source_table.opr_loss_reason,
    source_table.dsc_loss_reason,
    source_table.opr_source_system,
	source_table.meta_event_type,
	source_table.meta_event_time,
    lkp_user_creator.cod_base_user cod_base_user_creator, -- CORRIGIR
    source_table.hash_loss_reason,
    source_table.cod_source_system,
    max_cod_loss_reason.max_cod,
    row_number() over (order by source_table.opr_loss_reason desc) new_cod,
    target.cod_loss_reason,
    source_table.created_at,
    source_table.updated_at,
    case
      --when target.cod_loss_reason is null then 'I'
	  when target.cod_loss_reason is null or (source_table.hash_loss_reason != target.hash_loss_reason and target.valid_from = (select dat_processing from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_loss_reason')) then 'I'
	  when source_table.meta_event_type = 'deleted' then 'D'
      when source_table.hash_loss_reason != target.hash_loss_reason then 'U'
        else 'X'
    end dml_type
  from
    (
	select
		source.*,
		lkp_source_system.cod_source_system,
        md5(isnull(dsc_loss_reason,'') + isnull(opr_base_user_creator,0)) hash_loss_reason
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
        updated_at
	  FROM
        rdl_basecrm_v2.stg_d_base_loss_reasons
	  WHERE
		updated_at >= (select isnull(rel_integr_proc.last_processing_datetime,'1900-01-01 00:00:00.000000') from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_loss_reason')
		--and 1 = 0
	) source,
    sandbox_andre_matias.t_lkp_source_system lkp_source_system
	where source.opr_source_system = lkp_source_system.opr_source_system
	and lkp_source_system.cod_country = 1 -- Portugal
	) source_table,
    sandbox_andre_matias.t_lkp_base_user lkp_user_creator,
    (select isnull(max(cod_loss_reason),0) max_cod from sandbox_andre_matias.t_lkp_loss_reason) max_cod_loss_reason,
    sandbox_andre_matias.t_lkp_loss_reason target
  where
    isnull(source_table.opr_loss_reason,-1) = target.opr_loss_reason(+)
	and source_table.cod_source_system = target.cod_source_system (+)
    and target.valid_to(+) = 20991231
    and isnull(source_table.opr_base_user_creator,-1) = lkp_user_creator.opr_base_user
	and source_table.cod_source_system = lkp_user_creator.cod_source_system -- new
	and lkp_user_creator.valid_to = 20991231;

	$$$
	
delete from sandbox_andre_matias.t_lkp_loss_reason
using sandbox_andre_matias.tmp_load_loss_reason
where 
	tmp_load_loss_reason.dml_type = 'I' 
	and t_lkp_loss_reason.opr_loss_reason = tmp_load_loss_reason.opr_loss_reason 
	and t_lkp_loss_reason.valid_from = (select dat_processing from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_loss_reason');

	$$$
	
update sandbox_andre_matias.t_lkp_loss_reason
set valid_to = (select rel_integr_proc.dat_processing from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_loss_reason') 
from sandbox_andre_matias.tmp_load_loss_reason source
where source.cod_loss_reason = sandbox_andre_matias.t_lkp_loss_reason.cod_loss_reason
and sandbox_andre_matias.t_lkp_loss_reason.valid_to = 20991231
and source.dml_type in('U','D');

insert into sandbox_andre_matias.t_lkp_loss_reason
	select
	  case
		when dml_type = 'I' then max_cod + new_cod
		when dml_type = 'U' then cod_loss_reason
	  end cod_loss_reason,
	  opr_loss_reason,
	  dsc_loss_reason,
	  cod_source_system,
	  (select rel_integr_proc.dat_processing from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_loss_reason') valid_from, 
	  20991231 valid_to,
	  cod_base_user_creator,
	  created_at,
	  updated_at,
	  hash_loss_reason
	from
	  sandbox_andre_matias.tmp_load_loss_reason
	where
	  dml_type in ('U','I');

	$$$
	
-- #######################
-- ####    PASSO 5    ####
-- #######################
insert into sandbox_andre_matias.t_fac_scai_execution
  select
    max_cod_exec + 1 cod_execution,
    rel_integr_proc.cod_integration,
    rel_integr_proc.cod_process,
    1 cod_status,
    2 cod_execution_type, -- End
    rel_integr_proc.dat_processing,
    rel_integr_proc.execution_nbr,
    sysdate
  from
    sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr,
    (select isnull(max(cod_execution),0) max_cod_exec from sandbox_andre_matias.t_fac_scai_execution),
    sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc,
    sandbox_andre_matias.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 1 -- Portugal
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
update sandbox_andre_matias.t_rel_scai_integration_process
set cod_status = 1, -- Ok
last_processing_datetime = isnull((select max(updated_at) from sandbox_andre_matias.tmp_load_loss_reason),last_processing_datetime)
/*from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc, sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_loss_reason'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 1
  ) source*/
from sandbox_andre_matias.t_lkp_scai_process proc 
where t_rel_scai_integration_process.cod_process = proc.cod_process
and t_rel_scai_integration_process.cod_status = 2
and t_rel_scai_integration_process.cod_country = 1
and proc.dsc_process_short = 't_lkp_loss_reason'
and t_rel_scai_integration_process.ind_active = 1
/*sandbox_andre_matias.t_rel_scai_integration_process.cod_process = source.cod_process
and sandbox_andre_matias.t_rel_scai_integration_process.cod_country = source.cod_country
and sandbox_andre_matias.t_rel_scai_integration_process.cod_integration = source.cod_integration*/;

drop table if exists sandbox_andre_matias.tmp_load_loss_reason;

	$$$
	
-- #######################
-- ####    PASSO 3    ####
-- #######################
update sandbox_andre_matias.t_rel_scai_integration_process
set dat_processing = source.dat_processing, execution_nbr = source.execution_nbr, cod_status = 2 -- Running
from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc, sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_pipeline'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 1
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
  ) source
where sandbox_andre_matias.t_rel_scai_integration_process.cod_process = source.cod_process
and sandbox_andre_matias.t_rel_scai_integration_process.cod_country = source.cod_country
and sandbox_andre_matias.t_rel_scai_integration_process.cod_integration = source.cod_integration;

-- #######################
-- ####    PASSO 4    ####
-- #######################
insert into sandbox_andre_matias.t_fac_scai_execution
  select
    max_cod_exec + 1 cod_execution,
    rel_integr_proc.cod_integration,
    rel_integr_proc.cod_process,
    rel_integr_proc.cod_status,
    1 cod_execution_type, -- Begin
    rel_integr_proc.dat_processing,
    rel_integr_proc.execution_nbr,
    sysdate
  from
    sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr,
    (select isnull(max(cod_execution),0) max_cod_exec from sandbox_andre_matias.t_fac_scai_execution),
    sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc,
    sandbox_andre_matias.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 1 -- Portugal
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_lkp_pipeline';

	$$$
	
-- #############################################
-- # 	          BASE - Portugal              #
-- #		   LOADING t_lkp_pipeline          #
-- #############################################

drop table if exists sandbox_andre_matias.tmp_load_pipeline;

create table sandbox_andre_matias.tmp_load_pipeline 
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
    max_cod_pipeline.max_cod,
    row_number() over (order by source_table.opr_pipeline desc) new_cod, 
    target.cod_pipeline,
    case
      --when target.cod_pipeline is null then 'I'
	  when target.cod_pipeline is null or (source_table.hash_pipeline != target.hash_pipeline and target.valid_from = (select dat_processing from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_pipeline')) then 'I'
	  when source_table.meta_event_type = 'deleted' then 'D'
      when source_table.hash_pipeline != target.hash_pipeline then 'U'
        else 'X'
    end dml_type
  from
    (
	select
		source.*,
		lkp_source_system.cod_source_system,
        md5(isnull(dsc_pipeline,'') + decode(flg_disabled, 1, 1, 0)) hash_pipeline
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
        updated_at
	  FROM
        rdl_basecrm_v2.stg_d_base_pipelines
	  WHERE
		updated_at >= (select isnull(rel_integr_proc.last_processing_datetime,'1900-01-01 00:00:00.000000') from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_pipeline')
		--and 1 = 0
	) source,
    sandbox_andre_matias.t_lkp_source_system lkp_source_system
	where source.opr_source_system = lkp_source_system.opr_source_system
	and lkp_source_system.cod_country = 1 -- Portugal
	) source_table,
    (select isnull(max(cod_pipeline),0) max_cod from sandbox_andre_matias.t_lkp_pipeline) max_cod_pipeline,
    sandbox_andre_matias.t_lkp_pipeline target
  where
    isnull(source_table.opr_pipeline,-1) = target.opr_pipeline(+)
	and source_table.cod_source_system = target.cod_source_system (+)
    and target.valid_to(+) = 20991231;

	$$$
	
delete from sandbox_andre_matias.t_lkp_pipeline
using sandbox_andre_matias.tmp_load_pipeline
where 
	tmp_load_pipeline.dml_type = 'I' 
	and t_lkp_pipeline.opr_pipeline = tmp_load_pipeline.opr_pipeline
	and t_lkp_pipeline.valid_from = (select dat_processing from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_pipeline');

	$$$
	
update sandbox_andre_matias.t_lkp_pipeline
set valid_to = (select rel_integr_proc.dat_processing from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_pipeline') 
from sandbox_andre_matias.tmp_load_pipeline source
where source.cod_pipeline = sandbox_andre_matias.t_lkp_pipeline.cod_pipeline
and sandbox_andre_matias.t_lkp_pipeline.valid_to = 20991231
and source.dml_type in('U','D');

insert into sandbox_andre_matias.t_lkp_pipeline
    select
      case
        when dml_type = 'I' then max_cod + new_cod
        when dml_type = 'U' then cod_pipeline
      end cod_pipeline,
      opr_pipeline,
      dsc_pipeline,
      cod_source_system,
      (select rel_integr_proc.dat_processing from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_pipeline') valid_from, 
      20991231 valid_to,
      decode(flg_disabled,1,1,0) flg_confirmed,
      created_at,
      updated_at,
      hash_pipeline
    from
      sandbox_andre_matias.tmp_load_pipeline
    where
      dml_type in ('U','I');

	$$$
	
-- #######################
-- ####    PASSO 5    ####
-- #######################
insert into sandbox_andre_matias.t_fac_scai_execution
  select
    max_cod_exec + 1 cod_execution,
    rel_integr_proc.cod_integration,
    rel_integr_proc.cod_process,
    1 cod_status,
    2 cod_execution_type, -- End
    rel_integr_proc.dat_processing,
    rel_integr_proc.execution_nbr,
    sysdate
  from
    sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr,
    (select isnull(max(cod_execution),0) max_cod_exec from sandbox_andre_matias.t_fac_scai_execution),
    sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc,
    sandbox_andre_matias.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 1 -- Portugal
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
update sandbox_andre_matias.t_rel_scai_integration_process
set cod_status = 1, -- Ok
last_processing_datetime = isnull((select max(updated_at) from sandbox_andre_matias.tmp_load_pipeline),last_processing_datetime)
/*from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc, sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_pipeline'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 1
  ) source*/
from sandbox_andre_matias.t_lkp_scai_process proc 
where t_rel_scai_integration_process.cod_process = proc.cod_process
and t_rel_scai_integration_process.cod_status = 2
and t_rel_scai_integration_process.cod_country = 1
and proc.dsc_process_short = 't_lkp_pipeline'
and t_rel_scai_integration_process.ind_active = 1
/*sandbox_andre_matias.t_rel_scai_integration_process.cod_process = source.cod_process
and sandbox_andre_matias.t_rel_scai_integration_process.cod_country = source.cod_country
and sandbox_andre_matias.t_rel_scai_integration_process.cod_integration = source.cod_integration*/;

drop table if exists sandbox_andre_matias.tmp_load_pipeline;

	$$$
	
-- #######################
-- ####    PASSO 3    ####
-- #######################
update sandbox_andre_matias.t_rel_scai_integration_process
set dat_processing = source.dat_processing, execution_nbr = source.execution_nbr, cod_status = 2 -- Running
from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc, sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_stage'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 1
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
  ) source
where sandbox_andre_matias.t_rel_scai_integration_process.cod_process = source.cod_process
and sandbox_andre_matias.t_rel_scai_integration_process.cod_country = source.cod_country
and sandbox_andre_matias.t_rel_scai_integration_process.cod_integration = source.cod_integration;


-- #######################
-- ####    PASSO 4    ####
-- #######################
insert into sandbox_andre_matias.t_fac_scai_execution
  select
    max_cod_exec + 1 cod_execution,
    rel_integr_proc.cod_integration,
    rel_integr_proc.cod_process,
    rel_integr_proc.cod_status,
    1 cod_execution_type, -- Begin
    rel_integr_proc.dat_processing,
    rel_integr_proc.execution_nbr,
    sysdate
  from
    sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr,
    (select isnull(max(cod_execution),0) max_cod_exec from sandbox_andre_matias.t_fac_scai_execution),
    sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc,
    sandbox_andre_matias.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 1 -- Portugal
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_lkp_stage';	

	$$$
	
-- #############################################
-- # 	          BASE - Portugal              #
-- #		   LOADING t_lkp_stage            #
-- #############################################

drop table if exists sandbox_andre_matias.tmp_load_stage;

create table sandbox_andre_matias.tmp_load_stage 
distkey(cod_source_system)
sortkey(cod_stage, opr_stage)
as
  select
    source_table.opr_stage,
    source_table.dsc_stage,
    lkp_pipeline.cod_pipeline,
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
    case
      --when target.cod_stage is null then 'I'
	  when target.cod_stage is null or (source_table.hash_stages != target.hash_stages and target.valid_from = (select dat_processing from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_stage')) then 'I'
	  when source_table.meta_event_type = 'deleted' then 'D'
      when source_table.hash_stages != target.hash_stages then 'U'
        else 'X'
    end dml_type
  from
    (
	select
		source.*,
		lkp_source_system.cod_source_system,
        md5(isnull(dsc_stage,'') + isnull(position,0) + isnull(likelihood,0) + decode(flg_active, 1, 1, 0)) hash_stages
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
        pipeline_id opr_pipeline
      FROM
        rdl_basecrm_v2.stg_d_base_stages
	  WHERE
		updated_at >= (select isnull(rel_integr_proc.last_processing_datetime,'1900-01-01 00:00:00.000000') from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_stage')
		--and 1 = 0
	) source,
    sandbox_andre_matias.t_lkp_source_system lkp_source_system
	where source.opr_source_system = lkp_source_system.opr_source_system
	and lkp_source_system.cod_country = 1 -- Portugal
	) source_table,
    sandbox_andre_matias.t_lkp_pipeline lkp_pipeline,
    (select isnull(max(cod_pipeline),0) max_cod from sandbox_andre_matias.t_lkp_stage) max_cod_stages,
    sandbox_andre_matias.t_lkp_stage target
  where
    isnull(source_table.opr_stage,-1) = target.opr_stage(+)
	and source_table.cod_source_system = target.cod_source_system (+)
    and target.valid_to(+) = 20991231
    and isnull(source_table.opr_pipeline,-1) = lkp_pipeline.opr_pipeline
	and source_table.cod_source_system = lkp_pipeline.cod_source_system -- new
	and lkp_pipeline.valid_to = 20991231;

	$$$
	
delete from sandbox_andre_matias.t_lkp_stage
using sandbox_andre_matias.tmp_load_stage
where 
	tmp_load_stage.dml_type = 'I' 
	and t_lkp_stage.opr_stage = tmp_load_stage.opr_stage
	and t_lkp_stage.valid_from = (select dat_processing from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_stage');

	$$$
	
update sandbox_andre_matias.t_lkp_stage
set valid_to = (select rel_integr_proc.dat_processing from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_stage') 
from sandbox_andre_matias.tmp_load_stage source
where source.cod_stage = sandbox_andre_matias.t_lkp_stage.cod_stage
and sandbox_andre_matias.t_lkp_stage.valid_to = 20991231
and source.dml_type in('U','D');

	$$$
	
insert into sandbox_andre_matias.t_lkp_stage
    select
      case
        when dml_type = 'I' then max_cod + new_cod
        when dml_type = 'U' then cod_stage
      end cod_stage,
      opr_stage,
      dsc_stage,
      cod_source_system,
      (select rel_integr_proc.dat_processing from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_stage') valid_from, 
      20991231 valid_to,
      position,
      likelihood,
      decode(flg_active,1,1,0) flg_active,
      cod_pipeline,
      hash_stages
    from
      sandbox_andre_matias.tmp_load_stage
    where
      dml_type in ('U','I');

	$$$
	
-- #######################
-- ####    PASSO 5    ####
-- #######################
insert into sandbox_andre_matias.t_fac_scai_execution
  select
    max_cod_exec + 1 cod_execution,
    rel_integr_proc.cod_integration,
    rel_integr_proc.cod_process,
    1 cod_status,
    2 cod_execution_type, -- End
    rel_integr_proc.dat_processing,
    rel_integr_proc.execution_nbr,
    sysdate
  from
    sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr,
    (select isnull(max(cod_execution),0) max_cod_exec from sandbox_andre_matias.t_fac_scai_execution),
    sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc,
    sandbox_andre_matias.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 1 -- Portugal
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
update sandbox_andre_matias.t_rel_scai_integration_process
set cod_status = 1, -- Ok
last_processing_datetime = isnull((select max(updated_at) from sandbox_andre_matias.tmp_load_stage),last_processing_datetime)
/*from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc, sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_stage'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 1
  ) source*/
from sandbox_andre_matias.t_lkp_scai_process proc 
where t_rel_scai_integration_process.cod_process = proc.cod_process
and t_rel_scai_integration_process.cod_status = 2
and t_rel_scai_integration_process.cod_country = 1
and proc.dsc_process_short = 't_lkp_stage'
and t_rel_scai_integration_process.ind_active = 1
/*sandbox_andre_matias.t_rel_scai_integration_process.cod_process = source.cod_process
and sandbox_andre_matias.t_rel_scai_integration_process.cod_country = source.cod_country
and sandbox_andre_matias.t_rel_scai_integration_process.cod_integration = source.cod_integration*/;

drop table if exists sandbox_andre_matias.tmp_load_stage;

	$$$
	
-- #######################
-- ####    PASSO 3    ####
-- #######################
update sandbox_andre_matias.t_rel_scai_integration_process
set dat_processing = source.dat_processing, execution_nbr = source.execution_nbr, cod_status = 2 -- Running
from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc, sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_deal'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 1
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
  ) source
where sandbox_andre_matias.t_rel_scai_integration_process.cod_process = source.cod_process
and sandbox_andre_matias.t_rel_scai_integration_process.cod_country = source.cod_country
and sandbox_andre_matias.t_rel_scai_integration_process.cod_integration = source.cod_integration;


-- #######################
-- ####    PASSO 4    ####
-- #######################
insert into sandbox_andre_matias.t_fac_scai_execution
  select
    max_cod_exec + 1 cod_execution,
    rel_integr_proc.cod_integration,
    rel_integr_proc.cod_process,
    rel_integr_proc.cod_status,
    1 cod_execution_type, -- Begin
    rel_integr_proc.dat_processing,
    rel_integr_proc.execution_nbr,
    sysdate
  from
    sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr,
    (select isnull(max(cod_execution),0) max_cod_exec from sandbox_andre_matias.t_fac_scai_execution),
    sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc,
    sandbox_andre_matias.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 1 -- Portugal
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_lkp_deal';	

	$$$
	
-- #############################################
-- # 	          BASE - Portugal              #
-- #           LOADING t_lkp_deal             #
-- #############################################

drop table if exists sandbox_andre_matias.tmp_load_deals;

create table sandbox_andre_matias.tmp_load_deals 
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
	lkp_contact.cod_contact,
	lkp_base_source.cod_base_source,
	source_table.estimated_close_date,
	source_table.dropbox_email,
	lkp_base_user_creator.cod_base_user cod_base_user_creator,
	lkp_loss_reason.cod_loss_reason,
	lkp_currency.cod_currency,
	source_table.updated_at,
	source_table.organization_id,
	source_table.last_stage_change_at,
	lkp_base_user_owner.cod_base_user cod_base_user_owner,
	source_table.value,
	source_table.created_at,
	source_table.flg_hot,
	source_table.opr_base_user_last_change, -- ?
	lkp_stages.cod_stage,
	source_table.custom_fields,
	source_table.tags,
    source_table.hash_deal,
    max_cod_deals.max_cod,
    row_number() over (order by source_table.opr_deal desc) new_cod,
    target.cod_deal,
    case
      --when target.cod_deal is null then 'I'
	  when target.cod_deal is null or (source_table.hash_deal != target.hash_deal and target.valid_from = (select dat_processing from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_deal')) then 'I'
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
		isnull(dsc_deal                                                              ,'') +
		isnull(meta_event_type                                                   ,'') +
		--isnull(meta_event_time                                                   ,'2099-12-31 00:00:00.000000') +
		--isnull(last_activity_at                                                  ,'2099-12-31 00:00:00.000000') +
		isnull(opr_contact                                                        ,0) +
		isnull(opr_base_source                                                         ,0) +
		isnull(estimated_close_date                                              ,'2099-12-31') +
		isnull(dropbox_email                                                     ,'') +
		isnull(opr_base_user_creator                                                        ,0) +
		isnull(opr_loss_reason                                                    ,0) +
		isnull(opr_currency                                                          ,'') +
		--isnull(updated_at                                                      ,'') +
		isnull(organization_id                                                   ,0) +
		--isnull(last_stage_change_at                                              ,'2099-12-31 00:00:00.000000') +
		isnull(opr_base_user_owner                                                          ,0) +
		isnull(value                                                             ,0) +
		--isnull(created_at                                                      ,'') +
		decode(flg_hot,1,1,0)                                                        +
		isnull(opr_base_user_last_change                                           ,0) +
		isnull(opr_stage                                                          ,0) +
		isnull(custom_fields                                                     ,'') +
		isnull(tags                                                              ,'')
		) hash_deal
	from
	(
      SELECT
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
		tags
      FROM
        rdl_basecrm_v2.stg_d_base_deals
	  WHERE
		updated_at >= (select isnull(rel_integr_proc.last_processing_datetime,'1900-01-01 00:00:00.000000') from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_deal')
		--and 1 = 0
	) source,
    sandbox_andre_matias.t_lkp_source_system lkp_source_system
	where source.opr_source_system = lkp_source_system.opr_source_system
	and lkp_source_system.cod_country = 1 -- Portugal
	) source_table,
    (select isnull(max(cod_deal),0) max_cod from sandbox_andre_matias.t_lkp_deal) max_cod_deals,
	sandbox_andre_matias.t_lkp_base_user lkp_base_user_creator,
	sandbox_andre_matias.t_lkp_base_user lkp_base_user_owner,
	sandbox_andre_matias.t_lkp_contact lkp_contact,
	sandbox_andre_matias.t_lkp_base_source lkp_base_source,
	sandbox_andre_matias.t_lkp_currency lkp_currency,
	sandbox_andre_matias.t_lkp_stage lkp_stages,
	sandbox_andre_matias.t_lkp_loss_reason lkp_loss_reason,
    sandbox_andre_matias.t_lkp_deal target
  where
	source_table.opr_deal = target.opr_deal(+)
	and source_table.cod_source_system = target.cod_source_system (+)
    and target.valid_to(+) = 20991231
	and isnull(source_table.opr_base_user_owner,-1) = lkp_base_user_owner.opr_base_user
	and source_table.cod_source_system = lkp_base_user_owner.cod_source_system -- new
	and lkp_base_user_owner.valid_to = 20991231
    and isnull(source_table.opr_base_user_creator,-1) = lkp_base_user_creator.opr_base_user
	and source_table.cod_source_system = lkp_base_user_creator.cod_source_system -- new
	and lkp_base_user_creator.valid_to = 20991231
    and isnull(source_table.opr_currency,'') = lkp_currency.opr_currency
    and lkp_currency.valid_to = 20991231
	and isnull(source_table.opr_loss_reason,-1) = lkp_loss_reason.opr_loss_reason 
    and lkp_currency.valid_to = 20991231
	and isnull(source_table.opr_stage,-1) = lkp_stages.opr_stage
	and source_table.cod_source_system = lkp_stages.cod_source_system -- new
    and lkp_currency.valid_to = 20991231
	and isnull(source_table.opr_base_source,-1) = lkp_base_source.opr_base_source 
	and lkp_base_source.valid_to = 20991231
	and isnull(source_table.opr_contact,-1) = lkp_contact.opr_contact
	and source_table.cod_source_system = lkp_contact.cod_source_system -- new
	and lkp_contact.valid_to = 20991231;

	$$$
	
delete from sandbox_andre_matias.t_lkp_deal
using sandbox_andre_matias.tmp_load_deals
where 
	tmp_load_deals.dml_type = 'I' 
	and t_lkp_deal.opr_deal = tmp_load_deals.opr_deal
	and t_lkp_deal.valid_from = (select dat_processing from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_deal');

	$$$
	
update sandbox_andre_matias.t_lkp_deal
set valid_to = (select rel_integr_proc.dat_processing from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_deal') 
from sandbox_andre_matias.tmp_load_deals source
where source.cod_deal = sandbox_andre_matias.t_lkp_deal.cod_deal
and sandbox_andre_matias.t_lkp_deal.valid_to = 20991231
and source.dml_type in('U','D');
	
insert into sandbox_andre_matias.t_lkp_deal
    select
      case
        when dml_type = 'I' then max_cod + new_cod
        when dml_type = 'U' then cod_deal
      end cod_deal,
      opr_deal,
      dsc_deal,
      cod_source_system,
      (select rel_integr_proc.dat_processing from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_deal') valid_from, 
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
      hash_deal
    from
      sandbox_andre_matias.tmp_load_deals
    where
      dml_type in ('U','I');

	$$$
	
-- #######################
-- ####    PASSO 5    ####
-- #######################
insert into sandbox_andre_matias.t_fac_scai_execution
  select
    max_cod_exec + 1 cod_execution,
    rel_integr_proc.cod_integration,
    rel_integr_proc.cod_process,
    1 cod_status,
    2 cod_execution_type, -- End
    rel_integr_proc.dat_processing,
    rel_integr_proc.execution_nbr,
    sysdate
  from
    sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr,
    (select isnull(max(cod_execution),0) max_cod_exec from sandbox_andre_matias.t_fac_scai_execution),
    sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc,
    sandbox_andre_matias.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 1 -- Portugal
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
update sandbox_andre_matias.t_rel_scai_integration_process
set cod_status = 1, -- Ok
last_processing_datetime = isnull((select max(updated_at) from sandbox_andre_matias.tmp_load_deals),last_processing_datetime)
/*from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc, sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_deal'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 1
  ) source*/
from sandbox_andre_matias.t_lkp_scai_process proc 
where t_rel_scai_integration_process.cod_process = proc.cod_process
and t_rel_scai_integration_process.cod_status = 2
and t_rel_scai_integration_process.cod_country = 1
and proc.dsc_process_short = 't_lkp_deal'
and t_rel_scai_integration_process.ind_active = 1
/*sandbox_andre_matias.t_rel_scai_integration_process.cod_process = source.cod_process
and sandbox_andre_matias.t_rel_scai_integration_process.cod_country = source.cod_country
and sandbox_andre_matias.t_rel_scai_integration_process.cod_integration = source.cod_integration*/;

drop table if exists sandbox_andre_matias.tmp_load_deals;

	$$$
	
-- #######################
-- ####    PASSO 3    ####
-- #######################
update sandbox_andre_matias.t_rel_scai_integration_process
set dat_processing = source.dat_processing, execution_nbr = source.execution_nbr, cod_status = 2 -- Running
from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc, sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_fac_call'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 1
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
  ) source
where sandbox_andre_matias.t_rel_scai_integration_process.cod_process = source.cod_process
and sandbox_andre_matias.t_rel_scai_integration_process.cod_country = source.cod_country
and sandbox_andre_matias.t_rel_scai_integration_process.cod_integration = source.cod_integration;


-- #######################
-- ####    PASSO 4    ####
-- #######################
insert into sandbox_andre_matias.t_fac_scai_execution
  select
    max_cod_exec + 1 cod_execution,
    rel_integr_proc.cod_integration,
    rel_integr_proc.cod_process,
    rel_integr_proc.cod_status,
    1 cod_execution_type, -- Begin
    rel_integr_proc.dat_processing,
    rel_integr_proc.execution_nbr,
    sysdate
  from
    sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr,
    (select isnull(max(cod_execution),0) max_cod_exec from sandbox_andre_matias.t_fac_scai_execution),
    sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc,
    sandbox_andre_matias.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 1 -- Portugal
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_fac_call';	

	$$$
	
-- #############################################
-- # 	          BASE - Portugal              #
-- #           LOADING t_fac_call             #
-- #############################################

drop table if exists sandbox_andre_matias.tmp_load_calls;

create table sandbox_andre_matias.tmp_load_calls 
distkey(cod_source_system)
sortkey(opr_call)
as
  select source.*, isnull(lkp_call_outcome.cod_call_outcome,-1) cod_call_outcome
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
    lkp_base_user.cod_base_user,
    source_table.opr_call_outcome,
    lkp_resource_type.cod_resource_type,
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
		md5(isnull(opr_base_user,0) +
            isnull(phone_number,'') +
            decode(flg_missed,1,1,0) +
            isnull(opr_associated_deal,'') +
            isnull(summary,'') +
            isnull(opr_call_outcome,0) +
            isnull(call_duration,0) +
            decode(flg_incoming,1,1,0) +
            isnull(recording_url,'') +
            isnull(opr_resource_type,'')
			) hash_call
	from
	(
      SELECT
        base_account_country + base_account_category opr_source_system,
        id opr_call,
        user_id opr_base_user,
        phone_number,
        missed flg_missed,
        associated_deal_ids opr_associated_deal, -- TODO: ESTA COLUNA PRECISA QUE SEJA APLICADO UM TRANSPOSE
        made_at created_at,
        updated_at,
        summary,
        outcome_id opr_call_outcome,
        duration call_duration,
        incoming flg_incoming,
        recording_url,
        resource_type opr_resource_type,
		b.cod_source_system,
		row_number() over (partition by id order by meta_event_type desc) rn
      FROM
        rdl_basecrm_v2.stg_d_base_calls a,
        sandbox_andre_matias.t_lkp_source_system b
      WHERE
        updated_at >= (select isnull(rel_integr_proc.last_processing_datetime,'1900-01-01 00:00:00.000000') from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_fac_call')
        and (base_account_country + base_account_category) = opr_source_system
        and cod_country = 1
		--and 1 = 0
	) 
	) source_table,
    sandbox_andre_matias.t_lkp_base_user lkp_base_user,
    sandbox_andre_matias.t_lkp_resource_type lkp_resource_type,
    (select isnull(max(cod_call),0) max_cod from sandbox_andre_matias.t_fac_call) max_cod_calls,
    sandbox_andre_matias.t_fac_call target
  where
	source_table.opr_call = target.opr_call(+)
	and source_table.cod_source_system = target.cod_source_system (+)
    and isnull(source_table.opr_base_user,-1) = lkp_base_user.opr_base_user
	and source_table.cod_source_system = lkp_base_user.cod_source_system -- new
    and lkp_base_user.valid_to = 20991231
    and isnull(source_table.opr_resource_type,'') = lkp_resource_type.opr_resource_type
    and lkp_resource_type.valid_to = 20991231
	and source_table.rn = 1
    ) source,
    sandbox_andre_matias.t_lkp_call_outcome lkp_call_outcome
  where
  isnull(source.opr_call_outcome,-1) = lkp_call_outcome.opr_call_outcome (+)
	and source.cod_source_system = lkp_call_outcome.cod_source_system (+)
    and lkp_call_outcome.valid_to (+) = 20991231;

	$$$
	
insert into sandbox_andre_matias.t_hst_call
    select
      target.*
    from
      sandbox_andre_matias.t_fac_call target,
      sandbox_andre_matias.tmp_load_calls source
    where
      target.opr_call = source.opr_call
      and source.dml_type = 'U';

	$$$
	
delete from sandbox_andre_matias.t_fac_call
using sandbox_andre_matias.tmp_load_calls
where sandbox_andre_matias.t_fac_call.opr_call = sandbox_andre_matias.tmp_load_calls.opr_call
and sandbox_andre_matias.tmp_load_calls.dml_type = 'U';

	$$$
	
insert into sandbox_andre_matias.t_fac_call
    select
      case
        when dml_type = 'I' then max_cod + new_cod
        when dml_type = 'U' then cod_call
      end cod_call,
      opr_call,
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
      hash_call
    from
      sandbox_andre_matias.tmp_load_calls
    where
      dml_type in ('U','I');

	$$$
	
insert into sandbox_andre_matias.t_fac_call_deal
select
  all_deals.cod_source_system,
  cod_call,
  case when cod_deal is null then -1 else cod_deal end cod_deal
from
  (
        SELECT
          b.cod_source_system,
          case when cod_call is null then new_cod else cod_call end cod_call,
          opr_associated_deal,
          replace(replace(opr_associated_deal,'[',''),']','') cases -- 0
        FROM
          sandbox_andre_matias.tmp_load_calls a,
          sandbox_andre_matias.t_lkp_source_system b
        WHERE
          a.cod_source_system = b.cod_source_system
          and cod_country = 1
          and REGEXP_COUNT (opr_associated_deal,'[,]{1}') = 0
        union
        SELECT
          b.cod_source_system,
          case when cod_call is null then new_cod else cod_call end cod_call,
          opr_associated_deal,
          replace(replace(split_part(opr_associated_deal, ',', 1),'[',''),']','') cases -- 1
        FROM
          sandbox_andre_matias.tmp_load_calls a,
          sandbox_andre_matias.t_lkp_source_system b
        WHERE
          a.cod_source_system = b.cod_source_system
          and cod_country = 1
          and REGEXP_COUNT (opr_associated_deal,'[,]{1}') > 0
        union
        SELECT
          b.cod_source_system,
          case when cod_call is null then new_cod else cod_call end cod_call,
          opr_associated_deal,
          replace(replace(split_part(opr_associated_deal, ',', 2),']',''),'[','') cases -- 2
        FROM
          sandbox_andre_matias.tmp_load_calls a,
          sandbox_andre_matias.t_lkp_source_system b
        WHERE
          a.cod_source_system = b.cod_source_system
          and cod_country = 1
          and REGEXP_COUNT (opr_associated_deal,'[,]{1}') > 0
        union
        SELECT
          b.cod_source_system,
          case when cod_call is null then new_cod else cod_call end cod_call,
          opr_associated_deal,
          replace(replace(split_part(opr_associated_deal, ',', 3),']',''),'[','') cases -- 3
        FROM
          sandbox_andre_matias.tmp_load_calls a,
          sandbox_andre_matias.t_lkp_source_system b
        WHERE
          a.cod_source_system = b.cod_source_system
          and cod_country = 1
          and REGEXP_COUNT (opr_associated_deal,'[,]{1}') > 0
        union
        SELECT
          b.cod_source_system,
          case when cod_call is null then new_cod else cod_call end cod_call,
          opr_associated_deal,
          replace(replace(split_part(opr_associated_deal, ',', 4),']',''),'[','') cases -- 4
        FROM
          sandbox_andre_matias.tmp_load_calls a,
          sandbox_andre_matias.t_lkp_source_system b
        WHERE
          a.cod_source_system = b.cod_source_system
          and cod_country = 1
          and REGEXP_COUNT (opr_associated_deal,'[,]{1}') > 0
        union
        SELECT
          b.cod_source_system,
          case when cod_call is null then new_cod else cod_call end cod_call,
          opr_associated_deal,
          replace(replace(split_part(opr_associated_deal, ',', 5),']',''),'[','') cases -- 5
        FROM
          sandbox_andre_matias.tmp_load_calls a,
          sandbox_andre_matias.t_lkp_source_system b
        WHERE
          a.cod_source_system = b.cod_source_system
          and cod_country = 1
          and REGEXP_COUNT (opr_associated_deal,'[,]{1}') > 0
) all_deals, sandbox_andre_matias.t_lkp_deal lkp_deals
where all_deals.cases = lkp_deals.opr_deal (+)
and all_deals.cod_source_system = lkp_deals.cod_source_system (+)
and cases != ''
union all
SELECT
    b.cod_source_system,
    case when cod_call is null then new_cod else cod_call end cod_call,
    -2 cod_deal
FROM
    sandbox_andre_matias.tmp_load_calls a,
    sandbox_andre_matias.t_lkp_source_system b
WHERE
    a.cod_source_system = b.cod_source_system
    and cod_country = 1
    and opr_associated_deal = '[]';

	$$$
	
-- #######################
-- ####    PASSO 5    ####
-- #######################
insert into sandbox_andre_matias.t_fac_scai_execution
  select
    max_cod_exec + 1 cod_execution,
    rel_integr_proc.cod_integration,
    rel_integr_proc.cod_process,
    1 cod_status,
    2 cod_execution_type, -- End
    rel_integr_proc.dat_processing,
    rel_integr_proc.execution_nbr,
    sysdate
  from
    sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr,
    (select isnull(max(cod_execution),0) max_cod_exec from sandbox_andre_matias.t_fac_scai_execution),
    sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc,
    sandbox_andre_matias.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 1 -- Portugal
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
update sandbox_andre_matias.t_rel_scai_integration_process
set cod_status = 1, -- Ok
last_processing_datetime = isnull((select max(updated_at) from sandbox_andre_matias.tmp_load_calls),last_processing_datetime)
/*from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc, sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_fac_call'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 1
  ) source*/
from sandbox_andre_matias.t_lkp_scai_process proc 
where t_rel_scai_integration_process.cod_process = proc.cod_process
and t_rel_scai_integration_process.cod_status = 2
and t_rel_scai_integration_process.cod_country = 1
and proc.dsc_process_short = 't_fac_call'
and t_rel_scai_integration_process.ind_active = 1
/*sandbox_andre_matias.t_rel_scai_integration_process.cod_process = source.cod_process
and sandbox_andre_matias.t_rel_scai_integration_process.cod_country = source.cod_country
and sandbox_andre_matias.t_rel_scai_integration_process.cod_integration = source.cod_integration*/;

drop table if exists sandbox_andre_matias.tmp_load_calls;

	$$$
	
-- #######################
-- ####    PASSO 3    ####
-- #######################
update sandbox_andre_matias.t_rel_scai_integration_process
set dat_processing = source.dat_processing, execution_nbr = source.execution_nbr, cod_status = 2 -- Running
from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc, sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_fac_order'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 1
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
  ) source
where sandbox_andre_matias.t_rel_scai_integration_process.cod_process = source.cod_process
and sandbox_andre_matias.t_rel_scai_integration_process.cod_country = source.cod_country
and sandbox_andre_matias.t_rel_scai_integration_process.cod_integration = source.cod_integration;


-- #######################
-- ####    PASSO 4    ####
-- #######################
insert into sandbox_andre_matias.t_fac_scai_execution
  select
    max_cod_exec + 1 cod_execution,
    rel_integr_proc.cod_integration,
    rel_integr_proc.cod_process,
    rel_integr_proc.cod_status,
    1 cod_execution_type, -- Begin
    rel_integr_proc.dat_processing,
    rel_integr_proc.execution_nbr,
    sysdate
  from
    sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr,
    (select isnull(max(cod_execution),0) max_cod_exec from sandbox_andre_matias.t_fac_scai_execution),
    sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc,
    sandbox_andre_matias.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 1 -- Portugal
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_fac_order';	

	$$$
	
-- #############################################
-- # 	          BASE - Portugal              #
-- #           LOADING t_fac_order            #
-- #############################################

drop table if exists sandbox_andre_matias.tmp_load_orders;

create table sandbox_andre_matias.tmp_load_orders 
distkey(cod_source_system)
sortkey(opr_order)
as
  select source.*, isnull(lkp_deals.cod_deal,-1) cod_deal
from
  (
  select
    --source_table.opr_source_system,
    source_table.dat_order,
    source_table.opr_order,
    source_table.opr_deal,
    source_table.val_discount,
    source_table.created_at,
    source_table.updated_at,
    source_table.hash_order,
    source_table.cod_source_system,
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
        md5(isnull(deal_id,0) +
            isnull(discount,0)
        ) hash_order
      FROM
        rdl_basecrm_v2.stg_d_base_orders a,
        sandbox_andre_matias.t_lkp_source_system b
      WHERE
        updated_at >= (select isnull(rel_integr_proc.last_processing_datetime,'1900-01-01 00:00:00.000000') from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_fac_order')
		and base_account_country + base_account_category = opr_source_system
        and cod_country = 1
		--and 1 = 0
	) source_table,
    (select isnull(max(cod_order),0) max_cod from sandbox_andre_matias.t_fac_order) max_cod_orders,
    sandbox_andre_matias.t_fac_order target
  where
    source_table.opr_order = target.opr_order(+)
	and source_table.cod_source_system = target.cod_source_system (+)
    and source_table.rn = 1
  ) source, sandbox_andre_matias.t_lkp_deal lkp_deals
  where
    source.opr_deal = lkp_deals.opr_deal (+)
	and source.cod_source_system = lkp_deals.cod_source_system (+)
    and lkp_deals.valid_to (+) = 20991231;

	$$$
	
insert into sandbox_andre_matias.t_hst_order
    select
      target.*
    from
      sandbox_andre_matias.t_fac_order target,
      sandbox_andre_matias.tmp_load_orders source
    where
      target.opr_order = source.opr_order
      and source.dml_type = 'U';

	$$$
	
delete from sandbox_andre_matias.t_fac_order
using sandbox_andre_matias.tmp_load_orders
where sandbox_andre_matias.t_fac_order.opr_order = sandbox_andre_matias.tmp_load_orders.opr_order
and sandbox_andre_matias.tmp_load_orders.dml_type = 'U';

	$$$
	
insert into sandbox_andre_matias.t_fac_order
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
      hash_order
    from
      sandbox_andre_matias.tmp_load_orders
    where
      dml_type in ('U','I');

-- #######################
-- ####    PASSO 5    ####
-- #######################
insert into sandbox_andre_matias.t_fac_scai_execution
  select
    max_cod_exec + 1 cod_execution,
    rel_integr_proc.cod_integration,
    rel_integr_proc.cod_process,
    1 cod_status,
    2 cod_execution_type, -- End
    rel_integr_proc.dat_processing,
    rel_integr_proc.execution_nbr,
    sysdate
  from
    sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr,
    (select isnull(max(cod_execution),0) max_cod_exec from sandbox_andre_matias.t_fac_scai_execution),
    sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc,
    sandbox_andre_matias.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 1 -- Portugal
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
update sandbox_andre_matias.t_rel_scai_integration_process
set cod_status = 1, -- Ok
last_processing_datetime = isnull((select max(updated_at) from sandbox_andre_matias.tmp_load_orders),last_processing_datetime)
/*from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc, sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_fac_order'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 1
  ) source*/
from sandbox_andre_matias.t_lkp_scai_process proc 
where t_rel_scai_integration_process.cod_process = proc.cod_process
and t_rel_scai_integration_process.cod_status = 2
and t_rel_scai_integration_process.cod_country = 1
and proc.dsc_process_short = 't_fac_order'
and t_rel_scai_integration_process.ind_active = 1
/*sandbox_andre_matias.t_rel_scai_integration_process.cod_process = source.cod_process
and sandbox_andre_matias.t_rel_scai_integration_process.cod_country = source.cod_country
and sandbox_andre_matias.t_rel_scai_integration_process.cod_integration = source.cod_integration*/;

drop table if exists sandbox_andre_matias.tmp_load_orders;

	$$$
	
-- #######################
-- ####    PASSO 3    ####
-- #######################
update sandbox_andre_matias.t_rel_scai_integration_process
set dat_processing = source.dat_processing, execution_nbr = source.execution_nbr, cod_status = 2 -- Running
from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc, sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_fac_order_line_item'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 1
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
  ) source
where sandbox_andre_matias.t_rel_scai_integration_process.cod_process = source.cod_process
and sandbox_andre_matias.t_rel_scai_integration_process.cod_country = source.cod_country
and sandbox_andre_matias.t_rel_scai_integration_process.cod_integration = source.cod_integration;


-- #######################
-- ####    PASSO 4    ####
-- #######################
insert into sandbox_andre_matias.t_fac_scai_execution
  select
    max_cod_exec + 1 cod_execution,
    rel_integr_proc.cod_integration,
    rel_integr_proc.cod_process,
    rel_integr_proc.cod_status,
    1 cod_execution_type, -- Begin
    rel_integr_proc.dat_processing,
    rel_integr_proc.execution_nbr,
    sysdate
  from
    sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr,
    (select isnull(max(cod_execution),0) max_cod_exec from sandbox_andre_matias.t_fac_scai_execution),
    sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc,
    sandbox_andre_matias.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 1 -- Portugal
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_fac_order_line_item';	

	$$$
	
-- #############################################
-- # 	          BASE - Portugal              #
-- #      LOADING t_fac_order_line_item       #
-- #############################################

drop table if exists sandbox_andre_matias.tmp_load_order_line_items;

create table sandbox_andre_matias.tmp_load_order_line_items 
distkey(cod_source_system)
sortkey(opr_order_line_item)
as
  select source.*, isnull(lkp_orders.cod_order) cod_order
  from
  (
  select
    --source_table.opr_source_system,
    source_table.opr_order_line_item,
    --source_table.opr_sku,
    source_table.dsc_order_line_item,
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
    lkp_product.cod_sku,
    source_table.opr_order,
    lkp_currency.cod_currency,
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
         md5(isnull(sku,'') + isnull(description,'') + isnull(order_id,0) + isnull(deal_id,0)
            + cast(isnull(value,0) as varchar) + cast(isnull(price,0) as varchar) + isnull(currency,'')
            + cast(isnull(variation,0) as varchar) + isnull(quantity,0)
        ) hash_order_line_item
      FROM
        rdl_basecrm_v2.stg_d_base_line_items a,
        sandbox_andre_matias.t_lkp_source_system b
      WHERE
        updated_at >= (select isnull(rel_integr_proc.last_processing_datetime,'1900-01-01 00:00:00.000000') from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_fac_order_line_item')
		and base_account_country + base_account_category = opr_source_system
        and cod_country = 1
		--and 1 = 0
	) source_table,
    sandbox_andre_matias.t_lkp_product lkp_product,
    sandbox_andre_matias.t_lkp_currency lkp_currency,
    (select isnull(max(cod_order_line_item),0) max_cod from sandbox_andre_matias.t_fac_order_line_item) max_cod_order_line_items,
    sandbox_andre_matias.t_fac_order_line_item target
  where
    isnull(source_table.opr_order_line_item,-1) = target.opr_order_line_item(+)
	and source_table.cod_source_system = target.cod_source_system (+)
    and isnull(source_table.opr_sku,'') = lkp_product.opr_sku
	and source_table.cod_source_system = lkp_product.cod_source_system -- new
    and lkp_product.valid_to = 20991231
    and isnull(source_table.opr_currency,'') = lkp_currency.opr_currency
    and lkp_currency.valid_to = 20991231
	and source_table.rn = 1
  ) source,
    sandbox_andre_matias.t_fac_order lkp_orders
  where
	isnull(source.opr_order,-1) = lkp_orders.opr_order (+)-- TAMBÉM DEVEREMOS CONSIDERAR A DATA DAT_ORDER
	and source.cod_source_system = lkp_orders.cod_source_system (+); -- new

	$$$
	
insert into sandbox_andre_matias.t_hst_order_line_item
    select
      target.*
    from
      sandbox_andre_matias.t_fac_order_line_item target,
      sandbox_andre_matias.tmp_load_order_line_items source
    where
      target.opr_order_line_item = source.opr_order_line_item
      and source.dml_type = 'U';

	$$$
	
delete from sandbox_andre_matias.t_fac_order_line_item
using sandbox_andre_matias.tmp_load_order_line_items
where sandbox_andre_matias.t_fac_order_line_item.opr_order_line_item=sandbox_andre_matias.tmp_load_order_line_items.opr_order_line_item
and sandbox_andre_matias.tmp_load_order_line_items.dml_type = 'U';

	$$$
	
insert into sandbox_andre_matias.t_fac_order_line_item
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
	  hash_order_line_item
    from
      sandbox_andre_matias.tmp_load_order_line_items
    where
      dml_type in ('U','I');

	$$$
	
-- #######################
-- ####    PASSO 5    ####
-- #######################
insert into sandbox_andre_matias.t_fac_scai_execution
  select
    max_cod_exec + 1 cod_execution,
    rel_integr_proc.cod_integration,
    rel_integr_proc.cod_process,
    1 cod_status,
    2 cod_execution_type, -- End
    rel_integr_proc.dat_processing,
    rel_integr_proc.execution_nbr,
    sysdate
  from
    sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr,
    (select isnull(max(cod_execution),0) max_cod_exec from sandbox_andre_matias.t_fac_scai_execution),
    sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc,
    sandbox_andre_matias.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 1 -- Portugal
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
update sandbox_andre_matias.t_rel_scai_integration_process
set cod_status = 1, -- Ok
last_processing_datetime = isnull((select max(updated_at) from sandbox_andre_matias.tmp_load_order_line_items),last_processing_datetime)
/*from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc, sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_fac_order_line_item'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 1
  ) source*/
from sandbox_andre_matias.t_lkp_scai_process proc 
where t_rel_scai_integration_process.cod_process = proc.cod_process
and t_rel_scai_integration_process.cod_status = 2
and t_rel_scai_integration_process.cod_country = 1
and proc.dsc_process_short = 't_fac_order_line_item'
and t_rel_scai_integration_process.ind_active = 1
/*sandbox_andre_matias.t_rel_scai_integration_process.cod_process = source.cod_process
and sandbox_andre_matias.t_rel_scai_integration_process.cod_country = source.cod_country
and sandbox_andre_matias.t_rel_scai_integration_process.cod_integration = source.cod_integration*/;

drop table if exists sandbox_andre_matias.tmp_load_order_line_items;

	$$$
	
-- #######################
-- ####    PASSO 3    ####
-- #######################
update sandbox_andre_matias.t_rel_scai_integration_process
set dat_processing = source.dat_processing, execution_nbr = source.execution_nbr, cod_status = 2 -- Running
from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc, sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_category'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 1
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
  ) source
where sandbox_andre_matias.t_rel_scai_integration_process.cod_process = source.cod_process
and sandbox_andre_matias.t_rel_scai_integration_process.cod_country = source.cod_country
and sandbox_andre_matias.t_rel_scai_integration_process.cod_integration = source.cod_integration;


-- #######################
-- ####    PASSO 4    ####
-- #######################
insert into sandbox_andre_matias.t_fac_scai_execution
  select
    max_cod_exec + 1 cod_execution,
    rel_integr_proc.cod_integration,
    rel_integr_proc.cod_process,
    rel_integr_proc.cod_status,
    1 cod_execution_type, -- Begin
    rel_integr_proc.dat_processing,
    rel_integr_proc.execution_nbr,
    sysdate
  from
    sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr,
    (select isnull(max(cod_execution),0) max_cod_exec from sandbox_andre_matias.t_fac_scai_execution),
    sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc,
    sandbox_andre_matias.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 1 -- Portugal
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_lkp_category';	

	$$$
	
-- #############################################
-- # 		     ATLAS - PORTUGAL              #
-- #          LOADING t_lkp_category           #
-- #############################################

drop table if exists sandbox_andre_matias.tmp_load_category;

create table sandbox_andre_matias.tmp_load_category 
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
    max_cod_category.max_cod,
    row_number() over (order by source_table.opr_category desc) new_cod,
    target.cod_category,
    case
      --when target.cod_category is null then 'I'
	  when target.cod_category is null or (source_table.hash_category != target.hash_category and target.valid_from = (select dat_processing from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_category')) then 'I'
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
		isnull(dsc_category_pl                                                                ,'') +
		isnull(dsc_category_pt                                                                ,'') +
		isnull(dsc_category_en                                                                ,'') +
		isnull(dsc_category_ro                                                                ,'') +
		isnull(dsc_category_ru                                                                ,'') +
		isnull(dsc_category_hi                                                                ,'') +
		isnull(dsc_category_uk                                                                ,'') +
		isnull(opr_category_parent                                                            ,0) +
		isnull(category_code                                                                  ,'') +
		isnull(offer_name_pl                                                                  ,'') +
		isnull(offer_name_en                                                                  ,'') +
		isnull(seek_name_pl                                                                   ,'') +
		isnull(seek_name_en                                                                   ,'') +
		isnull(private_name_pl                                                                ,'') +
		isnull(private_name_en                                                                ,'') +
		isnull(private_name_adding_pl                                                         ,'') +
		isnull(private_name_adding_en                                                         ,'') +
		isnull(business_name_pl                                                               ,'') +
		isnull(business_name_en                                                               ,'') +
		isnull(business_name_adding_pl                                                        ,'') +
		isnull(business_name_adding_en                                                        ,'') +
		isnull(offer_name_adding_pl                                                           ,'') +
		isnull(offer_name_adding_en                                                           ,'') +
		isnull(seek_name_adding_pl                                                            ,'') +
		isnull(seek_name_adding_en                                                            ,'') +
		isnull(flg_premoderated                                                               ,0) +
		isnull(display_order                                                                  ,0) +
		isnull(flg_offer_seek                                                                 ,0) +
		isnull(flg_private_business                                                           ,0) +
		isnull(flg_remove_companies                                                           ,0) +
		isnull(max_photos                                                                     ,0) +
		isnull(extend_days                                                                    ,0) +
		isnull(default_currency                                                               ,'') +
		isnull(filter_label_pl                                                                ,'') +
		isnull(filter_label_en                                                                ,'') +
		isnull(search_category                                                                ,0) +
		isnull(search_args                                                                    ,'') +
		isnull(search_routing_params                                                          ,'') +
		isnull(flg_rmoderation_checkhistory                                                   ,0) +
		cast(isnull(rmoderation_min_price                                                      ,0) as varchar) +
		isnull(rmoderation_hotkey                                                             ,'') +
		isnull(flg_rmoderation_block_new_price                                                ,0) +
		isnull(flg_rmoderation_can_accept_automatically                                       ,0) +
		isnull(address_label_pl                                                               ,'') +
		isnull(address_label_en                                                               ,'') +
		isnull(cod_category_meta                                                              ,0) +
		isnull(topads_count                                                                   ,0) +
		isnull(default_view                                                                   ,'') +
		isnull(default_mobile_view                                                            ,'') +
		isnull(related_categories                                                             ,'') +
		isnull(hint_description                                                               ,'') +
		isnull(flg_show_map                                                                   ,0) +
		isnull(title_parameters                                                               ,'') +
		isnull(flg_for_sale_category                                                          ,0) +
		isnull(flg_prioritized                                                                ,0) +
		isnull(default_price_type                                                             ,'') +
		isnull(flg_use_name_in_solr                                                           ,0) +
		isnull(flg_allow_exchange                                                             ,0) +
		isnull(legacy_code                                                                    ,'') +
		isnull(dsc_category_long_pl                                                           ,'') +
		isnull(dsc_category_long_en                                                           ,'') +
		isnull(dsc_category_singular_pl                                                       ,'') +
		isnull(dsc_category_singular_en                                                       ,'') +
		isnull(dsc_category_singular_pt                                                       ,'') +
		isnull(title_format                                                                   ,'') +
		isnull(flg_has_free_text_search                                                       ,0) +
		isnull(title_format_description                                                       ,'') +
		isnull(path_params                                                                    ,'') +
		isnull(seek_name_adding_pt                                                            ,'') +
		isnull(private_name_pt                                                                ,'') +
		isnull(private_name_adding_pt                                                         ,'') +
		isnull(offer_name_pt                                                                  ,'') +
		isnull(offer_name_adding_pt                                                           ,'') +
		isnull(name_pt                                                                        ,'') +
		isnull(dsc_category_long_pt                                                           ,'') +
		isnull(filter_label_pt                                                                ,'') +
		isnull(business_name_pt                                                               ,'') +
		isnull(business_name_adding_pt                                                        ,'') +
		isnull(address_label_pt                                                               ,'') +
		isnull(short_name_with_pronoun_pt                                                     ,'') +
		isnull(short_name_with_pronoun_en                                                     ,'') +
		isnull(short_name_with_pronoun_ro                                                     ,'') +
		isnull(short_name_with_pronoun_hi                                                     ,'') +
		isnull(short_name_pt                                                                  ,'') +
		isnull(short_name_en                                                                  ,'') +
		isnull(short_name_ro                                                                  ,'') +
		isnull(short_name_hi                                                                  ,'') +
		isnull(seek_name_pt                                                                   ,'') +
		isnull(genitive_name_pt                                                               ,'') +
		isnull(genitive_name_en                                                               ,'') +
		isnull(genitive_name_ro                                                               ,'') +
		isnull(genitive_name_ru                                                               ,'') +
		isnull(genitive_name_hi                                                               ,'') +
		isnull(genitive_name_uk                                                               ,'') +
		isnull(dsc_category_singular_ro                                                       ,'') +
		isnull(dsc_category_singular_ru                                                       ,'') +
		isnull(dsc_category_singular_uk                                                       ,'') +
		isnull(seek_name_ro                                                                   ,'') +
		isnull(seek_name_adding_ro                                                            ,'') +
		isnull(private_name_ro                                                                ,'') +
		isnull(private_name_adding_ro                                                         ,'') +
		isnull(offer_name_ro                                                                  ,'') +
		isnull(offer_name_adding_ro                                                           ,'') +
		isnull(name_ro                                                                        ,'') +
		isnull(dsc_category_long_ro                                                           ,'') +
		isnull(dsc_category_long_ru                                                           ,'') +
		isnull(dsc_category_long_uk                                                           ,'') +
		isnull(filter_label_ro                                                                ,'') +
		isnull(business_name_ro                                                               ,'') +
		isnull(business_name_adding_ro                                                        ,'') +
		isnull(address_label_ro                                                               ,'') +
		isnull(genitive_name_pl                                                               ,'') +
		isnull(short_name_pl                                                                  ,'') +
		isnull(short_name_with_pronoun_pl                                                     ,'') +
		isnull(address_label_ru                                                               ,'') +
		isnull(address_label_uk                                                               ,'') +
		isnull(business_name_adding_ru                                                        ,'') +
		isnull(business_name_adding_uk                                                        ,'') +
		isnull(business_name_ru                                                               ,'') +
		isnull(business_name_uk                                                               ,'') +
		isnull(filter_label_ru                                                                ,'') +
		isnull(filter_label_uk                                                                ,'') +
		isnull(name_ru                                                                        ,'') +
		isnull(name_uk                                                                        ,'') +
		isnull(offer_name_adding_ru                                                           ,'') +
		isnull(offer_name_adding_uk                                                           ,'') +
		isnull(offer_name_ru                                                                  ,'') +
		isnull(offer_name_uk                                                                  ,'') +
		isnull(private_name_adding_ru                                                         ,'') +
		isnull(private_name_adding_uk                                                         ,'') +
		isnull(private_name_ru                                                                ,'') +
		isnull(private_name_uk                                                                ,'') +
		isnull(seek_name_adding_ru                                                            ,'') +
		isnull(seek_name_adding_uk                                                            ,'') +
		isnull(seek_name_ru                                                                   ,'') +
		isnull(seek_name_uk                                                                   ,'') +
		isnull(short_name_ru                                                                  ,'') +
		isnull(short_name_uk                                                                  ,'') +
		isnull(short_name_with_pronoun_ru                                                     ,'') +
		isnull(short_name_with_pronoun_uk                                                     ,'') +
		cast(isnull(rmoderation_max_price                                                      ,0) as varchar) +
		isnull(address_label_hi                                                               ,'') +
		isnull(business_name_adding_hi                                                        ,'') +
		isnull(business_name_hi                                                               ,'') +
		isnull(filter_label_hi                                                                ,'') +
		isnull(dsc_category_long_hi                                                           ,'') +
		isnull(name_hi                                                                        ,'') +
		isnull(offer_name_adding_hi                                                           ,'') +
		isnull(offer_name_hi                                                                  ,'') +
		isnull(private_name_adding_hi                                                         ,'') +
		isnull(private_name_hi                                                                ,'') +
		isnull(seek_name_adding_hi                                                            ,'') +
		isnull(seek_name_hi                                                                   ,'') +
		isnull(dsc_category_singular_hi                                                       ,'') +
		isnull(flg_for_sale                                                                   ,0) +
		isnull(paidlimits_packet_id                                                           ,0) +
		isnull(free_ads_limit                                                                 ,0) +
		isnull(app_ads_fb                                                                     ,0) +
		isnull(app_ads_admob                                                                  ,0) +
		isnull(app_ads_admob_ad_campaign                                                      ,'') +
		isnull(app_ads_admob_ads_campaign                                                     ,'') +
		isnull(parent_level1                                                                  ,0) +
		isnull(parent_level2                                                                  ,0) +
		isnull(flg_leaf                                                                       ,0)
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
		null dsc_category_ro, -- não existe de todo
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
		cast(null as bigint) flg_leaf
     FROM
        sandbox_andre_matias.stg_db_atlas_verticals_categories a,
		sandbox_andre_matias.t_lkp_source_system b
	  where
		a.livesync_dbname = b.opr_source_system
		and b.cod_business_type = 1 -- Verticals
		and b.cod_country = 1 -- Portugal
		--and 1 = 0
	  union all
	  SELECT
	  	id opr_category,
		'olxpt' opr_source_system,
		operation_type,
		operation_timestamp,
		null dsc_category_pl,
		null dsc_category_pt, -- não existe de todo
		null dsc_category_en,
		null dsc_category_ro, -- não existe de todo
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
		seek_name_adding_pt,
		private_name_pt,
		private_name_adding_pt,
		offer_name_pt,
		offer_name_adding_pt,
		name_pt,
		null dsc_category_long_pt,
		filter_label_pt,
		business_name_pt,
		business_name_adding_pt,
		address_label_pt,
		null short_name_with_pronoun_pt,
		null short_name_with_pronoun_en,
		null short_name_with_pronoun_ro, -- não existe de todo
		null short_name_with_pronoun_hi, -- não existe de todo
		null short_name_pt,
		null short_name_en,
		null short_name_ro, -- não existe de todo
		null short_name_hi, -- não existe de todo
		seek_name_pt,
		null genitive_name_pt,
		null genitive_name_en,
		null genitive_name_ro, -- não existe de todo
		null genitive_name_ru, -- não existe de todo
		null genitive_name_hi, -- não existe de todo
		null genitive_name_uk, -- não existe de todo
		null dsc_category_singular_ro,
		null dsc_category_singular_ru, -- não existe de todo
		null dsc_category_singular_uk, -- não existe de todo
		null seek_name_ro,
		null seek_name_adding_ro,
		null private_name_ro,
		null private_name_adding_ro,
		null offer_name_ro,
		null offer_name_adding_ro,
		null name_ro,
		null dsc_category_long_ro,
		null dsc_category_long_ru, -- não existe de todo
		null dsc_category_long_uk, -- não existe de todo
		null filter_label_ro,
		null business_name_ro,
		null business_name_adding_ro,
		null address_label_ro,
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
		app_ads_fb,
		app_ads_admob,
		app_ads_admob_ad_campaign,
		app_ads_admob_ads_campaign,
		parent_level1,
		parent_level2,
		is_leaf flg_leaf
	from
		sandbox_andre_matias.stg_db_atlas_olxpt_categories
	--where 1 = 0
		) source, 
    sandbox_andre_matias.t_lkp_source_system lkp_source_system
	where source.opr_source_system = lkp_source_system.opr_source_system
    ) source_table,
    (select isnull(max(cod_category),0) max_cod from sandbox_andre_matias.t_lkp_category) max_cod_category,
    sandbox_andre_matias.t_lkp_category target
  where
    isnull(source_table.opr_category,-1) = target.opr_category(+)
	and source_table.cod_source_system = target.cod_source_system (+)
    and target.valid_to(+) = 20991231;

	$$$
	
delete from sandbox_andre_matias.t_lkp_category
using sandbox_andre_matias.tmp_load_category
where 
	tmp_load_category.dml_type = 'I' 
	and t_lkp_category.opr_category = tmp_load_category.opr_category
	and t_lkp_category.valid_from = (select dat_processing from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_category');

	$$$
	
update sandbox_andre_matias.t_lkp_category
set valid_to = (select rel_integr_proc.dat_processing from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_category') 
from sandbox_andre_matias.tmp_load_category source
where source.cod_category = sandbox_andre_matias.t_lkp_category.cod_category
and sandbox_andre_matias.t_lkp_category.valid_to = 20991231
and source.dml_type in('U','D');

	$$$
	
insert into sandbox_andre_matias.t_lkp_category
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
		(select rel_integr_proc.dat_processing from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_category') valid_from, 
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
		hash_category
    from
      sandbox_andre_matias.tmp_load_category
    where
      dml_type in ('U','I');

	$$$
	
update sandbox_andre_matias.t_lkp_category
set cod_category_parent = lkp.cod_category
from sandbox_andre_matias.tmp_load_category source, sandbox_andre_matias.t_lkp_category lkp, sandbox_andre_matias.t_lkp_source_system ss
where isnull(source.opr_category_parent,-1) = lkp.opr_category
and source.cod_source_system = ss.cod_source_system
and ss.cod_country = 1
and lkp.cod_category_parent = -2
and lkp.valid_to = 20991231 ;

	$$$
		  
-- #######################
-- ####    PASSO 5    ####
-- #######################
insert into sandbox_andre_matias.t_fac_scai_execution
  select
    max_cod_exec + 1 cod_execution,
    rel_integr_proc.cod_integration,
    rel_integr_proc.cod_process,
    1 cod_status,
    2 cod_execution_type, -- End
    rel_integr_proc.dat_processing,
    rel_integr_proc.execution_nbr,
    sysdate
  from
    sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr,
    (select isnull(max(cod_execution),0) max_cod_exec from sandbox_andre_matias.t_fac_scai_execution),
    sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc,
    sandbox_andre_matias.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 1 -- Portugal
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
update sandbox_andre_matias.t_rel_scai_integration_process
set cod_status = 1, -- Ok
last_processing_datetime = isnull((select max(operation_timestamp) from sandbox_andre_matias.tmp_load_category),last_processing_datetime)
/*from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc, sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_category'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 1
  ) source*/
from sandbox_andre_matias.t_lkp_scai_process proc 
where t_rel_scai_integration_process.cod_process = proc.cod_process
and t_rel_scai_integration_process.cod_status = 2
and t_rel_scai_integration_process.cod_country = 1
and proc.dsc_process_short = 't_lkp_category'
and t_rel_scai_integration_process.ind_active = 1
/*sandbox_andre_matias.t_rel_scai_integration_process.cod_process = source.cod_process
and sandbox_andre_matias.t_rel_scai_integration_process.cod_country = source.cod_country
and sandbox_andre_matias.t_rel_scai_integration_process.cod_integration = source.cod_integration*/;
	  
drop table if exists sandbox_andre_matias.tmp_load_category;

	$$$
	
/*
-- #############################################
-- # 		      ATLAS - POLAND               #
-- #		LOADING t_lkp_paidad_index  	   #
-- #############################################

--drop table sandbox_andre_matias.tmp_load_paidad_index;
--delete from sandbox_andre_matias.t_lkp_paidad_index;

create table sandbox_andre_matias.tmp_load_paidad_index as
select
    source_table.opr_paidad_index,
    source_table.dsc_paidad_index,
    source_table.dsc_paidad_index_pt,
    source_table.dsc_paidad_index_en,
    lkp_paidad_index_type.cod_paidad_index_type,
    lkp_source_system.cod_source_system,
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
    case
      when target.cod_paidad_index is null then 'I'
	  when source_table.operation_type = 'delete' then 'D'
      when source_table.hash_paidad_index != target.hash_paidad_index then 'U'
        else 'X'
    end dml_type
  from
    (
         select
        *,
        md5(isnull(dsc_paidad_index,'') + isnull(paidad_index_code,'') + isnull(opr_paidad_index_type,'') + isnull(name_pl,'') + isnull(name_en,'') + isnull(parameters,'') + isnull(duration,0) + isnull(display_order,0)
        + isnull(simple_user,0) + isnull(business_user,0) + isnull(fixed_price,0) + isnull(lead_pl,'') + isnull(lead_en,'') + isnull(fk_name,'') + isnull(fk_id,0)
        + isnull(user_specific,0) + isnull(simple_user_help,'') + isnull(opr_paidad_index_related,0) + isnull(name_pt,'') + isnull(invoiceable,0) + isnull(dsc_paidad_index_pt,'')
        + isnull(dsc_paidad_index_en,'') + isnull(business_promoter_help,'') + isnull(business_promoter,0) + isnull(business_manager_help,'') + isnull(business_manager,0)
        + isnull(business_developer_help,'') + isnull(business_developer,0) + isnull(business_consultant_help,'') + isnull(business_consultant,0)
        + isnull(business_agency_help,'') + isnull(business_agency,0) + isnull(lead_pt,'') + isnull(name_ro,'') + isnull(lead_ro,'') + isnull(name_ru,'')
        + isnull(name_uk,'') + isnull(display_default,0) + isnull(lead_hi,'') + isnull(name_hi,'') + isnull(bonus_credits,0) + cast(isnull(loadaccount,0) as varchar)
        + isnull(free_refresh,0) + isnull(free_refresh_frequency,0) + isnull(makes_account_premium,0) + isnull(recurrencies,0)
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
           cast(null as bigint)            recurrencies
        from
           sandbox_andre_matias.stg_db_atlas_verticals_paidads_indexes a,
           sandbox_andre_matias.t_lkp_source_system b
        where
           a.livesync_dbname = b.opr_source_system
           and b.cod_business_type = 1 -- Verticals
           and b.cod_country = 2 -- Poland
        union all
        select
           id opr_paidad_index,
           description dsc_paidad_index,
           'olxpl' opr_source_system,
		   operation_type,
		   operation_timestamp,
           code paidad_index_code,
          type opr_paidad_index_type,
           null name_pl,
           null name_en,
           name_pt,
           null name_ro,
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
           loadaccount,
           null free_refresh,
           null free_refresh_frequency,
           null makes_account_premium,
           recurrencies
        from
          sandbox_andre_matias.stg_db_atlas_olxpl_paidad_indexes
       )
    ) source_table,
    sandbox_andre_matias.t_lkp_source_system lkp_source_system,
    sandbox_andre_matias.t_lkp_paidad_index_type lkp_paidad_index_type,
    (select isnull(max(cod_paidad_index),0) max_cod from sandbox_andre_matias.t_lkp_paidad_index) max_cod_paidad_index,
    sandbox_andre_matias.t_lkp_paidad_index target
  where
    source_table.opr_source_system = lkp_source_system.opr_source_system
    and isnull(source_table.opr_paidad_index,-1) = target.opr_paidad_index(+)
    and target.valid_to(+) = 20991231
    and isnull(source_table.opr_paidad_index_type,'') = lkp_paidad_index_type.opr_paidad_index_type
	and lkp_source_system.cod_source_system = lkp_paidad_index_type.cod_source_system -- new
	and lkp_paidad_index_type.valid_to = 20991231;

update sandbox_andre_matias.t_lkp_paidad_index
set valid_to = 20180101 -- replace with variable 
from sandbox_andre_matias.tmp_load_paidad_index source
where source.cod_paidad_index = sandbox_andre_matias.t_lkp_paidad_index.cod_paidad_index
and sandbox_andre_matias.t_lkp_paidad_index.valid_to = 20991231
and source.dml_type in('U','D');

insert into sandbox_andre_matias.t_lkp_paidad_index
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
      20180101 valid_from, -- replace with variable 
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
      bonus_credits,
      loadaccount,
      free_refresh,
      free_refresh_frequency,
      makes_account_premium,
      recurrencies,
      cod_source_system,
      hash_paidad_index
    from
      sandbox_andre_matias.tmp_load_paidad_index
    where
      dml_type in ('U','I');

-- New -> Lookup to itself

update sandbox_andre_matias.t_lkp_paidad_index
set cod_paidad_index_related = lkp.cod_paidad_index
from sandbox_andre_matias.tmp_load_paidad_index source, sandbox_andre_matias.t_lkp_paidad_index lkp, sandbox_andre_matias.t_lkp_source_system ss
where isnull(source.opr_paidad_index_related,-1) = lkp.opr_paidad_index
and source.cod_source_system = ss.cod_source_system
and ss.cod_country = 2
and lkp.cod_paidad_index_related = -2
and lkp.valid_to = 20991231;

drop table sandbox_andre_matias.tmp_load_paidad_index;
*/

-- #######################
-- ####    PASSO 3    ####
-- #######################
update sandbox_andre_matias.t_rel_scai_integration_process
set dat_processing = source.dat_processing, execution_nbr = source.execution_nbr, cod_status = 2 -- Running
from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc, sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_paidad_index'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 1
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
  ) source
where sandbox_andre_matias.t_rel_scai_integration_process.cod_process = source.cod_process
and sandbox_andre_matias.t_rel_scai_integration_process.cod_country = source.cod_country
and sandbox_andre_matias.t_rel_scai_integration_process.cod_integration = source.cod_integration;


-- #######################
-- ####    PASSO 4    ####
-- #######################
insert into sandbox_andre_matias.t_fac_scai_execution
  select
    max_cod_exec + 1 cod_execution,
    rel_integr_proc.cod_integration,
    rel_integr_proc.cod_process,
    rel_integr_proc.cod_status,
    1 cod_execution_type, -- Begin
    rel_integr_proc.dat_processing,
    rel_integr_proc.execution_nbr,
    sysdate
  from
    sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr,
    (select isnull(max(cod_execution),0) max_cod_exec from sandbox_andre_matias.t_fac_scai_execution),
    sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc,
    sandbox_andre_matias.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 1 -- Portugal
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_lkp_paidad_index';	

	$$$
	
-- #############################################
-- # 		     ATLAS - PORTUGAL              #
-- #		LOADING t_lkp_paidad_index  	   #
-- #############################################

drop table if exists sandbox_andre_matias.tmp_load_paidad_index;

create table sandbox_andre_matias.tmp_load_paidad_index 
distkey(cod_source_system)
sortkey(cod_paidad_index, opr_paidad_index)
as
select
    source_table.opr_paidad_index,
    source_table.dsc_paidad_index,
    source_table.dsc_paidad_index_pt,
    source_table.dsc_paidad_index_en,
    lkp_paidad_index_type.cod_paidad_index_type,
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
    case
      --when target.cod_paidad_index is null then 'I'
	  when target.cod_paidad_index is null or (source_table.hash_paidad_index != target.hash_paidad_index and target.valid_from = (select dat_processing from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_paidad_index')) then 'I'
	  when source_table.operation_type = 'delete' then 'D'
      when source_table.hash_paidad_index != target.hash_paidad_index then 'U'
        else 'X'
    end dml_type
  from
    (
         select
        source.*,
		lkp_source_system.cod_source_system,
        md5(isnull(dsc_paidad_index,'') + isnull(paidad_index_code,'') + isnull(opr_paidad_index_type,'') + isnull(name_pl,'') + isnull(name_en,'') + isnull(parameters,'') + isnull(duration,0) + isnull(display_order,0)
        + isnull(simple_user,0) + isnull(business_user,0) + isnull(fixed_price,0) + isnull(lead_pl,'') + isnull(lead_en,'') + isnull(fk_name,'') + isnull(fk_id,0)
        + isnull(user_specific,0) + isnull(simple_user_help,'') + isnull(opr_paidad_index_related,0) + isnull(name_pt,'') + isnull(invoiceable,0) + isnull(dsc_paidad_index_pt,'')
        + isnull(dsc_paidad_index_en,'') + isnull(business_promoter_help,'') + isnull(business_promoter,0) + isnull(business_manager_help,'') + isnull(business_manager,0)
        + isnull(business_developer_help,'') + isnull(business_developer,0) + isnull(business_consultant_help,'') + isnull(business_consultant,0)
        + isnull(business_agency_help,'') + isnull(business_agency,0) + isnull(lead_pt,'') + isnull(name_ro,'') + isnull(lead_ro,'') + isnull(name_ru,'')
        + isnull(name_uk,'') + isnull(display_default,0) + isnull(lead_hi,'') + isnull(name_hi,'') + isnull(bonus_credits,0) + cast(isnull(loadaccount,0) as varchar)
        + isnull(free_refresh,0) + isnull(free_refresh_frequency,0) + isnull(makes_account_premium,0) + isnull(recurrencies,0)
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
           cast(null as bigint)            recurrencies
        from
           sandbox_andre_matias.stg_db_atlas_verticals_paidads_indexes a,
           sandbox_andre_matias.t_lkp_source_system b
        where
           a.livesync_dbname = b.opr_source_system
           and b.cod_business_type = 1 -- Verticals
           and b.cod_country = 1 -- Portugal
		   --and 1 = 0
        union all
        select
           id opr_paidad_index,
           description dsc_paidad_index,
           'olxpt' opr_source_system,
		   operation_type,
		   operation_timestamp,
           code paidad_index_code,
          type opr_paidad_index_type,
           null name_pl,
           null name_en,
           name_pt,
           null name_ro,
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
           loadaccount,
           null free_refresh,
           null free_refresh_frequency,
           null makes_account_premium,
           recurrencies
        from
          sandbox_andre_matias.stg_db_atlas_olxpt_paidads_indexes
		--where 1 = 0
       ) source,
    sandbox_andre_matias.t_lkp_source_system lkp_source_system
	where source.opr_source_system = lkp_source_system.opr_source_system
    ) source_table,
    sandbox_andre_matias.t_lkp_paidad_index_type lkp_paidad_index_type,
    (select isnull(max(cod_paidad_index),0) max_cod from sandbox_andre_matias.t_lkp_paidad_index) max_cod_paidad_index,
    sandbox_andre_matias.t_lkp_paidad_index target
  where
    isnull(source_table.opr_paidad_index,-1) = target.opr_paidad_index(+)
	and source_table.cod_source_system = target.cod_source_system (+)
    and target.valid_to(+) = 20991231
    and isnull(source_table.opr_paidad_index_type,'') = lkp_paidad_index_type.opr_paidad_index_type
	and source_table.cod_source_system = lkp_paidad_index_type.cod_source_system -- new
	and lkp_paidad_index_type.valid_to = 20991231;

	$$$
	
delete from sandbox_andre_matias.t_lkp_paidad_index
using sandbox_andre_matias.tmp_load_paidad_index
where 
	tmp_load_paidad_index.dml_type = 'I' 
	and t_lkp_paidad_index.opr_paidad_index = tmp_load_paidad_index.opr_paidad_index
	and t_lkp_paidad_index.valid_from = (select dat_processing from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_paidad_index');

	$$$
	
update sandbox_andre_matias.t_lkp_paidad_index
set valid_to = (select rel_integr_proc.dat_processing from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_paidad_index') 
from sandbox_andre_matias.tmp_load_paidad_index source
where source.cod_paidad_index = sandbox_andre_matias.t_lkp_paidad_index.cod_paidad_index
and sandbox_andre_matias.t_lkp_paidad_index.valid_to = 20991231
and source.dml_type in('U','D');

	$$$
	
insert into sandbox_andre_matias.t_lkp_paidad_index
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
      (select rel_integr_proc.dat_processing from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_paidad_index') valid_from, 
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
      hash_paidad_index
    from
      sandbox_andre_matias.tmp_load_paidad_index
    where
      dml_type in ('U','I');

-- New -> Lookup to itself

	$$$
	
update sandbox_andre_matias.t_lkp_paidad_index
set cod_paidad_index_related = lkp.cod_paidad_index
from sandbox_andre_matias.tmp_load_paidad_index source, sandbox_andre_matias.t_lkp_paidad_index lkp, sandbox_andre_matias.t_lkp_source_system ss
where isnull(source.opr_paidad_index_related,-1) = lkp.opr_paidad_index
and source.cod_source_system = ss.cod_source_system
and ss.cod_country = 1
and lkp.cod_paidad_index_related = -2
and lkp.valid_to = 20991231;

	$$$
	
-- #######################
-- ####    PASSO 5    ####
-- #######################
insert into sandbox_andre_matias.t_fac_scai_execution
  select
    max_cod_exec + 1 cod_execution,
    rel_integr_proc.cod_integration,
    rel_integr_proc.cod_process,
    1 cod_status,
    2 cod_execution_type, -- End
    rel_integr_proc.dat_processing,
    rel_integr_proc.execution_nbr,
    sysdate
  from
    sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr,
    (select isnull(max(cod_execution),0) max_cod_exec from sandbox_andre_matias.t_fac_scai_execution),
    sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc,
    sandbox_andre_matias.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 1 -- Portugal
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
update sandbox_andre_matias.t_rel_scai_integration_process
set cod_status = 1, -- Ok
last_processing_datetime = isnull((select max(operation_timestamp) from sandbox_andre_matias.tmp_load_paidad_index),last_processing_datetime)
/*from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc, sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_paidad_index'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 1
  ) source*/
from sandbox_andre_matias.t_lkp_scai_process proc 
where t_rel_scai_integration_process.cod_process = proc.cod_process
and t_rel_scai_integration_process.cod_status = 2
and t_rel_scai_integration_process.cod_country = 1
and proc.dsc_process_short = 't_lkp_paidad_index'
and t_rel_scai_integration_process.ind_active = 1
/*sandbox_andre_matias.t_rel_scai_integration_process.cod_process = source.cod_process
and sandbox_andre_matias.t_rel_scai_integration_process.cod_country = source.cod_country
and sandbox_andre_matias.t_rel_scai_integration_process.cod_integration = source.cod_integration*/;

drop table if exists sandbox_andre_matias.tmp_load_paidad_index;

	$$$
	
/*
-- #############################################
-- # 		      ATLAS - ROMANIA              #
-- #		LOADING t_lkp_paidad_index  	   #
-- #############################################

--drop table sandbox_andre_matias.tmp_load_paidad_index;
--delete from sandbox_andre_matias.t_lkp_paidad_index;

create table sandbox_andre_matias.tmp_load_paidad_index as
select
    source_table.opr_paidad_index,
    source_table.dsc_paidad_index,
    source_table.dsc_paidad_index_pt,
    source_table.dsc_paidad_index_en,
    lkp_paidad_index_type.cod_paidad_index_type,
    lkp_source_system.cod_source_system,
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
    case
      when target.cod_paidad_index is null then 'I'
	  when source_table.operation_type = 'delete' then 'D'
      when source_table.hash_paidad_index != target.hash_paidad_index then 'U'
        else 'X'
    end dml_type
  from
    (
         select
        *,
        md5(isnull(dsc_paidad_index,'') + isnull(paidad_index_code,'') + isnull(opr_paidad_index_type,'') + isnull(name_pl,'') + isnull(name_en,'') + isnull(parameters,'') + isnull(duration,0) + isnull(display_order,0)
        + isnull(simple_user,0) + isnull(business_user,0) + isnull(fixed_price,0) + isnull(lead_pl,'') + isnull(lead_en,'') + isnull(fk_name,'') + isnull(fk_id,0)
        + isnull(user_specific,0) + isnull(simple_user_help,'') + isnull(opr_paidad_index_related,0) + isnull(name_pt,'') + isnull(invoiceable,0) + isnull(dsc_paidad_index_pt,'')
        + isnull(dsc_paidad_index_en,'') + isnull(business_promoter_help,'') + isnull(business_promoter,0) + isnull(business_manager_help,'') + isnull(business_manager,0)
        + isnull(business_developer_help,'') + isnull(business_developer,0) + isnull(business_consultant_help,'') + isnull(business_consultant,0)
        + isnull(business_agency_help,'') + isnull(business_agency,0) + isnull(lead_pt,'') + isnull(name_ro,'') + isnull(lead_ro,'') + isnull(name_ru,'')
        + isnull(name_uk,'') + isnull(display_default,0) + isnull(lead_hi,'') + isnull(name_hi,'') + isnull(bonus_credits,0) + cast(isnull(loadaccount,0) as varchar)
        + isnull(free_refresh,0) + isnull(free_refresh_frequency,0) + isnull(makes_account_premium,0) + isnull(recurrencies,0)
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
           cast(null as bigint)            recurrencies
        from
           sandbox_andre_matias.stg_db_atlas_verticals_paidads_indexes a,
           sandbox_andre_matias.t_lkp_source_system b
        where
           a.livesync_dbname = b.opr_source_system
           and b.cod_business_type = 1 -- Verticals
           and b.cod_country = 4 -- Romania
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
           name_pt,
           null name_ro,
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
           loadaccount,
           null free_refresh,
           null free_refresh_frequency,
           null makes_account_premium,
           recurrencies
        from
          sandbox_andre_matias.stg_db_atlas_olxro_paidad_indexes
       )
    ) source_table,
    sandbox_andre_matias.t_lkp_source_system lkp_source_system,
    sandbox_andre_matias.t_lkp_paidad_index_type lkp_paidad_index_type,
    (select isnull(max(cod_paidad_index),0) max_cod from sandbox_andre_matias.t_lkp_paidad_index) max_cod_paidad_index,
    sandbox_andre_matias.t_lkp_paidad_index target
  where
    source_table.opr_source_system = lkp_source_system.opr_source_system
    and isnull(source_table.opr_paidad_index,-1) = target.opr_paidad_index(+)
    and target.valid_to(+) = 20991231
    and isnull(source_table.opr_paidad_index_type,'') = lkp_paidad_index_type.opr_paidad_index_type
	and lkp_source_system.cod_source_system = lkp_paidad_index_type.cod_source_system -- new
	and lkp_paidad_index_type.valid_to = 20991231;

update sandbox_andre_matias.t_lkp_paidad_index
set valid_to = 20180101 -- replace with variable 
from sandbox_andre_matias.tmp_load_paidad_index source
where source.cod_paidad_index = sandbox_andre_matias.t_lkp_paidad_index.cod_paidad_index
and sandbox_andre_matias.t_lkp_paidad_index.valid_to = 20991231
and source.dml_type in('U','D');

insert into sandbox_andre_matias.t_lkp_paidad_index
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
      20180101 valid_from, -- replace with variable 
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
      bonus_credits,
      loadaccount,
      free_refresh,
      free_refresh_frequency,
      makes_account_premium,
      recurrencies,
      cod_source_system,
      hash_paidad_index
    from
      sandbox_andre_matias.tmp_load_paidad_index
    where
      dml_type in ('U','I');

-- New -> Lookup to itself

update sandbox_andre_matias.t_lkp_paidad_index
set cod_paidad_index_related = lkp.cod_paidad_index
from sandbox_andre_matias.tmp_load_paidad_index source, sandbox_andre_matias.t_lkp_paidad_index lkp, sandbox_andre_matias.t_lkp_source_system ss
where isnull(source.opr_paidad_index_related,-1) = lkp.opr_paidad_index
and source.cod_source_system = ss.cod_source_system
and ss.cod_country = 4
and lkp.cod_paidad_index_related = -2
and lkp.valid_to = 20991231;

drop table sandbox_andre_matias.tmp_load_paidad_index;


-- #############################################
-- # 		     ATLAS - UKRAINE               #
-- #		LOADING t_lkp_paidad_index  	   #
-- #############################################

--drop table sandbox_andre_matias.tmp_load_paidad_index;
--delete from sandbox_andre_matias.t_lkp_paidad_index;

create table sandbox_andre_matias.tmp_load_paidad_index as
select
    source_table.opr_paidad_index,
    source_table.dsc_paidad_index,
    source_table.dsc_paidad_index_pt,
    source_table.dsc_paidad_index_en,
    lkp_paidad_index_type.cod_paidad_index_type,
    lkp_source_system.cod_source_system,
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
    case
      when target.cod_paidad_index is null then 'I'
	  when source_table.operation_type = 'delete' then 'D'
      when source_table.hash_paidad_index != target.hash_paidad_index then 'U'
        else 'X'
    end dml_type
  from
    (
         select
        *,
        md5(isnull(dsc_paidad_index,'') + isnull(paidad_index_code,'') + isnull(opr_paidad_index_type,'') + isnull(name_pl,'') + isnull(name_en,'') + isnull(parameters,'') + isnull(duration,0) + isnull(display_order,0)
        + isnull(simple_user,0) + isnull(business_user,0) + isnull(fixed_price,0) + isnull(lead_pl,'') + isnull(lead_en,'') + isnull(fk_name,'') + isnull(fk_id,0)
        + isnull(user_specific,0) + isnull(simple_user_help,'') + isnull(opr_paidad_index_related,0) + isnull(name_pt,'') + isnull(invoiceable,0) + isnull(dsc_paidad_index_pt,'')
        + isnull(dsc_paidad_index_en,'') + isnull(business_promoter_help,'') + isnull(business_promoter,0) + isnull(business_manager_help,'') + isnull(business_manager,0)
        + isnull(business_developer_help,'') + isnull(business_developer,0) + isnull(business_consultant_help,'') + isnull(business_consultant,0)
        + isnull(business_agency_help,'') + isnull(business_agency,0) + isnull(lead_pt,'') + isnull(name_ro,'') + isnull(lead_ro,'') + isnull(name_ru,'')
        + isnull(name_uk,'') + isnull(display_default,0) + isnull(lead_hi,'') + isnull(name_hi,'') + isnull(bonus_credits,0) + cast(isnull(loadaccount,0) as varchar)
        + isnull(free_refresh,0) + isnull(free_refresh_frequency,0) + isnull(makes_account_premium,0) + isnull(recurrencies,0)
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
           cast(null as bigint)            recurrencies
        from
           sandbox_andre_matias.stg_db_atlas_verticals_paidads_indexes a,
           sandbox_andre_matias.t_lkp_source_system b
        where
           a.livesync_dbname = b.opr_source_system
           and b.cod_business_type = 1 -- Verticals
           and b.cod_country = 3 -- Ukraine
        union all
        select
           id opr_paidad_index,
           description dsc_paidad_index,
           'olxua' opr_source_system,
		   operation_type,
		   operation_timestamp,
           code paidad_index_code,
          type opr_paidad_index_type,
           null name_pl,
           null name_en,
           name_pt,
           null name_ro,
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
           loadaccount,
           null free_refresh,
           null free_refresh_frequency,
           null makes_account_premium,
           recurrencies
        from
          sandbox_andre_matias.stg_db_atlas_olxua_paidad_indexes
       )
    ) source_table,
    sandbox_andre_matias.t_lkp_source_system lkp_source_system,
    sandbox_andre_matias.t_lkp_paidad_index_type lkp_paidad_index_type,
    (select isnull(max(cod_paidad_index),0) max_cod from sandbox_andre_matias.t_lkp_paidad_index) max_cod_paidad_index,
    sandbox_andre_matias.t_lkp_paidad_index target
  where
    source_table.opr_source_system = lkp_source_system.opr_source_system
    and isnull(source_table.opr_paidad_index,-1) = target.opr_paidad_index(+)
    and target.valid_to(+) = 20991231
    and isnull(source_table.opr_paidad_index_type,'') = lkp_paidad_index_type.opr_paidad_index_type
	and lkp_source_system.cod_source_system = lkp_paidad_index_type.cod_source_system -- new
	and lkp_paidad_index_type.valid_to = 20991231;

update sandbox_andre_matias.t_lkp_paidad_index
set valid_to = 20180101 -- replace with variable 
from sandbox_andre_matias.tmp_load_paidad_index source
where source.cod_paidad_index = sandbox_andre_matias.t_lkp_paidad_index.cod_paidad_index
and sandbox_andre_matias.t_lkp_paidad_index.valid_to = 20991231
and source.dml_type in('U','D');

insert into sandbox_andre_matias.t_lkp_paidad_index
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
      20180101 valid_from, -- replace with variable 
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
      bonus_credits,
      loadaccount,
      free_refresh,
      free_refresh_frequency,
      makes_account_premium,
      recurrencies,
      cod_source_system,
      hash_paidad_index
    from
      sandbox_andre_matias.tmp_load_paidad_index
    where
      dml_type in ('U','I');

-- New -> Lookup to itself

update sandbox_andre_matias.t_lkp_paidad_index
set cod_paidad_index_related = lkp.cod_paidad_index
from sandbox_andre_matias.tmp_load_paidad_index source, sandbox_andre_matias.t_lkp_paidad_index lkp, sandbox_andre_matias.t_lkp_source_system ss
where isnull(source.opr_paidad_index_related,-1) = lkp.opr_paidad_index
and source.cod_source_system = ss.cod_source_system
and ss.cod_country = 3
and lkp.cod_paidad_index_related = -2
and lkp.valid_to = 20991231;

drop table sandbox_andre_matias.tmp_load_paidad_index;
*/

-- #######################
-- ####    PASSO 3    ####
-- #######################
update sandbox_andre_matias.t_rel_scai_integration_process
set dat_processing = source.dat_processing, execution_nbr = source.execution_nbr, cod_status = 2 -- Running
from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc, sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_region'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 1
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
  ) source
where sandbox_andre_matias.t_rel_scai_integration_process.cod_process = source.cod_process
and sandbox_andre_matias.t_rel_scai_integration_process.cod_country = source.cod_country
and sandbox_andre_matias.t_rel_scai_integration_process.cod_integration = source.cod_integration;


-- #######################
-- ####    PASSO 4    ####
-- #######################
insert into sandbox_andre_matias.t_fac_scai_execution
  select
    max_cod_exec + 1 cod_execution,
    rel_integr_proc.cod_integration,
    rel_integr_proc.cod_process,
    rel_integr_proc.cod_status,
    1 cod_execution_type, -- Begin
    rel_integr_proc.dat_processing,
    rel_integr_proc.execution_nbr,
    sysdate
  from
    sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr,
    (select isnull(max(cod_execution),0) max_cod_exec from sandbox_andre_matias.t_fac_scai_execution),
    sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc,
    sandbox_andre_matias.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 1 -- Portugal
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_lkp_region';	

	$$$
	
-- #############################################
-- # 		        ATLAS - GERAL              #
-- #	       LOADING t_lkp_region     	   #
-- #############################################

drop table if exists sandbox_andre_matias.tmp_load_region;

create table sandbox_andre_matias.tmp_load_region 
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
    max_cod_region.max_cod,
    row_number() over (order by source_table.opr_region desc) new_cod,
    target.cod_region,
    case
      --when target.cod_region is null then 'I'
      when target.cod_region is null or (source_table.hash_region != target.hash_region and target.valid_from = (select dat_processing from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_region')) then 'I'
	  when source_table.operation_type = 'delete' then 'D'
      when source_table.hash_region != target.hash_region then 'U'
        else 'X'
    end dml_type
  from
    (
	select 
		source.*,
		lkp_source_system.cod_source_system,
		md5(isnull(dsc_region_pt,'') + isnull(dsc_region_en,'') + isnull(dsc_region_pl,'') + isnull(dsc_region_ro,'') + isnull(dsc_region_ru,'') + isnull(dsc_region_hi,'')
                  + isnull(code,'') + isnull(domain,'')
                  + cast(isnull(lon,0) as varchar) + cast(isnull(lat,0) as varchar) + isnull(seo_weight,0) + isnull(zoom,0) + isnull(locative_pt,'') + isnull(locative_en,'')
                  + isnull(locative_pl,'') + isnull(locative_ro,'') + isnull(locative_ru,'') + isnull(locative_hi,'') + isnull(possessive_pt,'') + isnull(possessive_en,'')
                  + isnull(possessive_pl,'') + isnull(possessive_ro,'') + isnull(possessive_ru,'') + isnull(possessive_hi,'') + isnull(search_combo_label_pt,'')
                  + isnull(search_combo_label_en,'') + isnull(search_combo_label_pl,'') + isnull(search_combo_label_ro,'') + isnull(search_combo_label_ru,'')
                  + isnull(search_combo_label_hi,'') + isnull(aliases_pt,'') + isnull(aliases_en,'') + isnull(aliases_pl,'') + isnull(aliases_ro,'') + isnull(aliases_ru,'')
                  + isnull(aliases_hi,'') + isnull(country_id,0)
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
        country_id
      FROM
        sandbox_andre_matias.stg_db_atlas_verticals_regions a,
           sandbox_andre_matias.t_lkp_source_system b
        where
           a.livesync_dbname = b.opr_source_system
           and b.cod_business_type = 1 -- Verticals
           and b.cod_country = 1 -- Portugal
		   --and 1 = 0
	  union all
	  select
		id opr_region,
        name_pt dsc_region_pt,
        null dsc_region_en,
        null dsc_region_pl,
        null dsc_region_ro,
        null dsc_region_ru,
        null dsc_region_hi,
        'olxpt' opr_source_system,
		operation_timestamp,
		operation_type,
        code,
        domain,
        lon,
        lat,
        seo_weight,
        zoom,
        locative_pt,
        null locative_en,
        null locative_pl,
        null locative_ro,
        null locative_ru,
        null locative_hi,
        possessive_pt,
        null possessive_en,
        null possessive_pl,
        null possessive_ro,
        null possessive_ru,
        null possessive_hi,
        search_combo_label_pt,
        null search_combo_label_en,
        null search_combo_label_pl,
        null search_combo_label_ro,
        null search_combo_label_ru,
        null search_combo_label_hi,
        aliases_pt,
        null aliases_en,
        null aliases_pl,
        null aliases_ro,
        null aliases_ru,
        null aliases_hi,
        null country_id
	  from
		sandbox_andre_matias.stg_db_atlas_olxpt_regions
	  --where 1 = 0
	) source,
    sandbox_andre_matias.t_lkp_source_system lkp_source_system
	where source.opr_source_system = lkp_source_system.opr_source_system
    ) source_table,
    (select isnull(max(cod_region),0) max_cod from sandbox_andre_matias.t_lkp_region) max_cod_region,
    sandbox_andre_matias.t_lkp_region target
  where
    isnull(source_table.opr_region,-1) = target.opr_region(+)
	and source_table.cod_source_system = target.cod_source_system (+)
    and target.valid_to(+) = 20991231;

	$$$
	
delete from sandbox_andre_matias.t_lkp_region
using sandbox_andre_matias.tmp_load_region
where 
	tmp_load_region.dml_type = 'I' 
	and t_lkp_region.opr_region = tmp_load_region.opr_region
	and t_lkp_region.valid_from = (select dat_processing from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_region');

	$$$
	
update sandbox_andre_matias.t_lkp_region
set valid_to = (select rel_integr_proc.dat_processing from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_region') 
from sandbox_andre_matias.tmp_load_region source
where source.cod_region = sandbox_andre_matias.t_lkp_region.cod_region
and sandbox_andre_matias.t_lkp_region.valid_to = 20991231
and source.dml_type = 'U';

	$$$
	
insert into sandbox_andre_matias.t_lkp_region
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
      (select rel_integr_proc.dat_processing from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_region') valid_from, 
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
      hash_region
    from
      sandbox_andre_matias.tmp_load_region
    where
      dml_type in ('U','I');

	$$$
	
-- #######################
-- ####    PASSO 5    ####
-- #######################
insert into sandbox_andre_matias.t_fac_scai_execution
  select
    max_cod_exec + 1 cod_execution,
    rel_integr_proc.cod_integration,
    rel_integr_proc.cod_process,
    1 cod_status,
    2 cod_execution_type, -- End
    rel_integr_proc.dat_processing,
    rel_integr_proc.execution_nbr,
    sysdate
  from
    sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr,
    (select isnull(max(cod_execution),0) max_cod_exec from sandbox_andre_matias.t_fac_scai_execution),
    sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc,
    sandbox_andre_matias.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 1 -- Portugal
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
update sandbox_andre_matias.t_rel_scai_integration_process
set cod_status = 1, -- Ok
last_processing_datetime = isnull((select max(operation_timestamp) from sandbox_andre_matias.tmp_load_region),last_processing_datetime)
/*from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc, sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_region'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 1
  ) source*/
from sandbox_andre_matias.t_lkp_scai_process proc 
where t_rel_scai_integration_process.cod_process = proc.cod_process
and t_rel_scai_integration_process.cod_status = 2
and t_rel_scai_integration_process.cod_country = 1
and proc.dsc_process_short = 't_lkp_region'
and t_rel_scai_integration_process.ind_active = 1
/*sandbox_andre_matias.t_rel_scai_integration_process.cod_process = source.cod_process
and sandbox_andre_matias.t_rel_scai_integration_process.cod_country = source.cod_country
and sandbox_andre_matias.t_rel_scai_integration_process.cod_integration = source.cod_integration*/;

drop table if exists sandbox_andre_matias.tmp_load_region;

	$$$
	
-- #######################
-- ####    PASSO 3    ####
-- #######################
update sandbox_andre_matias.t_rel_scai_integration_process
set dat_processing = source.dat_processing, execution_nbr = source.execution_nbr, cod_status = 2 -- Running
from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc, sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_subregion'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 1
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
  ) source
where sandbox_andre_matias.t_rel_scai_integration_process.cod_process = source.cod_process
and sandbox_andre_matias.t_rel_scai_integration_process.cod_country = source.cod_country
and sandbox_andre_matias.t_rel_scai_integration_process.cod_integration = source.cod_integration;


-- #######################
-- ####    PASSO 4    ####
-- #######################
insert into sandbox_andre_matias.t_fac_scai_execution
  select
    max_cod_exec + 1 cod_execution,
    rel_integr_proc.cod_integration,
    rel_integr_proc.cod_process,
    rel_integr_proc.cod_status,
    1 cod_execution_type, -- Begin
    rel_integr_proc.dat_processing,
    rel_integr_proc.execution_nbr,
    sysdate
  from
    sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr,
    (select isnull(max(cod_execution),0) max_cod_exec from sandbox_andre_matias.t_fac_scai_execution),
    sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc,
    sandbox_andre_matias.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 1 -- Portugal
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_lkp_subregion';	

	$$$
	
-- #############################################
-- # 		        ATLAS - GERAL              #
-- #	      LOADING t_lkp_subregion     	   #
-- #############################################

drop table if exists sandbox_andre_matias.tmp_load_subregion;

create table sandbox_andre_matias.tmp_load_subregion 
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
    lkp_region.cod_region,
    source_table.cod_source_system,
    max_cod_subregion.max_cod,
    row_number() over (order by source_table.opr_subregion desc) new_cod,
    target.cod_subregion,
    case
      --when target.cod_subregion is null then 'I'
      when target.cod_subregion is null or (source_table.hash_subregion != target.hash_subregion and target.valid_from = (select dat_processing from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_subregion')) then 'I'
	  when source_table.operation_type = 'delete' then 'D'
      when source_table.hash_subregion != target.hash_subregion then 'U'
        else 'X'
    end dml_type
  from
    (
	select
		*,
		md5(isnull(dsc_subregion_pt,'') + isnull(dsc_subregion_en,'') + isnull(dsc_subregion_pl,'') + isnull(dsc_subregion_ro,'') + isnull(dsc_subregion_ru,'') + isnull(dsc_subregion_hi,'') + isnull(dsc_subregion_uk,'')
            + isnull(code,'') + isnull(opr_region,0) + isnull(dsc_subregion_normalized_pt,'') + isnull(dsc_subregion_normalized_en,'') + isnull(dsc_subregion_normalized_pl,'')
            + isnull(dsc_subregion_normalized_ru,'') + isnull(dsc_subregion_normalized_uk,'') + cast(isnull(lon,0) as varchar) + cast(isnull(lat,0) as varchar) + isnull(seo_weight,0)
            + isnull(zoom,0) + isnull(locative_pt,'') + isnull(locative_en,'') + isnull(locative_pl,'') + isnull(locative_ro,'') + isnull(locative_ru,'')
            + isnull(locative_hi,'') + isnull(locative_hi,'') + isnull(locative_uk,'') + isnull(url_code,'') + isnull(possessive_pt,'') + isnull(possessive_en,'')
            + isnull(possessive_pl,'') + isnull(possessive_ro,'') + isnull(possessive_ru,'') + isnull(possessive_hi,'') + isnull(possessive_uk,'') + isnull(price_group,'')
           + isnull(display_order,0) + isnull(flg_urban,0) + isnull(external_id,'')) hash_subregion
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
        cod_source_system
      FROM
        sandbox_andre_matias.stg_db_atlas_verticals_subregions a,
        sandbox_andre_matias.t_lkp_source_system b
      where
		a.livesync_dbname = b.opr_source_system
		and b.cod_business_type = 1 -- Verticals
		and b.cod_country = 1 -- Portugal
		--and 1 = 0
	)
    ) source_table,
    sandbox_andre_matias.t_lkp_region lkp_region,
    (select isnull(max(cod_subregion),0) max_cod from sandbox_andre_matias.t_lkp_subregion) max_cod_subregion,
    sandbox_andre_matias.t_lkp_subregion target
  where
    isnull(source_table.opr_subregion,-1) = target.opr_subregion(+)
	and source_table.cod_source_system = target.cod_source_system (+)
    and target.valid_to(+) = 20991231
    and isnull(source_table.opr_region,-1) = lkp_region.opr_region
	and source_table.cod_source_system = lkp_region.cod_source_system -- new
	and lkp_region.valid_to = 20991231;

	$$$
	
delete from sandbox_andre_matias.t_lkp_subregion
using sandbox_andre_matias.tmp_load_subregion
where 
	tmp_load_subregion.dml_type = 'I' 
	and t_lkp_subregion.opr_subregion = tmp_load_subregion.opr_subregion
	and t_lkp_subregion.valid_from = (select dat_processing from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_subregion');

	$$$
	
update sandbox_andre_matias.t_lkp_subregion
set valid_to = (select rel_integr_proc.dat_processing from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_subregion') 
from sandbox_andre_matias.tmp_load_subregion source
where source.cod_subregion = sandbox_andre_matias.t_lkp_subregion.cod_subregion
and sandbox_andre_matias.t_lkp_subregion.valid_to = 20991231
and source.dml_type = 'U';

	$$$
	
insert into sandbox_andre_matias.t_lkp_subregion
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
      (select rel_integr_proc.dat_processing from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_subregion') valid_from, 
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
      hash_subregion
    from
      sandbox_andre_matias.tmp_load_subregion
    where
      dml_type in ('U','I');

	$$$
	
-- #######################
-- ####    PASSO 5    ####
-- #######################
insert into sandbox_andre_matias.t_fac_scai_execution
  select
    max_cod_exec + 1 cod_execution,
    rel_integr_proc.cod_integration,
    rel_integr_proc.cod_process,
    1 cod_status,
    2 cod_execution_type, -- End
    rel_integr_proc.dat_processing,
    rel_integr_proc.execution_nbr,
    sysdate
  from
    sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr,
    (select isnull(max(cod_execution),0) max_cod_exec from sandbox_andre_matias.t_fac_scai_execution),
    sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc,
    sandbox_andre_matias.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 1 -- Portugal
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
update sandbox_andre_matias.t_rel_scai_integration_process
set cod_status = 1, -- Ok
last_processing_datetime = isnull((select max(operation_timestamp) from sandbox_andre_matias.tmp_load_subregion),last_processing_datetime)
/*from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc, sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_subregion'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 1
  ) source*/
from sandbox_andre_matias.t_lkp_scai_process proc 
where t_rel_scai_integration_process.cod_process = proc.cod_process
and t_rel_scai_integration_process.cod_status = 2
and t_rel_scai_integration_process.cod_country = 1
and proc.dsc_process_short = 't_lkp_subregion'
and t_rel_scai_integration_process.ind_active = 1
/*sandbox_andre_matias.t_rel_scai_integration_process.cod_process = source.cod_process
and sandbox_andre_matias.t_rel_scai_integration_process.cod_country = source.cod_country
and sandbox_andre_matias.t_rel_scai_integration_process.cod_integration = source.cod_integration*/;

drop table if exists sandbox_andre_matias.tmp_load_subregion;

	$$$
	
-- #######################
-- ####    PASSO 3    ####
-- #######################
update sandbox_andre_matias.t_rel_scai_integration_process
set dat_processing = source.dat_processing, execution_nbr = source.execution_nbr, cod_status = 2 -- Running
from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc, sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_city'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 1
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
  ) source
where sandbox_andre_matias.t_rel_scai_integration_process.cod_process = source.cod_process
and sandbox_andre_matias.t_rel_scai_integration_process.cod_country = source.cod_country
and sandbox_andre_matias.t_rel_scai_integration_process.cod_integration = source.cod_integration;


-- #######################
-- ####    PASSO 4    ####
-- #######################
insert into sandbox_andre_matias.t_fac_scai_execution
  select
    max_cod_exec + 1 cod_execution,
    rel_integr_proc.cod_integration,
    rel_integr_proc.cod_process,
    rel_integr_proc.cod_status,
    1 cod_execution_type, -- Begin
    rel_integr_proc.dat_processing,
    rel_integr_proc.execution_nbr,
    sysdate
  from
    sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr,
    (select isnull(max(cod_execution),0) max_cod_exec from sandbox_andre_matias.t_fac_scai_execution),
    sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc,
    sandbox_andre_matias.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 1 -- Portugal
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_lkp_city';	

	$$$
	
-- #############################################
-- # 		        ATLAS - GERAL              #
-- #	        LOADING t_lkp_city       	   #
-- #############################################

drop table if exists sandbox_andre_matias.tmp_load_city;

create table sandbox_andre_matias.tmp_load_city 
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
    max_cod_city.max_cod,
    row_number() over (order by source_table.opr_city desc) new_cod,
    target.cod_city,
    case
      --when target.cod_city is null then 'I'
      when target.cod_city is null or (source_table.hash_city != target.hash_city and target.valid_from = (select dat_processing from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_city')) then 'I'
	  when source_table.operation_type = 'delete' then 'D'
      when source_table.hash_city != target.hash_city then 'U'
        else 'X'
    end dml_type
  from
    (
	select 
		*,
		md5(
		isnull(dsc_city_pl,'') +
		isnull(dsc_city_en,'') +
		isnull(url,'') +
		isnull(county,'') +
		isnull(municipality,'') +
		isnull(flg_unique,0) +
		isnull(zip,'') +
		isnull(city_id,0) +
		cast(isnull(lat,0) as varchar) +
		cast(isnull(lon,0) as varchar) +
		isnull(zoom,0) +
		isnull(citizens_count,0) +
		isnull(citizens_weight,0) +
		isnull(opr_region,0) +
		isnull(opr_subregion,0) +
		isnull(flg_main,0) +
		isnull(flg_import_approximation,0) +
		isnull(flg_show_on_mainpage,0) +
		cast(isnull(radius,0) as varchar) +
		isnull(polygon,'') +
		isnull(group_id,0) +
		isnull(external_id,'') +
		isnull(external_type,'') +
		isnull(dsc_city_normalized_pl,'')
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
		b.cod_source_system
      FROM
        sandbox_andre_matias.stg_db_atlas_verticals_cities a,
        sandbox_andre_matias.t_lkp_source_system b,
		sandbox_andre_matias.t_lkp_subregion c,
		sandbox_andre_matias.t_lkp_region d
        where
           a.livesync_dbname = b.opr_source_system
		   and a.region_id = d.opr_region
		   and d.cod_source_system = b.cod_source_system
		   and a.subregion_id = c.opr_subregion
		   and c.cod_source_system = b.cod_source_system
           and b.cod_business_type = 1 -- Verticals
           and b.cod_country = 1 -- Portugal
		   --and 1 = 0
	  union all
	  select
	    a.id opr_city,
		'olxpt' opr_source_system,
		a.operation_timestamp,
		a.operation_type,
		null dsc_city_pl,
		a.name_pt dsc_city_en,
		a.url,
		a.county,
		a.municipality_pt municipality,
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
		b.cod_source_system
      FROM
        sandbox_andre_matias.stg_db_atlas_olxpt_cities a,
        sandbox_andre_matias.t_lkp_source_system b,
	sandbox_andre_matias.t_lkp_region c
	where 
		'olxpt' = b.opr_source_system
		and a.region_id = c.opr_region
		and c.cod_source_system = b.cod_source_system
		--and 1 = 0
    )
	) source_table,
    (select isnull(max(cod_city),0) max_cod from sandbox_andre_matias.t_lkp_city) max_cod_city,
    sandbox_andre_matias.t_lkp_city target
  where
    isnull(source_table.opr_city,-1) = target.opr_city(+)
	and source_table.cod_source_system = target.cod_source_system (+)
    and target.valid_to(+) = 20991231;

	$$$
	
delete from sandbox_andre_matias.t_lkp_city
using sandbox_andre_matias.tmp_load_city
where 
	tmp_load_city.dml_type = 'I' 
	and t_lkp_city.opr_city = tmp_load_city.opr_city
	and t_lkp_city.valid_from = (select dat_processing from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_city');

	$$$
	
update sandbox_andre_matias.t_lkp_city
set valid_to = (select rel_integr_proc.dat_processing from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_city') 
from sandbox_andre_matias.tmp_load_city source
where source.cod_city = sandbox_andre_matias.t_lkp_city.cod_city
and sandbox_andre_matias.t_lkp_city.valid_to = 20991231
and source.dml_type = 'U';

	$$$
	
insert into sandbox_andre_matias.t_lkp_city
    select
		case
			when dml_type = 'I' then max_cod + new_cod
			when dml_type = 'U' then cod_city
		end cod_city,
		opr_city,
		dsc_city_en,
		dsc_city_pl,
		(select rel_integr_proc.dat_processing from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_city') valid_from, 
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
		hash_city
    from
      sandbox_andre_matias.tmp_load_city
    where
      dml_type in ('U','I');

	$$$
	
-- #######################
-- ####    PASSO 5    ####
-- #######################
insert into sandbox_andre_matias.t_fac_scai_execution
  select
    max_cod_exec + 1 cod_execution,
    rel_integr_proc.cod_integration,
    rel_integr_proc.cod_process,
    1 cod_status,
    2 cod_execution_type, -- End
    rel_integr_proc.dat_processing,
    rel_integr_proc.execution_nbr,
    sysdate
  from
    sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr,
    (select isnull(max(cod_execution),0) max_cod_exec from sandbox_andre_matias.t_fac_scai_execution),
    sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc,
    sandbox_andre_matias.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 1 -- Portugal
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
update sandbox_andre_matias.t_rel_scai_integration_process
set cod_status = 1, -- Ok
last_processing_datetime = isnull((select max(operation_timestamp) from sandbox_andre_matias.tmp_load_city),last_processing_datetime)
/*from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc, sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_city'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 1
  ) source*/
from sandbox_andre_matias.t_lkp_scai_process proc 
where t_rel_scai_integration_process.cod_process = proc.cod_process
and t_rel_scai_integration_process.cod_status = 2
and t_rel_scai_integration_process.cod_country = 1
and proc.dsc_process_short = 't_lkp_city'
and t_rel_scai_integration_process.ind_active = 1
/*sandbox_andre_matias.t_rel_scai_integration_process.cod_process = source.cod_process
and sandbox_andre_matias.t_rel_scai_integration_process.cod_country = source.cod_country
and sandbox_andre_matias.t_rel_scai_integration_process.cod_integration = source.cod_integration*/;

drop table if exists sandbox_andre_matias.tmp_load_city;

	$$$
	
-- #######################
-- ####    PASSO 3    ####
-- #######################
update sandbox_andre_matias.t_rel_scai_integration_process
set dat_processing = source.dat_processing, execution_nbr = source.execution_nbr, cod_status = 2 -- Running
from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc, sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_atlas_user'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 1
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
  ) source
where sandbox_andre_matias.t_rel_scai_integration_process.cod_process = source.cod_process
and sandbox_andre_matias.t_rel_scai_integration_process.cod_country = source.cod_country
and sandbox_andre_matias.t_rel_scai_integration_process.cod_integration = source.cod_integration;

-- #######################
-- ####    PASSO 4    ####
-- #######################
insert into sandbox_andre_matias.t_fac_scai_execution
  select
    max_cod_exec + 1 cod_execution,
    rel_integr_proc.cod_integration,
    rel_integr_proc.cod_process,
    rel_integr_proc.cod_status,
    1 cod_execution_type, -- Begin
    rel_integr_proc.dat_processing,
    rel_integr_proc.execution_nbr,
    sysdate
  from
    sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr,
    (select isnull(max(cod_execution),0) max_cod_exec from sandbox_andre_matias.t_fac_scai_execution),
    sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc,
    sandbox_andre_matias.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 1 -- Portugal
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_lkp_atlas_user';

	$$$
	
-- #############################################
-- # 		      ATLAS - PORTUGAL             #
-- #	      LOADING t_lkp_atlas_user     	   #
-- #############################################

drop table if exists sandbox_andre_matias.tmp_load_atlas_user;

create table sandbox_andre_matias.tmp_load_atlas_user 
distkey(cod_source_system)
sortkey(cod_atlas_user, opr_atlas_user)
as
select a.*, isnull(b.cod_city,-1) cod_city from (
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
	lkp_source.cod_source,
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
    max_cod_atlas_user.max_cod,
    row_number() over (order by source_table.opr_atlas_user desc) new_cod,
    target.cod_atlas_user,
    case
      --when target.cod_atlas_user is null then 'I'
	  when target.cod_atlas_user is null or (source_table.hash_atlas_user != target.hash_atlas_user and target.valid_from = (select dat_processing from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_atlas_user')) then 'I'
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
			isnull(dsc_atlas_user                                      ,'') +
			isnull(email_original                                      ,'') +
			isnull(password                                            ,'') +
			isnull(autologin_rev                                       ,0) +
			isnull(type                                                ,'') +
			isnull(created_at                                          ,'2099-12-31 00:00:00.000000') +
			isnull(last_login_at                                       ,'2099-12-31 00:00:00.000000') +
			isnull(default_lang                                        ,'') +
			isnull(flg_newsletter                                      ,0) +
			isnull(flg_use_offer_limits                                ,0) +
			isnull(ban_reason_id                                       ,0) +
			isnull(flg_autocomplete_defaults                           ,0) +
			isnull(default_skype                                       ,'') +
			isnull(default_phone                                       ,'') +
			isnull(default_map_address                                 ,'') +
			isnull(default_gg                                          ,'') +
			isnull(default_person                                      ,'') +
			isnull(opr_region                                   ,0) +
			isnull(opr_subregion                                ,0) +
			cast(isnull(default_lat                                    ,0) as varchar) +
			cast(isnull(default_lng                                    ,0) as varchar) +
			isnull(default_zoom                                        ,0) +
			isnull(default_district_id                                 ,0) +
			isnull(opr_city                                            ,0) +
			isnull(last_login_ip                                       ,0) +
			isnull(last_login_port                                     ,0) +
			isnull(fraudster                                           ,0) +
			isnull(rmoderation_moderated_by                            ,0) +
			--isnull(rmoderation_moderated_at                            ,'2099-12-31 00:00:00.000000') +
			isnull(rmoderation_moderated_days                          ,0) +
			isnull(rmoderation_moderated_total                         ,0) +
			--isnull(rmoderation_moderated_last                          ,'2099-12-31 00:00:00.000000') +
			cast(isnull(credits                                        ,0) as varchar) +
			isnull(flg_app                                             ,'0') +
			isnull(flg_android_app                                     ,0) +
			isnull(flg_apple_app                                       ,0) +
			isnull(flg_wp_app                                          ,0) +
			isnull(flg_spammer                                      ,0) +
			isnull(opr_source                                          ,'') +
			isnull(flg_hide_user_ads                                   ,0) +
			isnull(flg_email_msg_notif                                 ,0) +
			isnull(flg_email_alarms_notif                              ,0) +
			isnull(police_comment                                      ,'') +
			isnull(police_bank_account                                 ,'') +
			isnull(flg_monitored                                    ,0) +
			isnull(flg_hide_bank_warning                               ,0) +
			isnull(flg_external_login                                  ,0) +
			isnull(flg_business                                     ,0) +
			isnull(flg_restricted                                   ,0) +
			--isnull(trusted_started_at                                  ,'2099-12-31 00:00:00.000000') +
			isnull(flg_trusted_accepted                                ,0) +
			isnull(migration_status                                    ,'') +
			isnull(suspend_reason                                      ,'') +
			isnull(password_method                                     ,'') +
			isnull(default_person_first_name                           ,'') +
			isnull(default_person_last_name                            ,'') +
			isnull(default_postcode                                    ,'') +
			--isnull(last_modification_date                              ,'2099-12-31 00:00:00.000000') +
			isnull(flg_autorenew                                       ,0) +
			cast(isnull(quality_score                                  ,0) as varchar) +
			--isnull(first_app_login_at                                  ,'2099-12-31 00:00:00.000000') +
			isnull(flg_email_promo_notif                               ,0) +
			isnull(flg_email_expired_notif                             ,0) +
			isnull(disabled_export_clients                             ,'') +
			isnull(username_legacy                                     ,'') +
			isnull(user_legacy_id                                      ,0) +
			cast(isnull(bonus_credits                                  ,0) as varchar) +
			--isnull(bonus_credits_expire_at                             ,'2099-12-31 00:00:00.000000') +
			isnull(hermes_dirty                                        ,0) +
			isnull(flg_uses_crm                                        ,0) +
			isnull(sms_verification_phone                              ,'') +
			isnull(sms_verification_status                             ,'') +
			isnull(sms_verification_code                               ,'')
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
		cast(null as varchar) sms_verification_code
      FROM
        sandbox_andre_matias.stg_db_atlas_verticals_users a,
		sandbox_andre_matias.t_lkp_source_system b
	  where
		a.livesync_dbname = b.opr_source_system
		and b.cod_business_type = 1 -- Verticals
		and b.cod_country = 1 -- Portugal
		--and 1 = 0
	  union all
	  SELECT
		id opr_atlas_user,
        'olxpt' opr_source_system,
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
		last_login_port,
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
		sms_verification_code
	  FROM
		sandbox_andre_matias.stg_db_atlas_olxpt_users
	  --where 1 = 0
	) source,
    sandbox_andre_matias.t_lkp_source_system lkp_source_system
	where source.opr_source_system = lkp_source_system.opr_source_system
    ) source_table,
	sandbox_andre_matias.t_lkp_source lkp_source,
    (select isnull(max(cod_atlas_user),0) max_cod from sandbox_andre_matias.t_lkp_atlas_user) max_cod_atlas_user,
    sandbox_andre_matias.t_lkp_atlas_user target
  where
    isnull(source_table.opr_atlas_user,-1) = target.opr_atlas_user(+)
	and source_table.cod_source_system = target.cod_source_system (+)
    and target.valid_to(+) = 20991231	
	and isnull(source_table.opr_source,'') = lkp_source.opr_source
	and lkp_source.valid_to = 20991231
	) a,  sandbox_andre_matias.t_lkp_city b
	where
	isnull(a.opr_city,-1) = b.opr_city (+)
	and a.cod_source_system = b.cod_source_system (+)
	and b.valid_to = 20991231;

	$$$
	
delete from sandbox_andre_matias.t_lkp_atlas_user
using sandbox_andre_matias.tmp_load_atlas_user
where 
	tmp_load_atlas_user.dml_type = 'I' 
	and t_lkp_atlas_user.opr_atlas_user = tmp_load_atlas_user.opr_atlas_user
	and t_lkp_atlas_user.valid_from = (select dat_processing from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_atlas_user');
	
	$$$
	
update sandbox_andre_matias.t_lkp_atlas_user
set valid_to = (select rel_integr_proc.dat_processing from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_atlas_user') 
from sandbox_andre_matias.tmp_load_atlas_user source
where source.cod_atlas_user = sandbox_andre_matias.t_lkp_atlas_user.cod_atlas_user
and sandbox_andre_matias.t_lkp_atlas_user.valid_to = 20991231
and source.dml_type in('U','D');

	$$$
	
insert into sandbox_andre_matias.t_lkp_atlas_user
	 select
      case
        when dml_type = 'I' then max_cod + new_cod
        when dml_type = 'U' then cod_atlas_user
      end cod_atlas_user,
	  opr_atlas_user,
	  dsc_atlas_user,
	  (select rel_integr_proc.dat_processing from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_atlas_user') valid_from, 
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
      hash_atlas_user
    from
      sandbox_andre_matias.tmp_load_atlas_user
    where
      dml_type in ('U','I');

	$$$
	
-- #######################
-- ####    PASSO 5    ####
-- #######################
insert into sandbox_andre_matias.t_fac_scai_execution
  select
    max_cod_exec + 1 cod_execution,
    rel_integr_proc.cod_integration,
    rel_integr_proc.cod_process,
    1 cod_status,
    2 cod_execution_type, -- End
    rel_integr_proc.dat_processing,
    rel_integr_proc.execution_nbr,
    sysdate
  from
    sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr,
    (select isnull(max(cod_execution),0) max_cod_exec from sandbox_andre_matias.t_fac_scai_execution),
    sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc,
    sandbox_andre_matias.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 1 -- Portugal
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
update sandbox_andre_matias.t_rel_scai_integration_process
set cod_status = 1, -- Ok
last_processing_datetime = isnull((select max(operation_timestamp) from sandbox_andre_matias.tmp_load_atlas_user),last_processing_datetime)
/*from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc, sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_atlas_user'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 1
  ) source*/
from sandbox_andre_matias.t_lkp_scai_process proc 
where t_rel_scai_integration_process.cod_process = proc.cod_process
and t_rel_scai_integration_process.cod_status = 2
and t_rel_scai_integration_process.cod_country = 1
and proc.dsc_process_short = 't_lkp_atlas_user'
and t_rel_scai_integration_process.ind_active = 1
/*sandbox_andre_matias.t_rel_scai_integration_process.cod_process = source.cod_process
and sandbox_andre_matias.t_rel_scai_integration_process.cod_country = source.cod_country
and sandbox_andre_matias.t_rel_scai_integration_process.cod_integration = source.cod_integration*/;

drop table if exists sandbox_andre_matias.tmp_load_atlas_user;

	$$$
	
-- #######################
-- ####    PASSO 3    ####
-- #######################
update sandbox_andre_matias.t_rel_scai_integration_process
set dat_processing = source.dat_processing, execution_nbr = source.execution_nbr, cod_status = 2 -- Running
from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc, sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_ad'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 1
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
  ) source
where sandbox_andre_matias.t_rel_scai_integration_process.cod_process = source.cod_process
and sandbox_andre_matias.t_rel_scai_integration_process.cod_country = source.cod_country
and sandbox_andre_matias.t_rel_scai_integration_process.cod_integration = source.cod_integration;

-- #######################
-- ####    PASSO 4    ####
-- #######################
insert into sandbox_andre_matias.t_fac_scai_execution
  select
    max_cod_exec + 1 cod_execution,
    rel_integr_proc.cod_integration,
    rel_integr_proc.cod_process,
    rel_integr_proc.cod_status,
    1 cod_execution_type, -- Begin
    rel_integr_proc.dat_processing,
    rel_integr_proc.execution_nbr,
    sysdate
  from
    sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr,
    (select isnull(max(cod_execution),0) max_cod_exec from sandbox_andre_matias.t_fac_scai_execution),
    sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc,
    sandbox_andre_matias.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 1 -- Portugal
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_lkp_ad';

	$$$
		
-- #############################################
-- # 		     ATLAS - PORTUGAL              #
-- #             LOADING t_lkp_ad              #
-- #############################################

drop table if exists sandbox_andre_matias.tmp_load_ad_aux_horz;

create table sandbox_andre_matias.tmp_load_ad_aux_horz 
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
			created_at_backup_20150730 -- não se usa
	  from
		(
SELECT
				isnull(id,-1) opr_ad,
				title dsc_ad_title,
				description dsc_ad,
				'olxpt' opr_source_system,
				operation_type,
				operation_timestamp,
				isnull(category_id,-1) opr_category,
				isnull(city_id,-1) opr_city,
				isnull(user_id,-1) opr_atlas_user,
				cast(null as timestamp) last_update_date,
				created_at,
				created_at_first,
				cast(null as timestamp) bump_date,
				valid_to ad_valid_to,
				isnull(status,'') opr_ad_status,
				reason_id,
				remove_reason_details,
				phone,
				params,
				contactform contact_form,
				ip,
				port,
				search_tags,
				map_address,
				isnull(offer_seek,'') opr_offer_seek,
				isnull(solr_archive_status,'') opr_solr_archive_status,
				isnull(solr_status,'') opr_solr_status,
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
				isnull(rmoderation_status,'') opr_rmoderation_status,
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
				isnull(source,'') opr_source,
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
				paidads_id_index opr_paidad_index, -- não vai ser usado
				paidads_id_payment opr_paidad_user_payment, -- não vai ser usado
				cast(null as bigint) district_id_old, -- não se usa
				cast(null as bigint) opr_district, -- não se usa
				cast(null as bigint) opr_region, -- não se usa
				cast(null as bigint) opr_subregion, -- não se usa
				null district_name, -- não se usa
				cast(null as timestamp) created_at_backup_20150730, -- não se usa
				row_number() over (partition by id order by operation_type desc) rn
			  FROM
				sandbox_andre_matias.stg_db_atlas_olxpt_ads
			  --where 1 = 0
		)
	where
		rn = 1
);

	$$$
	
drop table if exists sandbox_andre_matias.tmp_load_ad_aux;

create table sandbox_andre_matias.tmp_load_ad_aux 
distkey(opr_source_system)
sortkey(opr_ad, opr_source_system, opr_category, opr_city, opr_atlas_user, opr_ad_status, opr_offer_seek, opr_solr_archive_status, opr_solr_status, opr_rmoderation_status, opr_source, opr_new_used)
as
(
SELECT
		isnull(id,-1) opr_ad,
		title dsc_ad_title,
		description dsc_ad,
		livesync_dbname opr_source_system,
		operation_type,
		operation_timestamp,
		isnull(category_id,-1) opr_category,
		isnull(city_id,-1) opr_city,
		isnull(user_id,-1) opr_atlas_user,
		last_update_date,
		created_at,
		created_at_first,
		bump_date,
		a.valid_to ad_valid_to,
		isnull(status,'') opr_ad_status,
		reason_id,
		remove_reason_details,
		phone,
		params,
		contactform contact_form,
		ip,
		port,
		search_tags,
		map_address,
		isnull(offer_seek,'') opr_offer_seek,
		isnull(solr_archive_status,'') opr_solr_archive_status,
		isnull(solr_status,'') opr_solr_status,
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
		isnull(rmoderation_status,'') opr_rmoderation_status,
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
		isnull(source,'') opr_source,
		net_ad_counted flg_net_ad_counted,
		was_paid_for_post flg_was_paid_for_post,
		is_paid_for_post flg_paid_for_post,
		id_legacy,
		email,
		highlight_to,
		isnull(new_used,'') opr_new_used,
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
		created_at_backup_20150730 -- não se usa
      FROM
		sandbox_andre_matias.stg_db_atlas_verticals_ads a,
		sandbox_andre_matias.t_lkp_source_system b
	  where
		a.livesync_dbname = b.opr_source_system
		and b.cod_business_type = 1 -- Verticals
		and b.cod_country = 1 -- Portugal
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
			created_at_backup_20150730 -- não se usa
	  from
		sandbox_andre_matias.tmp_load_ad_aux_horz
);

	$$$
	
drop table if exists sandbox_andre_matias.tmp_load_ad_md5;

create table sandbox_andre_matias.tmp_load_ad_md5 
distkey(opr_source_system)
sortkey(opr_ad, opr_source_system, opr_category, opr_city, opr_atlas_user, opr_ad_status, opr_offer_seek, opr_solr_archive_status, opr_solr_status, opr_rmoderation_status, opr_source, opr_new_used)
as
SELECT
		source.*,
		lkp_source_system.cod_source_system,
		md5
		(
			isnull(dsc_ad_title                                                            ,'') +
			isnull(dsc_ad                                                                  ,'') +
			isnull(opr_category                                                            ,0) +
			isnull(opr_city                                                                ,0) +
			isnull(opr_atlas_user                                                          ,0) +
			--isnull(bump_date                                                               ,'2099-12-31 00:00:00.000000') +
			isnull(ad_valid_to                                                             ,'2099-12-31 00:00:00.000000') +
			isnull(opr_ad_status                                                           ,'') +
			isnull(reason_id                                                               ,0) +
			isnull(remove_reason_details                                                   ,'') +
			isnull(phone                                                                   ,'') +
			isnull(params                                                                  ,'') +
			isnull(contact_form                                                            ,0) +
			isnull(ip                                                                      ,0) +
			isnull(port                                                                    ,0) +
			isnull(search_tags                                                             ,'') +
			isnull(map_address                                                             ,'') +
			isnull(opr_offer_seek                                                          ,'') +
			isnull(opr_solr_archive_status                                                 ,'') +
			isnull(opr_solr_status                                                         ,'') +
			isnull(external_partner_code                                                   ,'') +
			isnull(external_id                                                             ,'') +
			isnull(partner_offer_url                                                       ,'') +
			isnull(private_business                                                        ,'') +
			isnull(map_zoom                                                                ,0) +
			isnull(map_radius                                                              ,0) +
			isnull(skype                                                                   ,'') +
			isnull(gg                                                                      ,'') +
			isnull(person                                                                  ,'') +
			isnull(visible_in_profile                                                      ,0) +
			isnull(riak_ring                                                               ,0) +
			isnull(riak_key                                                                ,0) +
			isnull(riak_mapping                                                            ,0) +
			isnull(riak_order                                                              ,'') +
			isnull(riak_revision                                                           ,0) +
			isnull(riak_old                                                                ,0) +
			isnull(riak_validate                                                           ,0) +
			isnull(riak_sizes                                                              ,'') +
			--isnull(paidads_valid_to                                                        ,'2099-12-31 00:00:00.000000') +
			--isnull(ad_homepage_to                                                          ,'2099-12-31 00:00:00.000000') +
			--isnull(ad_bighomepage_to                                                       ,'2099-12-31 00:00:00.000000') +
			isnull(opr_rmoderation_status                                                  ,'') +
			isnull(rmoderation_ranking                                                     ,0) +
			isnull(rmoderation_previous_description                                        ,'') +
			isnull(rmoderation_reasons                                                     ,'') +
			isnull(rmoderation_ip_country                                                  ,'') +
			--isnull(rmoderation_moderation_started_at                                       ,'2099-12-31 00:00:00.000000') +
			--isnull(rmoderation_moderation_ended_at                                         ,'2099-12-31 00:00:00.000000') +
			--isnull(rmoderation_removed_at                                                  ,'2099-12-31 00:00:00.000000') +
			isnull(rmoderation_verified_by                                                 ,0) +
			isnull(rmoderation_forwarded_by                                                ,0) +
			isnull(rmoderation_bann_reason_id                                              ,0) +
			isnull(rmoderation_parent                                                      ,0) +
			isnull(rmoderation_duplicate_type                                              ,'') +
			isnull(rmoderation_markprice                                                   ,0) +
			isnull(rmoderation_paid                                                        ,0) +
			isnull(rmoderation_revision                                                    ,0) +
			isnull(moderation_disable_attribute                                            ,'') +
			isnull(opr_source                                                              ,'') +
			isnull(flg_net_ad_counted                                                      ,0) +
			isnull(flg_was_paid_for_post                                                   ,0) +
			isnull(flg_paid_for_post                                                    ,0) +
			isnull(id_legacy                                                               ,'') +
			isnull(email                                                                   ,'') +
			--isnull(highlight_to                                                            ,'2099-12-31 00:00:00.000000') +
			isnull(opr_new_used                                                            ,'') +
			--isnull(export_olx_to                                                           ,'2099-12-31 00:00:00.000000') +
			isnull(olx_id                                                                  ,0) +
			isnull(olx_image_collection_id                                                 ,0) +
			--isnull(migration_last_updated                                                  ,'2099-12-31 00:00:00.000000') +
			isnull(allegro_id                                                              ,0) +
			isnull(mysql_search_title                                                      ,'') +
			isnull(flg_autorenew                                                           ,0) +
			isnull(brand_program_id                                                        ,0) +
			--isnull(wp_to                                                                   ,'2099-12-31 00:00:00.000000') +
			isnull(walkaround                                                              ,'') +
			cast(isnull(user_quality_score                                                 ,0) as varchar) +
			--isnull(updated_at                                                              ,'2099-12-31 00:00:00.000000') +
			isnull(street_name                                                             ,'') +
			isnull(street_id                                                               ,0) +
			isnull(reference_id                                                            ,'') +
			isnull(punish_no_image_enabled                                                 ,0) +
			isnull(parent_id                                                               ,0) +
			isnull(panorama                                                                ,'') +
			--isnull(olx_last_updated                                                        ,'2099-12-31 00:00:00.000000') +
			isnull(mysql_search_rooms_num                                                  ,'') +
			cast(isnull(mysql_search_price_per_m                                           ,0) as varchar) +
			cast(isnull(mysql_search_price                                                 ,0) as varchar) +
			isnull(movie                                                                   ,'') +
			--isnull(mirror_to                                                               ,'2099-12-31 00:00:00.000000') +
			isnull(mailing_promotion_count                                                 ,0) +
			isnull(local_plan                                                              ,'') +
			--isnull(header_to                                                               ,'2099-12-31 00:00:00.000000') +
			isnull(header_category_id                                                      ,0) +
			isnull(hash                                                                    ,'') +
			isnull(flg_extend_automatically                                                ,0) +
			isnull(agent_id                                                                ,0) +
			cast(isnull(ad_quality_score                                                   ,0) as varchar) +
			isnull(view_3d                                                                 ,'') +
			isnull(stand_id                                                                ,0) +
			cast(isnull(map_lat                                                            ,0) as varchar) +
			cast(isnull(map_lon                                                            ,0) as varchar) +
			cast(isnull(mysql_search_m                                                     ,0) as varchar) +
			isnull(accurate_location                                                       ,0) +
			--isnull(created_at_pushup                                                       ,'2099-12-31 00:00:00.000000') +
			isnull(overlimit                                                               ,'') +
			isnull(net_ad_counted                                                          ,0) +
			isnull(was_paid_for_post                                                       ,0) +
			isnull(is_paid_for_post                                                        ,0) +
			isnull(hermes_dirty                                                            ,0) +
			isnull(hide_adverts                                                            ,0) +
			isnull(urgent                                                                  ,0) +
			--isnull(highlight                                                               ,'2099-12-31 00:00:00.000000') +
			--isnull(topads_to                                                               ,'2099-12-31 00:00:00.000000') +
			--isnull(topads_reminded_at                                                      ,'2099-12-31 00:00:00.000000') +
			--isnull(urgent_to                                                               ,'2099-12-31 00:00:00.000000') +
			isnull(pushup_recurrencies                                                     ,0)
			--isnull(pushup_next_recurrency                                                  ,'2099-12-31 00:00:00.000000') +
			--isnull(image_upload_monetization_to                                            ,'2099-12-31 00:00:00.000000')
	    ) hash_ad
	  FROM
		sandbox_andre_matias.tmp_load_ad_aux source,
    sandbox_andre_matias.t_lkp_source_system lkp_source_system
	where source.opr_source_system = lkp_source_system.opr_source_system;

	$$$
	
drop table if exists sandbox_andre_matias.tmp_load_ad;

create table sandbox_andre_matias.tmp_load_ad 
distkey(cod_source_system)
sortkey(cod_ad, opr_ad)
as
  select source.*, isnull(lkp_city.cod_city,-1) cod_city, isnull(lkp_atlas_user.cod_atlas_user,-1) cod_atlas_user, isnull(lkp_category.cod_category,-1) cod_category
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
	lkp_ad_status.cod_ad_status,
	source_table.reason_id,
	source_table.remove_reason_details,
	source_table.phone,
	source_table.params,
	source_table.contact_form,
	source_table.ip,
	source_table.port,
	source_table.search_tags,
	source_table.map_address,
	lkp_offer_seek.cod_offer_seek,
	lkp_solr_archive_status.cod_solr_archive_status,
	lkp_solr_status.cod_solr_status,
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
	lkp_rmoderation_status.cod_rmoderation_status,
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
	lkp_source.cod_source,
	source_table.flg_net_ad_counted,
	source_table.flg_was_paid_for_post,
	source_table.flg_paid_for_post,
	source_table.id_legacy,
	source_table.email,
	source_table.highlight_to,
	lkp_new_used.cod_new_used,
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
    max_cod_ad.max_cod,
    row_number() over (order by source_table.opr_ad desc) new_cod,
    target.cod_ad,
    case
      --when target.cod_ad is null then 'I'
	  when target.cod_ad is null or (source_table.hash_ad != target.hash_ad and target.valid_from = (select dat_processing from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_ad')) then 'I'
	  when source_table.operation_type = 'delete' then 'D'
      when source_table.hash_ad != target.hash_ad then 'U'
        else 'X'
    end dml_type
  from
	sandbox_andre_matias.tmp_load_ad_md5 source_table,
	sandbox_andre_matias.t_lkp_ad_status lkp_ad_status,
	sandbox_andre_matias.t_lkp_offer_seek lkp_offer_seek,
	sandbox_andre_matias.t_lkp_solr_archive_status lkp_solr_archive_status,
	sandbox_andre_matias.t_lkp_solr_status lkp_solr_status,
	sandbox_andre_matias.t_lkp_rmoderation_status lkp_rmoderation_status,
	sandbox_andre_matias.t_lkp_source lkp_source,
	sandbox_andre_matias.t_lkp_new_used lkp_new_used,
    (select isnull(max(cod_ad),0) max_cod from sandbox_andre_matias.t_lkp_ad) max_cod_ad,
    sandbox_andre_matias.t_lkp_ad target
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
	sandbox_andre_matias.t_lkp_city lkp_city,
	sandbox_andre_matias.t_lkp_atlas_user lkp_atlas_user,
	sandbox_andre_matias.t_lkp_category lkp_category
where 
	source.opr_city = lkp_city.opr_city (+)
	and source.cod_source_system = lkp_city.cod_source_system (+) -- new
	and lkp_city.valid_to (+) = 20991231
	and source.opr_atlas_user = lkp_atlas_user.opr_atlas_user (+)
	and source.cod_source_system = lkp_atlas_user.cod_source_system (+) -- new
	and lkp_atlas_user.valid_to (+) = 20991231
	and source.opr_category = lkp_category.opr_category (+)
	and source.cod_source_system = lkp_category.cod_source_system (+) -- new
	and lkp_category.valid_to (+) = 20991231;

	$$$ -- 129
	
delete from sandbox_andre_matias.t_lkp_ad
using sandbox_andre_matias.tmp_load_ad
where 
	tmp_load_ad.dml_type = 'I' 
	and t_lkp_ad.opr_ad = tmp_load_ad.opr_ad
	and t_lkp_ad.valid_from = (select dat_processing from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_ad');
	
	$$$
	
update sandbox_andre_matias.t_lkp_ad
set valid_to = (select rel_integr_proc.dat_processing from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_ad')
from sandbox_andre_matias.tmp_load_ad source
where source.cod_ad = sandbox_andre_matias.t_lkp_ad.cod_ad
and sandbox_andre_matias.t_lkp_ad.valid_to = 20991231
and source.dml_type in('U','D');

insert into sandbox_andre_matias.t_lkp_ad
    select
		case
			when dml_type = 'I' then max_cod + new_cod
			when dml_type = 'U' then cod_ad
		end cod_ad,
		opr_ad,
		dsc_ad_title,
		dsc_ad,
		(select rel_integr_proc.dat_processing from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 1 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_ad') valid_from, 
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
		hash_ad
    from
      sandbox_andre_matias.tmp_load_ad
    where
      dml_type in ('U','I');

	$$$ -- 131
	
-- #######################
-- ####    PASSO 5    ####
-- #######################
insert into sandbox_andre_matias.t_fac_scai_execution
  select
    max_cod_exec + 1 cod_execution,
    rel_integr_proc.cod_integration,
    rel_integr_proc.cod_process,
    1 cod_status,
    2 cod_execution_type, -- End
    rel_integr_proc.dat_processing,
    rel_integr_proc.execution_nbr,
    sysdate
  from
    sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr,
    (select isnull(max(cod_execution),0) max_cod_exec from sandbox_andre_matias.t_fac_scai_execution),
    sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc,
    sandbox_andre_matias.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 1 -- Portugal
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
update sandbox_andre_matias.t_rel_scai_integration_process
set cod_status = 1, -- Ok
last_processing_datetime = isnull((select max(operation_timestamp) from sandbox_andre_matias.tmp_load_ad),last_processing_datetime)
/*from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc, sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_ad'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 1
  ) source*/
from sandbox_andre_matias.t_lkp_scai_process proc 
where t_rel_scai_integration_process.cod_process = proc.cod_process
and t_rel_scai_integration_process.cod_status = 2
and t_rel_scai_integration_process.cod_country = 1
and proc.dsc_process_short = 't_lkp_ad'
and t_rel_scai_integration_process.ind_active = 1
/*sandbox_andre_matias.t_rel_scai_integration_process.cod_process = source.cod_process
and sandbox_andre_matias.t_rel_scai_integration_process.cod_country = source.cod_country
and sandbox_andre_matias.t_rel_scai_integration_process.cod_integration = source.cod_integration*/;

drop table if exists sandbox_andre_matias.tmp_load_ad;
drop table if exists sandbox_andre_matias.tmp_load_ad_md5;
drop table if exists sandbox_andre_matias.tmp_load_ad_aux;
drop table if exists sandbox_andre_matias.tmp_load_ad_aux_horz;

	$$$
	
/*
-- #############################################
-- # 		     ATLAS - PORTUGAL              #
-- #           LOADING t_fac_answer            #
-- #############################################

--drop table sandbox_andre_matias.tmp_load_answer;
--delete from sandbox_andre_matias.t_fac_answer;

create table sandbox_andre_matias.tmp_load_answer_aux AS
(
SELECT
        cast(isnull(id,-1) as bigint) opr_answer,
		livesync_dbname opr_source_system,
		parent_id opr_answer_parent,
		cast(isnull(user_id,-1)  as bigint) opr_atlas_user,
		cast(isnull(ad_id,-1)  as bigint) opr_ad,
		cast(isnull(seller_id,-1)  as bigint) opr_atlas_user_seller,
		cast(isnull(buyer_id,-1)  as bigint) opr_atlas_user_buyer,
		cast(isnull(sender_id,-1)  as bigint) opr_atlas_user_sender,
		cast(isnull(reciever_id,-1)  as bigint) opr_atlas_user_receiver,
		sender_phone,
		readed flg_readed,
		visible flg_visible,
		topics_count,
		unreaded_count,
		has_attachments flg_has_attachments,
		star flg_star,
		posted dat_posted,
		readed_at dat_readed_at,
		number,
		last_posted_in dat_last_posted_in,
		last_posted_out dat_last_posted_out,
		last_posted_id opr_answer_last_posted,
		ad_title,
		message,
		attachments_names,
		riak_ring,
		riak_key,
		riak_mapping,
		riak_order,
		riak_old,
		riak_validate,
		ip,
		port,
		isnull(source,'') opr_source,
		isnull(spam_status,'') opr_spam_status,
		isnull(spam_reason,'') opr_spam_reason,
		--spam_reason_id , -- ?
		token_valid flg_token_valid,
		seen_ago,
		show_bank_warning flg_show_bank_warning,
		checked_by,
		checked_at dat_checked_at,
		checked_as,
		embrace_user_id,
		olx_conversation_id
FROM
        sandbox_andre_matias.stg_db_atlas_verticals_answers a,
        sandbox_andre_matias.t_lkp_source_system b
where
        a.livesync_dbname = b.opr_source_system
        and b.cod_business_type = 1 -- Verticals
        and b.cod_country = 1 -- Portugal
union all
SELECT
        cast(isnull(id,-1) as bigint) opr_answer,
		'olxpt' opr_source_system,
		parent_id opr_answer_parent,
		cast(isnull(user_id,-1)  as bigint) opr_atlas_user,
		cast(isnull(ad_id,-1)  as bigint) opr_ad,
		cast(isnull(seller_id,-1)  as bigint) opr_atlas_user_seller,
		cast(isnull(buyer_id,-1)  as bigint) opr_atlas_user_buyer,
		cast(isnull(sender_id,-1)  as bigint) opr_atlas_user_sender,
		cast(isnull(reciever_id,-1)  as bigint) opr_atlas_user_receiver,
		null sender_phone,
		readed flg_readed,
		visible flg_visible,
		topics_count,
		unreaded_count ,
		has_attachments flg_has_attachments,
		star flg_star,
		posted dat_posted,
		readed_at dat_readed_at,
		number,
		last_posted_in dat_last_posted_in,
		last_posted_out dat_last_posted_out,
		last_posted_id opr_answer_last_posted,
		ad_title,
		message,
		attachments_names,
		riak_ring,
		riak_key,
		riak_mapping,
		riak_order,
		riak_old,
		riak_validate,
		ip,
		port,
		isnull(source,'') opr_source,
		isnull(spam_status,'') opr_spam_status,
		isnull(spam_reason,'') opr_spam_reason,
		--spam_reason_id , -- ?
		token_valid flg_token_valid,
		seen_ago,
		show_bank_warning flg_show_bank_warning,
		checked_by,
		checked_at dat_checked_at,
		checked_as,
		cast(null as bigint) embrace_user_id,
		cast(null as bigint) olx_conversation_id
FROM
        sandbox_andre_matias.stg_db_atlas_olxpt_answers
);

create table sandbox_andre_matias.tmp_load_answer_aux_md5 AS 
(
	SELECT
		*,
		md5
		(
			isnull(opr_atlas_user           ,0) +
			isnull(opr_ad                   ,0) +
			isnull(opr_atlas_user_seller    ,0) +
			isnull(opr_atlas_user_buyer     ,0) +
			isnull(opr_atlas_user_sender    ,0) +
			isnull(opr_atlas_user_receiver  ,0) +
			isnull(sender_phone             ,'') +
			isnull(flg_readed               ,0) +
			isnull(flg_visible              ,0) +
			isnull(topics_count             ,0) +
			isnull(unreaded_count           ,0) +
			isnull(flg_has_attachments      ,0) +
			isnull(flg_star                 ,0) +
			--isnull(dat_posted               ,'2099-12-31 00:00:00.000000') +
			--isnull(dat_readed_at            ,'2099-12-31 00:00:00.000000') +
			isnull(number                   ,0) +
			--isnull(dat_last_posted_in       ,'2099-12-31 00:00:00.000000') +
			--isnull(dat_last_posted_out      ,'2099-12-31 00:00:00.000000') +
			isnull(opr_answer_last_posted   ,0) +
			isnull(ad_title                 ,'') +
			isnull(message                  ,'') +
			isnull(attachments_names        ,'') +
			isnull(riak_ring                ,0) +
			isnull(riak_key                 ,0) +
			isnull(riak_mapping             ,0) +
			isnull(riak_order               ,'') +
			isnull(riak_old                 ,0) +
			isnull(riak_validate            ,0) +
			isnull(ip                       ,0) +
			isnull(port                     ,0) +
			isnull(opr_source               ,'') +
			isnull(opr_spam_status          ,'') +
			isnull(opr_spam_reason          ,'') +
			isnull(flg_token_valid          ,0) +
			isnull(seen_ago                 ,0) +
			isnull(flg_show_bank_warning    ,0) +
			isnull(checked_by               ,0) +
			--isnull(dat_checked_at           ,'2099-12-31 00:00:00.000000') +
			isnull(checked_as               ,'') +
			isnull(embrace_user_id          ,0) +
			isnull(olx_conversation_id      ,0)
	    ) hash_answer
	  FROM
		sandbox_andre_matias.tmp_load_answer_aux	
);

create table sandbox_andre_matias.tmp_load_answer as
  select
	source_table.opr_answer,
	source_table.sender_phone,
	source_table.flg_readed,
	source_table.flg_visible,
	source_table.topics_count,
	source_table.unreaded_count,
	source_table.flg_has_attachments,
	source_table.flg_star,
	source_table.dat_posted,
	source_table.dat_readed_at,
	source_table.number,
	source_table.dat_last_posted_in,
	source_table.dat_last_posted_out,
	source_table.opr_answer_last_posted,
	source_table.ad_title,
	source_table.message,
	source_table.attachments_names,
	source_table.riak_ring,
	source_table.riak_key,
	source_table.riak_mapping,
	source_table.riak_order,
	source_table.riak_old,
	source_table.riak_validate,
	source_table.ip,
	source_table.port,
	source_table.flg_token_valid,
	source_table.seen_ago,
	source_table.flg_show_bank_warning,
	source_table.checked_by,
	source_table.dat_checked_at,
	source_table.checked_as,
	source_table.embrace_user_id,
	source_table.olx_conversation_id,
  lkp_source_system.cod_source_system,
	lkp_ad.cod_ad,
	lkp_spam_status.cod_spam_status,
	lkp_spam_reason.cod_spam_reason,
	lkp_atlas_user.cod_atlas_user,
	lkp_atlas_user.cod_atlas_user cod_atlas_seller,
	lkp_atlas_user.cod_atlas_user cod_atlas_buyer,
	lkp_atlas_user.cod_atlas_user cod_atlas_sender,
	lkp_atlas_user.cod_atlas_user cod_atlas_receiver,
    lkp_source.cod_source,
    max_cod_answer.max_cod,
    row_number() over (order by source_table.opr_answer desc) new_cod,
    target.cod_answer,
    case
      when target.cod_answer is null then 'I'
      when source_table.hash_answer != target.hash_answer then 'U'
        else 'X'
    end dml_type
  from
    sandbox_andre_matias.tmp_load_answer_aux_md5 source_table,
    sandbox_andre_matias.t_lkp_source_system lkp_source_system,
	sandbox_andre_matias.t_lkp_atlas_user lkp_atlas_user,
	sandbox_andre_matias.t_lkp_atlas_user lkp_atlas_seller,
	sandbox_andre_matias.t_lkp_atlas_user lkp_atlas_buyer,
	sandbox_andre_matias.t_lkp_atlas_user lkp_atlas_sender,
	sandbox_andre_matias.t_lkp_atlas_user lkp_atlas_receiver,
    sandbox_andre_matias.t_lkp_ad lkp_ad,
	sandbox_andre_matias.t_lkp_spam_status lkp_spam_status,
	sandbox_andre_matias.t_lkp_spam_reason lkp_spam_reason,
    sandbox_andre_matias.t_lkp_source lkp_source,
    (select isnull(max(cod_answer),0) max_cod from sandbox_andre_matias.t_fac_answer) max_cod_answer,
    sandbox_andre_matias.t_fac_answer target
  where
    source_table.opr_source_system = lkp_source_system.opr_source_system
    and source_table.opr_answer = target.opr_answer(+)
    and source_table.opr_atlas_user = lkp_atlas_user.opr_atlas_user
	and lkp_source_system.cod_source_system = lkp_atlas_user.cod_source_system -- new
	and lkp_atlas_user.valid_to = 20991231
    and source_table.opr_atlas_user_seller = lkp_atlas_user.opr_atlas_user
	and lkp_source_system.cod_source_system = lkp_atlas_user.cod_source_system -- new
	and lkp_atlas_user.valid_to = 20991231
    and source_table.opr_atlas_user_buyer = lkp_atlas_user.opr_atlas_user
	and lkp_source_system.cod_source_system = lkp_atlas_user.cod_source_system -- new
	and lkp_atlas_user.valid_to = 20991231
    and source_table.opr_atlas_user_sender = lkp_atlas_user.opr_atlas_user
	and lkp_source_system.cod_source_system = lkp_atlas_user.cod_source_system -- new
	and lkp_atlas_user.valid_to = 20991231
    and source_table.opr_atlas_user_receiver = lkp_atlas_user.opr_atlas_user
	and lkp_source_system.cod_source_system = lkp_atlas_user.cod_source_system -- new
	and lkp_atlas_user.valid_to = 20991231
    and source_table.opr_ad = lkp_ad.opr_ad
	and lkp_source_system.cod_source_system = lkp_ad.cod_source_system -- new
    and lkp_ad.valid_to = 20991231
    and source_table.opr_spam_status = lkp_spam_status.opr_spam_status
	--and lkp_source_system.cod_source_system = lkp_spam_status.cod_source_system -- new
    and lkp_spam_status.valid_to = 20991231
    and source_table.opr_spam_reason = lkp_spam_reason.opr_spam_reason
	--and lkp_source_system.cod_source_system = lkp_spam_reason.cod_source_system -- new
    and lkp_spam_reason.valid_to = 20991231
    and source_table.opr_source = lkp_source.opr_source
	--and lkp_source_system.cod_source_system = lkp_source.cod_source_system -- new
    and lkp_source.valid_to = 20991231;

insert into sandbox_andre_matias.t_hst_answer
    select
      target.*
    from
      sandbox_andre_matias.t_fac_answer target,
      sandbox_andre_matias.tmp_load_answer source
    where
      target.opr_answer = source.opr_answer
      and source.dml_type = 'U';

delete from sandbox_andre_matias.t_fac_answer
using sandbox_andre_matias.tmp_load_answer
where sandbox_andre_matias.t_fac_answer.opr_answer=sandbox_andre_matias.tmp_load_answer.opr_answer
and sandbox_andre_matias.tmp_load_answer.dml_type = 'U';

insert into sandbox_andre_matias.t_fac_answer
    select
      case
        when dml_type = 'I' then max_cod + new_cod
        when dml_type = 'U' then cod_answer
      end cod_answer,
	  opr_answer,
	  -1 cod_answer_parent,
	  cod_atlas_user,
	  cod_ad,
	  cod_atlas_user_seller,
	  cod_atlas_user_buyer,
	  cod_atlas_user_sender,
	  cod_atlas_user_receiver,
	  cod_source_system,
	  sender_phone,
	  flg_readed,
	  flg_visible,
	  topics_count,
	  unreaded_count,
	  flg_has_attachments,
	  flg_star,
	  dat_posted,
	  dat_readed_at,
	  number,
	  dat_last_posted_in,
	  dat_last_posted_out,
	  opr_answer_last_posted,
	  message,
	  attachment_names,
	  riak_ring,
	  riak_key,
	  riak_mapping,
	  riak_order,
	  riak_old,
	  riak_validate,
	  ip,
	  port,
	  cod_source,
	  cod_spam_status,
	  cod_spam_reason,
	  flg_token_valid,
	  seen_ago,
	  flg_show_bank_warning,
	  checked_by,
	  dat_checked_at,
	  checked_as,
	  embrace_user_id,
	  olx_conversation_id,
	  hash_answer
    from
      sandbox_andre_matias.tmp_load_answer
    where
      dml_type in ('U','I');

	  -- carregamento do answer parent
update sandbox_andre_matias.t_fac_answer
set cod_answer_parent = fac.cod_answer
from sandbox_andre_matias.tmp_load_answer source, sandbox_andre_matias.t_fac_answer fac, sandbox_andre_matias.t_lkp_source_system ss
where isnull(source.opr_answer_parent,-1) = fac.opr_answer
and source.cod_source_system = ss.cod_source_system
and ss.cod_country = 1
and fac.cod_answer_parent = -2;

drop table sandbox_andre_matias.tmp_load_answer;
drop table sandbox_andre_matias.tmp_load_answer_aux;
drop table sandbox_andre_matias.tmp_load_answer_aux_md5;
*/

-- #######################
-- ####    PASSO 3    ####
-- #######################
update sandbox_andre_matias.t_rel_scai_integration_process
set dat_processing = source.dat_processing, execution_nbr = source.execution_nbr, cod_status = 2 -- Running
from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc, sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_fac_answer_outgoing'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 1
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
  ) source
where sandbox_andre_matias.t_rel_scai_integration_process.cod_process = source.cod_process
and sandbox_andre_matias.t_rel_scai_integration_process.cod_country = source.cod_country
and sandbox_andre_matias.t_rel_scai_integration_process.cod_integration = source.cod_integration;

-- #######################
-- ####    PASSO 4    ####
-- #######################
insert into sandbox_andre_matias.t_fac_scai_execution
  select
    max_cod_exec + 1 cod_execution,
    rel_integr_proc.cod_integration,
    rel_integr_proc.cod_process,
    rel_integr_proc.cod_status,
    1 cod_execution_type, -- Begin
    rel_integr_proc.dat_processing,
    rel_integr_proc.execution_nbr,
    sysdate
  from
    sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr,
    (select isnull(max(cod_execution),0) max_cod_exec from sandbox_andre_matias.t_fac_scai_execution),
    sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc,
    sandbox_andre_matias.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 1 -- Portugal
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_fac_answer_outgoing';

	$$$
	
-- #############################################
-- # 		   ATLAS - PORTUGAL                #
-- #       LOADING t_fac_answer_outgoing       #
-- #############################################

drop table if exists sandbox_andre_matias.tmp_load_answer_step1_outgoing;

create table sandbox_andre_matias.tmp_load_answer_step1_outgoing 
as
(
  select
    cast(isnull(id,-1) as bigint) opr_answer,
    livesync_dbname opr_source_system,
	operation_timestamp,
    parent_id opr_answer_parent,
    cast(isnull(ad_id,-1)  as bigint) opr_ad,
    cast(isnull(reciever_id,-1)  as bigint) opr_atlas_user_receiver,
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
    isnull(source,'') opr_source,
    embrace_user_id,
    olx_conversation_id
  from
    sandbox_andre_matias.stg_db_atlas_verticals_answers a,
    sandbox_andre_matias.t_lkp_source_system b
  where
    a.livesync_dbname = b.opr_source_system
    and b.cod_business_type = 1 -- Verticals
    and b.cod_country = 1 -- Portugal
    and a.seller_id = a.sender_id
	--and 1 = 0
  union all
  select
    cast(isnull(id,-1) as bigint) opr_answer,
    'olxpt' opr_source_system,
	operation_timestamp,
    parent_id opr_answer_parent,
    cast(isnull(ad_id,-1)  as bigint) opr_ad,
    cast(isnull(reciever_id,-1)  as bigint) opr_atlas_user_receiver,
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
    port,
    isnull(source,'') opr_source,
    cast(null as bigint) embrace_user_id,
    cast(null as bigint) olx_conversation_id
  from
    sandbox_andre_matias.stg_db_atlas_olxpt_answers
  where
    seller_id = sender_id
	--and 1 = 0
);

	$$$
	
drop table if exists sandbox_andre_matias.tmp_load_answer_step2_outgoing;

create table sandbox_andre_matias.tmp_load_answer_step2_outgoing 
distkey(opr_source_system)
sortkey(opr_answer, opr_source_system, opr_answer_parent, opr_ad, opr_atlas_user_receiver, opr_source)
as
(
  select
    source.*,
	lkp_source_system.cod_source_system,
	md5
    (
      isnull(opr_atlas_user_receiver  ,0) +
      isnull(sender_phone             ,'') +
      isnull(flg_readed               ,0) +
      isnull(flg_star                 ,0) +
      isnull(number                   ,0) +
      isnull(opr_answer_last_posted   ,0) +
      isnull(ip                       ,0) +
      isnull(port                     ,0) +
      isnull(opr_source               ,'') +
      isnull(embrace_user_id          ,0) +
      isnull(olx_conversation_id      ,0) +
      isnull(opr_answer_parent       ,0)
    )
     hash_answer
  from
    sandbox_andre_matias.tmp_load_answer_step1_outgoing source,
    sandbox_andre_matias.t_lkp_source_system lkp_source_system
	where source.opr_source_system = lkp_source_system.opr_source_system
);

	$$$
	
drop table if exists sandbox_andre_matias.tmp_load_answer_step3_outgoing;

create table sandbox_andre_matias.tmp_load_answer_step3_outgoing 
distkey(cod_source_system)
sortkey(opr_answer)
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
    lkp_ad.cod_ad,
    lkp_atlas_receiver.cod_atlas_user cod_atlas_user_receiver,
    lkp_source.cod_source,
    max_cod_answer.max_cod,
    row_number() over (order by source_table.opr_answer desc) new_cod,
    target.cod_answer,
    source_table.hash_answer,
    case
      when target.cod_answer is null then 'I'
      when source_table.hash_answer != target.hash_answer then 'U'
        else 'X'
    end dml_type
  from
    sandbox_andre_matias.tmp_load_answer_step2_outgoing source_table,
    sandbox_andre_matias.t_lkp_atlas_user lkp_atlas_receiver,
    sandbox_andre_matias.t_lkp_ad lkp_ad,
    sandbox_andre_matias.t_lkp_source lkp_source,
    (select isnull(max(cod_answer),0) max_cod from sandbox_andre_matias.t_fac_answer_outgoing) max_cod_answer,
    sandbox_andre_matias.t_fac_answer_outgoing target
  where
    source_table.opr_answer = target.opr_answer(+)
	and source_table.cod_source_system = target.cod_source_system (+)
    and source_table.opr_atlas_user_receiver = lkp_atlas_receiver.opr_atlas_user
    and source_table.cod_source_system = lkp_atlas_receiver.cod_source_system -- new
    and lkp_atlas_receiver.valid_to = 20991231
    and source_table.opr_ad = lkp_ad.opr_ad
    and source_table.cod_source_system = lkp_ad.cod_source_system -- new
    and lkp_ad.valid_to = 20991231
    and source_table.opr_source = lkp_source.opr_source
    and lkp_source.valid_to = 20991231;

	$$$
	
insert into sandbox_andre_matias.t_hst_answer_outgoing
    select
      target.*
    from
      sandbox_andre_matias.t_fac_answer_outgoing target,
      sandbox_andre_matias.tmp_load_answer_step3_outgoing source
    where
      target.opr_answer = source.opr_answer
      and source.dml_type = 'U';

	$$$
	
delete from sandbox_andre_matias.t_fac_answer_outgoing
using sandbox_andre_matias.tmp_load_answer_step3_outgoing
where sandbox_andre_matias.t_fac_answer_outgoing.opr_answer=sandbox_andre_matias.tmp_load_answer_step3_outgoing.opr_answer
and sandbox_andre_matias.tmp_load_answer_step3_outgoing.dml_type = 'U';

	$$$
	
insert into sandbox_andre_matias.t_fac_answer_outgoing
    select
      case
        when dml_type = 'I' then max_cod + new_cod
        when dml_type = 'U' then cod_answer
      end cod_answer,
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
      hash_answer
    from
      sandbox_andre_matias.tmp_load_answer_step3_outgoing
    where
      dml_type in ('U','I');

-- #######################
-- ####    PASSO 5    ####
-- #######################
insert into sandbox_andre_matias.t_fac_scai_execution
  select
    max_cod_exec + 1 cod_execution,
    rel_integr_proc.cod_integration,
    rel_integr_proc.cod_process,
    1 cod_status,
    2 cod_execution_type, -- End
    rel_integr_proc.dat_processing,
    rel_integr_proc.execution_nbr,
    sysdate
  from
    sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr,
    (select isnull(max(cod_execution),0) max_cod_exec from sandbox_andre_matias.t_fac_scai_execution),
    sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc,
    sandbox_andre_matias.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 1 -- Portugal
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
update sandbox_andre_matias.t_rel_scai_integration_process
set cod_status = 1, -- Ok
last_processing_datetime = isnull((select max(operation_timestamp) from sandbox_andre_matias.tmp_load_answer_step3_outgoing),last_processing_datetime)
/*from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc, sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_ad'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 1
  ) source*/
from sandbox_andre_matias.t_lkp_scai_process proc 
where t_rel_scai_integration_process.cod_process = proc.cod_process
and t_rel_scai_integration_process.cod_status = 2
and t_rel_scai_integration_process.cod_country = 1
and proc.dsc_process_short = 't_fac_answer_outgoing'
and t_rel_scai_integration_process.ind_active = 1
/*sandbox_andre_matias.t_rel_scai_integration_process.cod_process = source.cod_process
and sandbox_andre_matias.t_rel_scai_integration_process.cod_country = source.cod_country
and sandbox_andre_matias.t_rel_scai_integration_process.cod_integration = source.cod_integration*/;

drop table if exists sandbox_andre_matias.tmp_load_answer_step1_outgoing;
drop table if exists sandbox_andre_matias.tmp_load_answer_step2_outgoing;
drop table if exists sandbox_andre_matias.tmp_load_answer_step3_outgoing;

	$$$
	
-- #######################
-- ####    PASSO 3    ####
-- #######################
update sandbox_andre_matias.t_rel_scai_integration_process
set dat_processing = source.dat_processing, execution_nbr = source.execution_nbr, cod_status = 2 -- Running
from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc, sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_fac_answer_incoming'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 1
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
  ) source
where sandbox_andre_matias.t_rel_scai_integration_process.cod_process = source.cod_process
and sandbox_andre_matias.t_rel_scai_integration_process.cod_country = source.cod_country
and sandbox_andre_matias.t_rel_scai_integration_process.cod_integration = source.cod_integration;

-- #######################
-- ####    PASSO 4    ####
-- #######################
insert into sandbox_andre_matias.t_fac_scai_execution
  select
    max_cod_exec + 1 cod_execution,
    rel_integr_proc.cod_integration,
    rel_integr_proc.cod_process,
    rel_integr_proc.cod_status,
    1 cod_execution_type, -- Begin
    rel_integr_proc.dat_processing,
    rel_integr_proc.execution_nbr,
    sysdate
  from
    sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr,
    (select isnull(max(cod_execution),0) max_cod_exec from sandbox_andre_matias.t_fac_scai_execution),
    sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc,
    sandbox_andre_matias.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 1 -- Portugal
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_fac_answer_incoming';

	$$$
	
-- #############################################
-- # 		   ATLAS - PORTUGAL                #
-- #       LOADING t_fac_answer_incoming       #
-- #############################################

drop table if exists sandbox_andre_matias.tmp_load_answer_step1_incoming;

create table sandbox_andre_matias.tmp_load_answer_step1_incoming as
(
  select
    cast(isnull(id,-1) as bigint) opr_answer,
    livesync_dbname opr_source_system,
	operation_timestamp,
    parent_id opr_answer_parent,
    cast(isnull(ad_id,-1)  as bigint) opr_ad,
    cast(isnull(sender_id,-1)  as bigint) opr_atlas_user_sender,
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
    isnull(source,'') opr_source,
    embrace_user_id,
    olx_conversation_id
  from
    sandbox_andre_matias.stg_db_atlas_verticals_answers a,
    sandbox_andre_matias.t_lkp_source_system b
  where
    a.livesync_dbname = b.opr_source_system
    and b.cod_business_type = 1 -- Verticals
    and b.cod_country = 1 -- Portugal
    and a.seller_id = a.reciever_id
	--and 1 = 0
  union all
  select
    cast(isnull(id,-1) as bigint) opr_answer,
    'olxpt' opr_source_system,
	operation_timestamp,
    parent_id opr_answer_parent,
    cast(isnull(ad_id,-1)  as bigint) opr_ad,
    cast(isnull(sender_id,-1)  as bigint) opr_atlas_user_sender,
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
    port,
    isnull(source,'') opr_source,
    cast(null as bigint) embrace_user_id,
    cast(null as bigint) olx_conversation_id
  from
    sandbox_andre_matias.stg_db_atlas_olxpt_answers
  where
    seller_id = reciever_id
	--and 1 = 0
);

	$$$
	
drop table if exists sandbox_andre_matias.tmp_load_answer_step2_incoming;

create table sandbox_andre_matias.tmp_load_answer_step2_incoming 
distkey(opr_source_system)
sortkey(opr_answer, opr_source_system, opr_answer_parent, opr_ad, opr_atlas_user_sender, opr_source)
AS
(
  select
    source.*,
	lkp_source_system.cod_source_system,
	md5
    (
    isnull(opr_atlas_user_sender    ,0) +
    isnull(sender_phone             ,'') +
    isnull(flg_readed               ,0) +
    isnull(flg_star                 ,0) +
    isnull(number                   ,0) +
    isnull(opr_answer_last_posted   ,0) +
    isnull(ip                       ,0) +
    isnull(port                     ,0) +
    isnull(opr_source               ,'') +
    isnull(embrace_user_id          ,0) +
    isnull(olx_conversation_id      ,0) +
    isnull(opr_answer_parent        ,0)
    ) hash_answer
  from
    sandbox_andre_matias.tmp_load_answer_step1_incoming source,
    sandbox_andre_matias.t_lkp_source_system lkp_source_system
	where source.opr_source_system = lkp_source_system.opr_source_system
);

	$$$
	
drop table if exists sandbox_andre_matias.tmp_load_answer_step3_incoming;

create table sandbox_andre_matias.tmp_load_answer_step3_incoming 
distkey(cod_source_system)
sortkey(opr_answer)
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
    lkp_ad.cod_ad,
    lkp_atlas_sender.cod_atlas_user cod_atlas_user_sender,
    lkp_source.cod_source,
    max_cod_answer.max_cod,
    row_number() over (order by source_table.opr_answer desc) new_cod,
    target.cod_answer,
    source_table.hash_answer,
    case
      when target.cod_answer is null then 'I'
      when source_table.hash_answer != target.hash_answer then 'U'
        else 'X'
    end dml_type
  from
    sandbox_andre_matias.tmp_load_answer_step2_incoming source_table,
    sandbox_andre_matias.t_lkp_atlas_user lkp_atlas_sender,
    sandbox_andre_matias.t_lkp_ad lkp_ad,
    sandbox_andre_matias.t_lkp_source lkp_source,
    (select isnull(max(cod_answer),0) max_cod from sandbox_andre_matias.t_fac_answer_incoming) max_cod_answer,
    sandbox_andre_matias.t_fac_answer_incoming target
  where
    source_table.opr_answer = target.opr_answer(+)
	and source_table.cod_source_system = target.cod_source_system (+)
    and source_table.opr_atlas_user_sender = lkp_atlas_sender.opr_atlas_user
    and source_table.cod_source_system = lkp_atlas_sender.cod_source_system -- new
    and lkp_atlas_sender.valid_to = 20991231
    and source_table.opr_ad = lkp_ad.opr_ad
    and source_table.cod_source_system = lkp_ad.cod_source_system -- new
    and lkp_ad.valid_to = 20991231
    and source_table.opr_source = lkp_source.opr_source
    and lkp_source.valid_to = 20991231;

	$$$
	
insert into sandbox_andre_matias.t_hst_answer_incoming
    select
      target.*
    from
      sandbox_andre_matias.t_fac_answer_incoming target,
      sandbox_andre_matias.tmp_load_answer_step3_incoming source
    where
      target.opr_answer = source.opr_answer
      and source.dml_type = 'U';

	$$$
	
delete from sandbox_andre_matias.t_fac_answer_incoming
using sandbox_andre_matias.tmp_load_answer_step3_incoming
where sandbox_andre_matias.t_fac_answer_incoming.opr_answer=sandbox_andre_matias.tmp_load_answer_step3_incoming.opr_answer
and sandbox_andre_matias.tmp_load_answer_step3_incoming.dml_type = 'U';

	$$$
	
insert into sandbox_andre_matias.t_fac_answer_incoming
    select
      case
        when dml_type = 'I' then max_cod + new_cod
        when dml_type = 'U' then cod_answer
      end cod_answer,
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
      hash_answer
    from
      sandbox_andre_matias.tmp_load_answer_step3_incoming
    where
      dml_type in ('U','I');

	$$$
	
-- #######################
-- ####    PASSO 5    ####
-- #######################
insert into sandbox_andre_matias.t_fac_scai_execution
  select
    max_cod_exec + 1 cod_execution,
    rel_integr_proc.cod_integration,
    rel_integr_proc.cod_process,
    1 cod_status,
    2 cod_execution_type, -- End
    rel_integr_proc.dat_processing,
    rel_integr_proc.execution_nbr,
    sysdate
  from
    sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr,
    (select isnull(max(cod_execution),0) max_cod_exec from sandbox_andre_matias.t_fac_scai_execution),
    sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc,
    sandbox_andre_matias.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 1 -- Portugal
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
update sandbox_andre_matias.t_rel_scai_integration_process
set cod_status = 1, -- Ok
last_processing_datetime = isnull((select max(operation_timestamp) from sandbox_andre_matias.tmp_load_answer_step3_incoming),last_processing_datetime)
/*from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc, sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_ad'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 1
  ) source*/
from sandbox_andre_matias.t_lkp_scai_process proc 
where t_rel_scai_integration_process.cod_process = proc.cod_process
and t_rel_scai_integration_process.cod_status = 2
and t_rel_scai_integration_process.cod_country = 1
and proc.dsc_process_short = 't_fac_answer_incoming'
and t_rel_scai_integration_process.ind_active = 1
/*sandbox_andre_matias.t_rel_scai_integration_process.cod_process = source.cod_process
and sandbox_andre_matias.t_rel_scai_integration_process.cod_country = source.cod_country
and sandbox_andre_matias.t_rel_scai_integration_process.cod_integration = source.cod_integration*/;

drop table if exists sandbox_andre_matias.tmp_load_answer_step1_incoming;
drop table if exists sandbox_andre_matias.tmp_load_answer_step2_incoming;
drop table if exists sandbox_andre_matias.tmp_load_answer_step3_incoming;

	$$$
	
-- #######################
-- ####    PASSO 3    ####
-- #######################
update sandbox_andre_matias.t_rel_scai_integration_process
set dat_processing = source.dat_processing, execution_nbr = source.execution_nbr, cod_status = 2 -- Running
from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc, sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_fac_paidad_user_payment'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 1
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
  ) source
where sandbox_andre_matias.t_rel_scai_integration_process.cod_process = source.cod_process
and sandbox_andre_matias.t_rel_scai_integration_process.cod_country = source.cod_country
and sandbox_andre_matias.t_rel_scai_integration_process.cod_integration = source.cod_integration;

-- #######################
-- ####    PASSO 4    ####
-- #######################
insert into sandbox_andre_matias.t_fac_scai_execution
  select
    max_cod_exec + 1 cod_execution,
    rel_integr_proc.cod_integration,
    rel_integr_proc.cod_process,
    rel_integr_proc.cod_status,
    1 cod_execution_type, -- Begin
    rel_integr_proc.dat_processing,
    rel_integr_proc.execution_nbr,
    sysdate
  from
    sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr,
    (select isnull(max(cod_execution),0) max_cod_exec from sandbox_andre_matias.t_fac_scai_execution),
    sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc,
    sandbox_andre_matias.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 1 -- Portugal
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_fac_paidad_user_payment';

	$$$
	
-- #############################################
-- # 		     ATLAS - PORTUGAL              #
-- #    LOADING t_fac_paidad_user_payment      #
-- #############################################

drop table if exists sandbox_andre_matias.tmp_load_paidad_user_payment;

create table sandbox_andre_matias.tmp_load_paidad_user_payment 
distkey(cod_source_system)
sortkey(opr_paidad_user_payment)
as
  select source.*, isnull(lkp_paidad_index.cod_paidad_index,-1) cod_paidad_index, isnull(lkp_ad.cod_ad,-1) cod_ad, isnull(lkp_atlas_user.cod_atlas_user,-1) cod_atlas_user
		from
		(
	select
    source_table.opr_paidad_user_payment,
    --source_table.dsc_paidad_user_payment,
	source_table.operation_timestamp,
	source_table.opr_paidad_user_payment_transaction,
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
    lkp_payment_provider.cod_payment_provider,
    --lkp_paidad_index.cod_paidad_index,
		source_table.opr_paidad_index,
    --lkp_ad.cod_ad,
		source_table.opr_ad,
    --lkp_atlas_user.cod_atlas_user,
		source_table.opr_atlas_user,
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
			isnull(opr_atlas_user                          ,0) +
			isnull(opr_paidad_user_payment_transaction   ,0) +
			isnull(opr_ad                                  ,0) +
			--isnull(dsc_paidad_user_payment             ,'') +
			cast(isnull(val_price                          ,0) as varchar) +
			isnull(dat_paidad_user_payment               ,'2099-12-31 00:00:00.000000') +
			isnull(dat_valid_to                            ,'2099-12-31 00:00:00.000000') +
			isnull(opr_payment_provider                    ,'') +
			isnull(opr_paidad_index                       ,0) +
			isnull(flg_renewed                             ,0) +
			isnull(id_newsletter                           ,0) +
			cast(isnull(val_current_credits                ,0) as varchar) +
			isnull(flg_invoice_sent                        ,0) +
			isnull(flg_money_back_on_bank_account          ,0) +
			isnull(id_invoice                              ,0) +
			isnull(id_invoice_sap                          ,0) +
			isnull(flg_removed_from_invoice                ,0) +
			isnull(flg_invalid_item                        ,0) +
			isnull(flg_vas                                 ,0) +
			isnull(sap_id_invoice                          ,0) +
			isnull(migration_data                          ,'') +
			isnull(flg_migrated                            ,0)
	    ) hash_paidad_user_payment
	  FROM
	  (
            SELECT
              id opr_paidad_user_payment,
			  livesync_dbname opr_source_system,
			  operation_timestamp,
			  id_user opr_atlas_user,
			  id_transaction opr_paidad_user_payment_transaction,
			  id_ad opr_ad,
			  --name dsc_paidad_user_payment,
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
			  row_number() over (partition by id order by operation_type desc) rn
            FROM
              sandbox_andre_matias.stg_db_atlas_verticals_paidads_user_payments a,
              sandbox_andre_matias.t_lkp_source_system b
            where
              a.livesync_dbname = b.opr_source_system
              and b.cod_business_type = 1 -- Verticals
              and b.cod_country = 1 -- Portugal
			  --and 1 = 0
            union all
            SELECT
              id opr_paidad_user_payment,
			  'olxpt' opr_source_system,
			  operation_timestamp,
			  id_user opr_atlas_user,
			  id_transaction opr_paidad_user_payment_transaction,
			  id_ad opr_ad,
			  --name dsc_paidad_user_payment,
			  price val_price,
			  date dat_paidad_user_payment,
			  paidads_valid_to dat_valid_to,
			  payment_provider opr_payment_provider,
			  id_index opr_paidad_index,
			  is_renewed flg_renewed,
			  id_newsletter,
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
			  row_number() over (partition by id order by operation_type desc) rn
            FROM
              sandbox_andre_matias.stg_db_atlas_olxpt_paidads_user_payments
			--where 1 = 0
        ) source,
    sandbox_andre_matias.t_lkp_source_system lkp_source_system
	where source.opr_source_system = lkp_source_system.opr_source_system
    ) source_table,
	sandbox_andre_matias.t_lkp_payment_provider lkp_payment_provider,
    (select isnull(max(cod_paidad_user_payment),0) max_cod from sandbox_andre_matias.t_fac_paidad_user_payment) max_cod_paidad_user_payment,
    sandbox_andre_matias.t_fac_paidad_user_payment target
  where
    isnull(source_table.opr_paidad_user_payment,-1) = target.opr_paidad_user_payment(+)
	and source_table.cod_source_system = target.cod_source_system (+)
    and isnull(source_table.opr_payment_provider,'') = lkp_payment_provider.opr_payment_provider
	and lkp_payment_provider.valid_to = 20991231
	and source_table.rn = 1
) source,
	sandbox_andre_matias.t_lkp_paidad_index lkp_paidad_index,
  sandbox_andre_matias.t_lkp_ad lkp_ad,
  sandbox_andre_matias.t_lkp_atlas_user lkp_atlas_user
where
		isnull(source.opr_paidad_index,-1) = lkp_paidad_index.opr_paidad_index(+)
		and source.cod_source_system = lkp_paidad_index.cod_source_system(+) -- new
    and lkp_paidad_index.valid_to(+) = 20991231
    and isnull(source.opr_ad,-1) = lkp_ad.opr_ad(+)
		and source.cod_source_system = lkp_ad.cod_source_system(+) -- new
    and lkp_ad.valid_to(+) = 20991231
    and isnull(source.opr_atlas_user,-1) = lkp_atlas_user.opr_atlas_user (+)
		and source.cod_source_system = lkp_atlas_user.cod_source_system (+) -- new
    and lkp_atlas_user.valid_to(+) = 20991231;

	$$$
	
insert into sandbox_andre_matias.t_hst_paidad_user_payment
    select
      target.*
    from
      sandbox_andre_matias.t_fac_paidad_user_payment target,
      sandbox_andre_matias.tmp_load_paidad_user_payment source
    where
      target.opr_paidad_user_payment = source.opr_paidad_user_payment
      and source.dml_type = 'U';

	$$$
	
delete from sandbox_andre_matias.t_fac_paidad_user_payment
using sandbox_andre_matias.tmp_load_paidad_user_payment
where sandbox_andre_matias.t_fac_paidad_user_payment.opr_paidad_user_payment=sandbox_andre_matias.tmp_load_paidad_user_payment.opr_paidad_user_payment
and sandbox_andre_matias.tmp_load_paidad_user_payment.dml_type = 'U';

	$$$
	
insert into sandbox_andre_matias.t_fac_paidad_user_payment
    select
      case
        when dml_type = 'I' then max_cod + new_cod
        when dml_type = 'U' then cod_paidad_user_payment
      end cod_paidad_user_payment,
	  dat_paidad_user_payment,
	  opr_paidad_user_payment,
	  --dsc_paidad_user_payment,
	  opr_paidad_user_payment_transaction,
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
	  hash_paidad_user_payment
    from
      sandbox_andre_matias.tmp_load_paidad_user_payment
    where
      dml_type in ('U','I');

	$$$
	
-- #######################
-- ####    PASSO 5    ####
-- #######################
insert into sandbox_andre_matias.t_fac_scai_execution
  select
    max_cod_exec + 1 cod_execution,
    rel_integr_proc.cod_integration,
    rel_integr_proc.cod_process,
    1 cod_status,
    2 cod_execution_type, -- End
    rel_integr_proc.dat_processing,
    rel_integr_proc.execution_nbr,
    sysdate
  from
    sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr,
    (select isnull(max(cod_execution),0) max_cod_exec from sandbox_andre_matias.t_fac_scai_execution),
    sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc,
    sandbox_andre_matias.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 1 -- Portugal
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
update sandbox_andre_matias.t_rel_scai_integration_process
set cod_status = 1, -- Ok
last_processing_datetime = isnull((select max(operation_timestamp) from sandbox_andre_matias.tmp_load_paidad_user_payment),last_processing_datetime)
/*from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc, sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_fac_paidad_user_payment'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 1
  ) source*/
from sandbox_andre_matias.t_lkp_scai_process proc 
where t_rel_scai_integration_process.cod_process = proc.cod_process
and t_rel_scai_integration_process.cod_status = 2
and t_rel_scai_integration_process.cod_country = 1
and proc.dsc_process_short = 't_fac_paidad_user_payment'
and t_rel_scai_integration_process.ind_active = 1
/*sandbox_andre_matias.t_rel_scai_integration_process.cod_process = source.cod_process
and sandbox_andre_matias.t_rel_scai_integration_process.cod_country = source.cod_country
and sandbox_andre_matias.t_rel_scai_integration_process.cod_integration = source.cod_integration*/;

drop table if exists sandbox_andre_matias.tmp_load_paidad_user_payment;

	$$$
	
-- #######################
-- ####    PASSO 3    ####
-- #######################
update sandbox_andre_matias.t_rel_scai_integration_process
set dat_processing = source.dat_processing, execution_nbr = source.execution_nbr, cod_status = 2 -- Running
from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc, sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_fac_payment_session'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 1
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
  ) source
where sandbox_andre_matias.t_rel_scai_integration_process.cod_process = source.cod_process
and sandbox_andre_matias.t_rel_scai_integration_process.cod_country = source.cod_country
and sandbox_andre_matias.t_rel_scai_integration_process.cod_integration = source.cod_integration;

-- #######################
-- ####    PASSO 4    ####
-- #######################
insert into sandbox_andre_matias.t_fac_scai_execution
  select
    max_cod_exec + 1 cod_execution,
    rel_integr_proc.cod_integration,
    rel_integr_proc.cod_process,
    rel_integr_proc.cod_status,
    1 cod_execution_type, -- Begin
    rel_integr_proc.dat_processing,
    rel_integr_proc.execution_nbr,
    sysdate
  from
    sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr,
    (select isnull(max(cod_execution),0) max_cod_exec from sandbox_andre_matias.t_fac_scai_execution),
    sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc,
    sandbox_andre_matias.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 1 -- Portugal
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_fac_payment_session';

	$$$
	
-- #############################################
-- # 		     ATLAS - PORTUGAL              #
-- #       LOADING t_fac_payment_session       #
-- #############################################

drop table if exists sandbox_andre_matias.tmp_load_payment_session;

create table sandbox_andre_matias.tmp_load_payment_session 
distkey(cod_source_system)
sortkey(opr_payment_session)
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
    lkp_payment_provider.cod_payment_provider,
    lkp_payment_status.cod_payment_status,
    lkp_source.cod_source,
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
			--isnull(created_at                            ,'2099-12-31 00:00:00.000000') +
			--isnull(last_status_date                      ,'2099-12-31 00:00:00.000000') +
			isnull(ip                                      ,0) +
			isnull(opr_payment_status                      ,'') +
			isnull(opr_payment_provider                    ,'') +
			isnull(external_id                             ,0) +
			isnull(request                                 ,'')+
			isnull(message                                 ,'') +
			isnull(opr_source                              ,'') +
			isnull(additional_data                         ,'') +
			isnull(migration_data                          ,'') +
			isnull(flg_migrated                            ,0)
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
				row_number() over (partition by id order by operation_type desc) rn
            FROM
				sandbox_andre_matias.stg_db_atlas_verticals_payment_session a,
				sandbox_andre_matias.t_lkp_source_system b
            where
				a.livesync_dbname = b.opr_source_system
				and b.cod_business_type = 1 -- Verticals
				and b.cod_country = 1 -- Portugal
				--and 1 = 0
            union all
			SELECT
				id opr_payment_session,
				'olxpt' opr_source_system,
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
				row_number() over (partition by id order by operation_type desc) rn
			FROM
				sandbox_andre_matias.stg_db_atlas_olxpt_payment_session
			--where 1 = 0
        ) source,
    sandbox_andre_matias.t_lkp_source_system lkp_source_system
	where source.opr_source_system = lkp_source_system.opr_source_system
    ) source_table,
    sandbox_andre_matias.t_lkp_source lkp_source,
    sandbox_andre_matias.t_lkp_payment_status lkp_payment_status,
	sandbox_andre_matias.t_lkp_payment_provider lkp_payment_provider,
    (select isnull(max(cod_payment_session),0) max_cod from sandbox_andre_matias.t_fac_payment_session) max_cod_payment_session,
    sandbox_andre_matias.t_fac_payment_session target
  where
    isnull(source_table.opr_payment_session,-1) = target.opr_payment_session(+)
	and source_table.cod_source_system = target.cod_source_system (+)
    and isnull(source_table.opr_payment_provider,'') = lkp_payment_provider.opr_payment_provider
	and lkp_payment_provider.valid_to = 20991231
    and isnull(source_table.opr_payment_status,'') = lkp_payment_status.opr_payment_status
    and lkp_payment_status.valid_to = 20991231
    and isnull(source_table.opr_source,'') = lkp_source.opr_source
    and lkp_source.valid_to = 20991231
	and source_table.rn = 1;

	$$$
	
insert into sandbox_andre_matias.t_hst_payment_session
    select
      target.*
    from
      sandbox_andre_matias.t_fac_payment_session target,
      sandbox_andre_matias.tmp_load_payment_session source
    where
      target.opr_payment_session = source.opr_payment_session
      and source.dml_type = 'U';

	$$$
	
delete from sandbox_andre_matias.t_fac_payment_session
using sandbox_andre_matias.tmp_load_payment_session
where sandbox_andre_matias.t_fac_payment_session.opr_payment_session=sandbox_andre_matias.tmp_load_payment_session.opr_payment_session
and sandbox_andre_matias.tmp_load_payment_session.dml_type = 'U';

	$$$
	
insert into sandbox_andre_matias.t_fac_payment_session
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
	  hash_payment_session
    from
      sandbox_andre_matias.tmp_load_payment_session
    where
      dml_type in ('U','I');

	$$$
	
-- #######################
-- ####    PASSO 5    ####
-- #######################
insert into sandbox_andre_matias.t_fac_scai_execution
  select
    max_cod_exec + 1 cod_execution,
    rel_integr_proc.cod_integration,
    rel_integr_proc.cod_process,
    1 cod_status,
    2 cod_execution_type, -- End
    rel_integr_proc.dat_processing,
    rel_integr_proc.execution_nbr,
    sysdate
  from
    sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr,
    (select isnull(max(cod_execution),0) max_cod_exec from sandbox_andre_matias.t_fac_scai_execution),
    sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc,
    sandbox_andre_matias.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 1 -- Portugal
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
update sandbox_andre_matias.t_rel_scai_integration_process
set cod_status = 1, -- Ok
last_processing_datetime = isnull((select max(operation_timestamp) from sandbox_andre_matias.tmp_load_payment_session),last_processing_datetime)
/*from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc, sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_fac_payment_session'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 1
  ) source*/
from sandbox_andre_matias.t_lkp_scai_process proc 
where t_rel_scai_integration_process.cod_process = proc.cod_process
and t_rel_scai_integration_process.cod_status = 2
and t_rel_scai_integration_process.cod_country = 1
and proc.dsc_process_short = 't_fac_payment_session'
and t_rel_scai_integration_process.ind_active = 1
/*sandbox_andre_matias.t_rel_scai_integration_process.cod_process = source.cod_process
and sandbox_andre_matias.t_rel_scai_integration_process.cod_country = source.cod_country
and sandbox_andre_matias.t_rel_scai_integration_process.cod_integration = source.cod_integration*/;

drop table if exists sandbox_andre_matias.tmp_load_payment_session;

	$$$
	
-- #######################
-- ####    PASSO 3    ####
-- #######################
update sandbox_andre_matias.t_rel_scai_integration_process
set dat_processing = source.dat_processing, execution_nbr = source.execution_nbr, cod_status = 2 -- Running
from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc, sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_fac_payment_basket'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 1
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
  ) source
where sandbox_andre_matias.t_rel_scai_integration_process.cod_process = source.cod_process
and sandbox_andre_matias.t_rel_scai_integration_process.cod_country = source.cod_country
and sandbox_andre_matias.t_rel_scai_integration_process.cod_integration = source.cod_integration;

-- #######################
-- ####    PASSO 4    ####
-- #######################
insert into sandbox_andre_matias.t_fac_scai_execution
  select
    max_cod_exec + 1 cod_execution,
    rel_integr_proc.cod_integration,
    rel_integr_proc.cod_process,
    rel_integr_proc.cod_status,
    1 cod_execution_type, -- Begin
    rel_integr_proc.dat_processing,
    rel_integr_proc.execution_nbr,
    sysdate
  from
    sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr,
    (select isnull(max(cod_execution),0) max_cod_exec from sandbox_andre_matias.t_fac_scai_execution),
    sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc,
    sandbox_andre_matias.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 1 -- Portugal
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_fac_payment_basket';

	$$$
	
-- #############################################
-- # 		     ATLAS - PORTUGAL              #
-- #       LOADING t_fac_payment_basket        #
-- #############################################

drop table if exists sandbox_andre_matias.tmp_load_payment_basket;

create table sandbox_andre_matias.tmp_load_payment_basket 
distkey(cod_source_system)
sortkey(opr_payment_basket)
as
  select source.*,
    isnull(lkp_payment_session.cod_payment_session,-1) cod_payment_session,
    isnull(lkp_paidad_index.cod_paidad_index,-1) cod_paidad_index,
    isnull(lkp_ad.cod_ad,-1) cod_ad,
    isnull(lkp_atlas_user.cod_atlas_user,-1) cod_atlas_user
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
        (isnull(opr_payment_session,0) + isnull(opr_payment_index,0) + isnull(opr_ad,0) + isnull(opr_atlas_user,0)) hash_payment_basket
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
              row_number() over (partition by id order by operation_type desc) rn
            FROM
              sandbox_andre_matias.stg_db_atlas_verticals_payment_basket a,
              sandbox_andre_matias.t_lkp_source_system b
            where
              a.livesync_dbname = b.opr_source_system
              and b.cod_business_type = 1 -- Verticals
              and b.cod_country = 1 -- Portugal
			  --and 1 = 0
    union all
    SELECT
        id opr_payment_basket,
        'olxpt' opr_source_system,
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
        cast(null as numeric(8,2)) from_bonus_credits,
        cast(null as numeric(8,2)) from_refund_credits,
        row_number() over (partition by id order by operation_type desc) rn
    FROM
        sandbox_andre_matias.stg_db_atlas_olxpt_payment_basket
	--where 1 = 0
        ) source,
    sandbox_andre_matias.t_lkp_source_system lkp_source_system
	where source.opr_source_system = lkp_source_system.opr_source_system
    ) source_table,
    (select isnull(max(cod_payment_basket),0) max_cod from sandbox_andre_matias.t_fac_payment_basket) max_cod_payment_basket,
    sandbox_andre_matias.t_fac_payment_basket target
  where
    isnull(source_table.opr_payment_basket,-1) = target.opr_payment_basket(+)
	and source_table.cod_source_system = target.cod_source_system (+)
	and source_table.rn = 1
) source,
    sandbox_andre_matias.t_fac_payment_session lkp_payment_session,
    sandbox_andre_matias.t_lkp_paidad_index lkp_paidad_index,
    sandbox_andre_matias.t_lkp_ad lkp_ad,
    sandbox_andre_matias.t_lkp_atlas_user lkp_atlas_user
  where
    isnull(source.opr_payment_session,-1) = lkp_payment_session.opr_payment_session (+)
	  and source.cod_source_system = lkp_payment_session.cod_source_system (+) -- new
    and isnull(source.opr_payment_index,-1) = lkp_paidad_index.opr_paidad_index (+)
	  and source.cod_source_system = lkp_paidad_index.cod_source_system (+) -- new
    and lkp_paidad_index.valid_to (+) = 20991231
    and isnull(source.opr_ad,-1) = lkp_ad.opr_ad (+)
	  and source.cod_source_system = lkp_ad.cod_source_system (+) -- new
    and lkp_ad.valid_to (+) = 20991231
    and isnull(source.opr_atlas_user,-1) = lkp_atlas_user.opr_atlas_user (+)
	  and source.cod_source_system = lkp_atlas_user.cod_source_system (+) -- new
    and lkp_atlas_user.valid_to (+) = 20991231;

	$$$
	
insert into sandbox_andre_matias.t_hst_payment_basket
    select
      target.*
    from
      sandbox_andre_matias.t_fac_payment_basket target,
      sandbox_andre_matias.tmp_load_payment_basket source
    where
      target.opr_payment_basket = source.opr_payment_basket
      and source.dml_type = 'U';

	$$$
	
delete from sandbox_andre_matias.t_fac_payment_basket
using sandbox_andre_matias.tmp_load_payment_basket
where sandbox_andre_matias.t_fac_payment_basket.opr_payment_basket=sandbox_andre_matias.tmp_load_payment_basket.opr_payment_basket
and sandbox_andre_matias.tmp_load_payment_basket.dml_type = 'U';

	$$$
	
insert into sandbox_andre_matias.t_fac_payment_basket
    select
      case
        when dml_type = 'I' then max_cod + new_cod
        when dml_type = 'U' then cod_payment_basket
      end cod_lead,
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
      hash_payment_basket
    from
      sandbox_andre_matias.tmp_load_payment_basket
    where
      dml_type in ('U','I');

	$$$
	
-- #######################
-- ####    PASSO 5    ####
-- #######################
insert into sandbox_andre_matias.t_fac_scai_execution
  select
    max_cod_exec + 1 cod_execution,
    rel_integr_proc.cod_integration,
    rel_integr_proc.cod_process,
    1 cod_status,
    2 cod_execution_type, -- End
    rel_integr_proc.dat_processing,
    rel_integr_proc.execution_nbr,
    sysdate
  from
    sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr,
    (select isnull(max(cod_execution),0) max_cod_exec from sandbox_andre_matias.t_fac_scai_execution),
    sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc,
    sandbox_andre_matias.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 1 -- Portugal
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
update sandbox_andre_matias.t_rel_scai_integration_process
set cod_status = 1, -- Ok
last_processing_datetime = isnull((select max(operation_timestamp) from sandbox_andre_matias.tmp_load_payment_basket),last_processing_datetime)
/*from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc, sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_fac_payment_basket'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 1
  ) source*/
from sandbox_andre_matias.t_lkp_scai_process proc 
where t_rel_scai_integration_process.cod_process = proc.cod_process
and t_rel_scai_integration_process.cod_status = 2
and t_rel_scai_integration_process.cod_country = 1
and proc.dsc_process_short = 't_fac_payment_basket'
and t_rel_scai_integration_process.ind_active = 1
/*sandbox_andre_matias.t_rel_scai_integration_process.cod_process = source.cod_process
and sandbox_andre_matias.t_rel_scai_integration_process.cod_country = source.cod_country
and sandbox_andre_matias.t_rel_scai_integration_process.cod_integration = source.cod_integration*/;

drop table if exists sandbox_andre_matias.tmp_load_payment_basket;

	$$$
	
-- #######################
-- ####    PASSO 3    ####
-- #######################
update sandbox_andre_matias.t_rel_scai_integration_process
set dat_processing = source.dat_processing, execution_nbr = source.execution_nbr, cod_status = 2 -- Running
from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc, sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_fac_web'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 1
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
  ) source
where sandbox_andre_matias.t_rel_scai_integration_process.cod_process = source.cod_process
and sandbox_andre_matias.t_rel_scai_integration_process.cod_country = source.cod_country
and sandbox_andre_matias.t_rel_scai_integration_process.cod_integration = source.cod_integration;

-- #######################
-- ####    PASSO 4    ####
-- #######################
insert into sandbox_andre_matias.t_fac_scai_execution
  select
    max_cod_exec + 1 cod_execution,
    rel_integr_proc.cod_integration,
    rel_integr_proc.cod_process,
    rel_integr_proc.cod_status,
    1 cod_execution_type, -- Begin
    rel_integr_proc.dat_processing,
    rel_integr_proc.execution_nbr,
    sysdate
  from
    sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr,
    (select isnull(max(cod_execution),0) max_cod_exec from sandbox_andre_matias.t_fac_scai_execution),
    sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc,
    sandbox_andre_matias.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 1 -- Portugal
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_fac_web';

	$$$
	
-- #############################################
-- # 		 HYDRA - PORTUGAL                  #
-- #       LOADING t_fac_web                   #
-- #############################################

drop table if exists sandbox_andre_matias.tmp_load_web;

create table sandbox_andre_matias.tmp_load_web 
distkey(cod_source_system)
sortkey(cod_ad, cod_event, dat_event, cod_source_system)
as
  select
    source_table.server_date_day dat_event,
	source_table.server_date_day_datetime,
    source_table.cod_source_system,
    nvl(lkp_ad.cod_ad,-1) cod_ad,
    nvl(lkp_event.cod_event,-1) cod_event,
    source_table.occurrences,
    source_table.distinct_occurrences,
    source_table.hash_web,
    case
      when target.cod_ad is null or target.dat_event is null or target.cod_event is null then 'I'
      when source_table.hash_web != target.hash_web then 'U'
       else 'X'
    end dml_type
  from
    (
      select
        *,
        md5(isnull(occurrences,0) + isnull(distinct_occurrences,0)) hash_web
      from
        (
          SELECT
            cast(to_char(a.server_date_day,'yyyymmdd') as int) server_date_day,
			server_date_day server_date_day_datetime,
            a.ad_id opr_ad,
            a.trackname opr_event,
            a.occurrences,
            a.distinct_occurrences,
            b.cod_source_system
          FROM
            sandbox_andre_matias.stg_hydra_verticals_web a,
            sandbox_andre_matias.t_lkp_source_system b
          where
            a.source = b.opr_source_system
            and b.cod_business_type = 1 -- Verticals
            and b.cod_country = 1 -- Portugal
			--and 1 = 0
          union all
          SELECT
            cast(to_char(a.server_date_day,'yyyymmdd') as int) server_date_day,
			server_date_day server_date_day_datetime,
            a.ad_id opr_ad,
            a.action_type opr_event,
            a.occurrences,
            a.distinct_occurrences,
            b.cod_source_system
          FROM
            sandbox_andre_matias.stg_hydra_web a,
            sandbox_andre_matias.t_lkp_source_system b
          where
            a.source = b.opr_source_system
            and b.cod_business_type = 2 -- Horizontal
            and b.cod_country = 1 -- Portugal
			--and 1 = 0
        )
    ) source_table,
    sandbox_andre_matias.t_lkp_event lkp_event,
    sandbox_andre_matias.t_lkp_ad lkp_ad,
    sandbox_andre_matias.t_fac_web target
  where
    isnull(source_table.server_date_day,-1) = target.dat_event(+)
    and isnull(source_table.opr_ad,-1) = lkp_ad.opr_ad(+)
	and source_table.cod_source_system = lkp_ad.cod_source_system(+)
    and lkp_ad.valid_to(+) = 20991231
    and isnull(source_table.opr_event,'Unknown') = lkp_event.opr_event(+)
    and lkp_event.valid_to(+) = 20991231;

	$$$
	
insert into sandbox_andre_matias.t_hst_web
    select
      target.*
    from
      sandbox_andre_matias.t_fac_web target,
      sandbox_andre_matias.tmp_load_web source
    where
      target.cod_ad = source.cod_ad
      and target.dat_event = source.dat_event
      and target.cod_event = source.cod_event
      and target.cod_source_system = source.cod_source_system
      and source.dml_type = 'U';

	$$$
	
delete from sandbox_andre_matias.t_fac_web
using sandbox_andre_matias.tmp_load_web
where sandbox_andre_matias.t_fac_web.cod_ad = sandbox_andre_matias.tmp_load_web.cod_ad
and sandbox_andre_matias.t_fac_web.dat_event = sandbox_andre_matias.tmp_load_web.dat_event
and sandbox_andre_matias.t_fac_web.cod_event = sandbox_andre_matias.tmp_load_web.cod_event
and sandbox_andre_matias.t_fac_web.cod_source_system = sandbox_andre_matias.tmp_load_web.cod_source_system
and sandbox_andre_matias.tmp_load_web.dml_type = 'U';

	$$$
	
insert into sandbox_andre_matias.t_fac_web
    select
      cod_ad,
      dat_event,
      cod_event,
      cod_source_system,
      occurrences,
      distinct_occurrences,
      hash_web
    from
      sandbox_andre_matias.tmp_load_web
    where
      dml_type in ('U','I');

-- #######################
-- ####    PASSO 5    ####
-- #######################
insert into sandbox_andre_matias.t_fac_scai_execution
  select
    max_cod_exec + 1 cod_execution,
    rel_integr_proc.cod_integration,
    rel_integr_proc.cod_process,
    1 cod_status,
    2 cod_execution_type, -- End
    rel_integr_proc.dat_processing,
    rel_integr_proc.execution_nbr,
    sysdate
  from
    sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr,
    (select isnull(max(cod_execution),0) max_cod_exec from sandbox_andre_matias.t_fac_scai_execution),
    sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc,
    sandbox_andre_matias.t_lkp_scai_process proc
  where
    rel_country_integr.cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and rel_country_integr.cod_country = 1 -- Portugal
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
update sandbox_andre_matias.t_rel_scai_integration_process
set cod_status = 1, -- Ok
last_processing_datetime = isnull((select max(server_date_day_datetime) from sandbox_andre_matias.tmp_load_web),last_processing_datetime)
/*from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc, sandbox_andre_matias.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_fac_payment_basket'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 1
  ) source*/
from sandbox_andre_matias.t_lkp_scai_process proc 
where t_rel_scai_integration_process.cod_process = proc.cod_process
and t_rel_scai_integration_process.cod_status = 2
and t_rel_scai_integration_process.cod_country = 1
and proc.dsc_process_short = 't_fac_web'
and t_rel_scai_integration_process.ind_active = 1
/*sandbox_andre_matias.t_rel_scai_integration_process.cod_process = source.cod_process
and sandbox_andre_matias.t_rel_scai_integration_process.cod_country = source.cod_country
and sandbox_andre_matias.t_rel_scai_integration_process.cod_integration = source.cod_integration*/;

drop table if exists sandbox_andre_matias.tmp_load_web;

	$$$
	
-- #######################
-- ####    PASSO 7    ####
-- #######################
insert into sandbox_andre_matias.t_fac_scai_execution
  select
    max_cod_exec + 1 cod_execution,
    cod_integration,
    -1 cod_process,
    1 cod_status, -- Ok
    2 cod_execution_type, -- End
    dat_processing,
    execution_nbr,
    sysdate
  from
    sandbox_andre_matias.t_rel_scai_country_integration,
    (select isnull(max(cod_execution),0) max_cod_exec from sandbox_andre_matias.t_fac_scai_execution)
  where
    cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and cod_country = 1
	and ind_active = 1; -- Portugal

-- #######################
-- ####    PASSO 8    ####
-- #######################
update sandbox_andre_matias.t_rel_scai_country_integration
    set
      cod_status = 1 -- Ok
where
    cod_integration = 30000 -- Chandra (Operational) to Chandra (Analytical)
    and cod_country = 1
	and ind_active = 1; -- Portugal