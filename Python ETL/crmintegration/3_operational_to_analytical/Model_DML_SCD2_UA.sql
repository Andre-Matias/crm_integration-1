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
    and rel_integr_proc.cod_country = 3
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
    and rel_country_integr.cod_country = 3 -- Ukraine
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_lkp_base_user';	

--$$$
	
-- #############################################
-- # 			 BASE - Ukraine                #
-- #		 LOADING t_lkp_base_user           #
-- #############################################

create temp table tmp_ua_load_base_user
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
	null cod_base_user_responsible,
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
        null dsc_base_user,
        'uahorizontal' opr_source_system,
		null meta_event_type,
		null meta_event_time,
        null email,
        null role,
        null status,
        null flg_confirmed,
		null flg_invited,
		null phone_number,
		null roles,
		null team_name,
		null "group",
		id_boss reports_to,
		null timezone,
        null created_at,
        null updated_at,
        null deleted_at,
        scai_execution.cod_execution,
        scai_execution.dat_processing
      FROM
        crm_integration_stg.stg_ua_sales_reps,
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
            and rel_integr_proc.cod_country = 3
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
	and lkp_source_system.cod_country = 3
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
	and source_table.cod_source_system = target_base_user_responsible.cod_source_system (+); -- Ukraine

analyze tmp_ua_load_base_user;


delete from crm_integration_anlt.t_lkp_base_user
using tmp_ua_load_base_user
where 
	tmp_ua_load_base_user.dml_type = 'I' 
	and t_lkp_base_user.opr_base_user = tmp_ua_load_base_user.opr_base_user 
	and t_lkp_base_user.valid_from = (select dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 3 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_base_user');



-- update valid_to in the updated/deleted records on source	
update crm_integration_anlt.t_lkp_base_user
set valid_to = (select rel_integr_proc.dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 3 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_base_user') 
from tmp_ua_load_base_user source
where source.cod_base_user = crm_integration_anlt.t_lkp_base_user.cod_base_user
and crm_integration_anlt.t_lkp_base_user.valid_to = 20991231
and source.dml_type in('U','D');



insert into crm_integration_anlt.t_lkp_base_user
    select
      case
        when dml_type = 'I' then case when valid_from = (select dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc
														where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 3 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_base_user')
									then cod_base_user else max_cod + new_cod end
        when dml_type = 'U' then cod_base_user
      end cod_base_user,
      opr_base_user,
      dsc_base_user,
      cod_source_system,
      (select rel_integr_proc.dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 3 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_base_user') valid_from, 
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
      tmp_ua_load_base_user
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
    and rel_country_integr.cod_country = 3 -- Ukraine
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
last_processing_datetime = coalesce((select max(updated_at) from tmp_ua_load_base_user),last_processing_datetime)
/*from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc, crm_integration_anlt.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_base_user'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 3
  ) source*/
from crm_integration_anlt.t_lkp_scai_process proc 
where t_rel_scai_integration_process.cod_process = proc.cod_process
and t_rel_scai_integration_process.cod_status = 2
and t_rel_scai_integration_process.cod_country = 3
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
    where proc.dsc_process_short = 't_lkp_contact'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 3
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
    and rel_country_integr.cod_country = 3 -- Ukraine
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_lkp_contact';	

--$$$
	
-- #############################################
-- # 		     BASE - Ukraine                #
-- #           LOADING t_lkp_contact           #
-- #############################################


--Create temporary table with non-Company Contacts first
create temp table tmp_ua_load_contact 
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
		id_user opr_contact,
		null dsc_contact,
		'uahorizontal' opr_source_system, opr_source_system,
		null meta_event_type,
		null meta_event_time,
		null creator_id opr_base_user_creator,
		id_company contact_id,
		null created_at,
		null updated_at,
		null title,
		null first_name,
		null last_name,
		null description,
		null opr_industry,
		null website,
		null email,
		null phone,
		null mobile,
		null fax,
		null twitter,
		null facebook,
		null linkedin,
		null skype,
		id_manager opr_base_user_owner,
		0 flg_organization,
		null address,
		null custom_fields,
		null customer_status,
		null prospect_status,
		null tags,
		scai_execution.cod_execution,
		scai_execution.dat_processing
      FROM
        crm_integration_stg.stg_ua_users_companies,
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
            and rel_integr_proc.cod_country = 3
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
	and lkp_source_system.cod_country = 3 -- Ukraine
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


--Insert Companies into temporary table
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
		id_company opr_contact,
		null dsc_contact,
		'uahorizontal' opr_source_system,
		null meta_event_type,
		null meta_event_time,
		null creator_id opr_base_user_creator,
		null contact_id,
		null created_at,
		null updated_at,
		null title,
		null first_name,
		null last_name,
		null description,
		null opr_industry,
		null website,
		null email,
		null phone,
		null mobile,
		null fax,
		null twitter,
		null facebook,
		null linkedin,
		null skype,
		-1 opr_base_user_owner,
		1 flg_organization,
		null address,
		null custom_fields,
		null customer_status,
		null prospect_status,
		null tags,
		scai_execution.cod_execution,
		scai_execution.dat_processing
      FROM
        crm_integration_stg.stg_ua_users_companies,
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
            and rel_integr_proc.cod_country = 3
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
	and lkp_source_system.cod_country = 3 -- Ukraine
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

analyze tmp_ua_load_contact;
	
delete from crm_integration_anlt.t_lkp_contact
using tmp_ua_load_contact
where 
	tmp_ua_load_contact.dml_type = 'I' 
	and t_lkp_contact.opr_contact = tmp_ua_load_contact.opr_contact 
	and t_lkp_contact.valid_from = (select dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 3 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_contact');


	
-- update valid_to in the updated/deleted records on source	
update crm_integration_anlt.t_lkp_contact
set valid_to = (select rel_integr_proc.dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 3 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_contact') 
from tmp_ua_load_contact source
where source.cod_contact = crm_integration_anlt.t_lkp_contact.cod_contact
and crm_integration_anlt.t_lkp_contact.valid_to = 20991231
and source.dml_type in('U','D');


	
insert into crm_integration_anlt.t_lkp_contact
    select
      case
        when dml_type = 'I' then case when valid_from = (select dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc
														where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 3 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_contact')
									then cod_contact else max_cod + new_cod end
        when dml_type = 'U' then cod_contact
      end cod_contact,
      opr_contact,
      dsc_contact,
      cod_source_system,
      (select rel_integr_proc.dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 3 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_contact') valid_from, 
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
      tmp_ua_load_contact
    where
      dml_type in ('U','I');



analyze crm_integration_anlt.t_lkp_contact;
	   

-- update do contact_id/cod_contact_parent - OLX UA
update crm_integration_anlt.t_lkp_contact
set cod_contact_parent = contact_parent.cod_contact
from
(
select * from crm_integration_anlt.t_lkp_contact
where cod_source_system = 23
and cod_contact_parent is null
) contact_parent
where t_lkp_contact.cod_contact_parent = contact_parent.opr_contact
and t_lkp_contact.cod_source_system = contact_parent.cod_source_system;
	   
	
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
    and rel_country_integr.cod_country = 3 -- Ukraine
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
last_processing_datetime = coalesce((select max(updated_at) from tmp_ua_load_contact),last_processing_datetime)
/*from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc, crm_integration_anlt.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_contact'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 3
  ) source*/
from crm_integration_anlt.t_lkp_scai_process proc 
where t_rel_scai_integration_process.cod_process = proc.cod_process
and t_rel_scai_integration_process.cod_status = 2
and t_rel_scai_integration_process.cod_country = 3
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
    where proc.dsc_process_short = 't_fac_call'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 3
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
    and rel_country_integr.cod_country = 3 -- Ukraine
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_fac_call';	

--$$$
	
-- #############################################
-- # 	          BASE - Ukraine              #
-- #           LOADING t_fac_call              #
-- #############################################


create temp table tmp_ua_load_calls 
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
            'uahorizontal' opr_source_system,
            md5(id_user || record_link) opr_call,
            null opr_resource,
            id_manager opr_base_user,
            null phone_number,
            case
				when is_answered = 0 then 1
				when is_answered = 1 then 0
				else -1
			end flg_missed,
            null opr_associated_deal,
            null created_at,
            null updated_at,
            null summary,
            null opr_call_outcome,
            talk_time call_duration,
            case
				when direction = 'Outgoing' then 0
				when direction = 'Incoming' then 1
				else -1
			end flg_incoming,
            record_link recording_url,
            'contact' opr_resource_type,
            b.cod_source_system,
            row_number() over (partition by id order by meta_event_type desc) rn,
            scai_execution.cod_execution,
            scai_execution.dat_processing
          from
            crm_integration_stg.stg_ua_users_calls a,
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
                and rel_integr_proc.cod_country = 3
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
            'uahorizontal' = opr_source_system
            and cod_country = 3
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

analyze tmp_ua_load_calls;
	

	
insert into crm_integration_anlt.t_hst_call
    select
      target.*
    from
      crm_integration_anlt.t_fac_call target,
      tmp_ua_load_calls source
    where
      target.opr_call = source.opr_call
      and source.dml_type = 'U';


	
delete from crm_integration_anlt.t_fac_call
using tmp_ua_load_calls
where crm_integration_anlt.t_fac_call.opr_call = tmp_ua_load_calls.opr_call
and tmp_ua_load_calls.dml_type = 'U';


	
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
      tmp_ua_load_calls
    where
      dml_type in ('U','I');



analyze crm_integration_anlt.t_fac_call;
	  
	
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
    and rel_country_integr.cod_country = 3 -- Ukraine
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
last_processing_datetime = coalesce((select max(updated_at) from tmp_ua_load_calls),last_processing_datetime)
/*from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc, crm_integration_anlt.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_fac_call'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 3
  ) source*/
from crm_integration_anlt.t_lkp_scai_process proc 
where t_rel_scai_integration_process.cod_process = proc.cod_process
and t_rel_scai_integration_process.cod_status = 2
and t_rel_scai_integration_process.cod_country = 3
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
    where proc.dsc_process_short = 't_lkp_paidad_index'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 3
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
    and rel_country_integr.cod_country = 3 -- Ukraine
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_lkp_paidad_index';	

--$$$
	
-- #############################################
-- # 		     ATLAS - Ukraine               #
-- #		LOADING t_lkp_paidad_index  	   #
-- #############################################


create temp table tmp_ua_load_paidad_index 
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
	  when target.cod_paidad_index is null or (source_table.hash_paidad_index != target.hash_paidad_index and target.valid_from = (select dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 3 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_paidad_index')) then 'I'
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
           crm_integration_stg.stg_ua_db_atlas_verticals_paidads_indexes a,
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
              and rel_integr_proc.cod_country = 3
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
           and b.cod_country = 3 -- Ukraine
		   --and 1 = 0
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
           null name_pt,
           null name_ro,
           name_ru,
           name_uk,
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
          crm_integration_stg.stg_ua_db_atlas_olxua_paidads_indexes,
          (
            select
              max(fac.cod_execution) cod_execution
            from
              crm_integration_anlt.t_lkp_scai_process proc,
              crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
              crm_integration_anlt.t_fac_scai_execution fac
            where
              rel_integr_proc.cod_process = proc.cod_process
              and rel_integr_proc.cod_country = 3
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

analyze tmp_ua_load_paidad_index;
	

	
delete from crm_integration_anlt.t_lkp_paidad_index
using tmp_ua_load_paidad_index
where 
	tmp_ua_load_paidad_index.dml_type = 'I' 
	and t_lkp_paidad_index.opr_paidad_index = tmp_ua_load_paidad_index.opr_paidad_index
	and t_lkp_paidad_index.valid_from = (select dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 3 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_paidad_index');


	
update crm_integration_anlt.t_lkp_paidad_index
set valid_to = (select rel_integr_proc.dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 3 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_paidad_index') 
from tmp_ua_load_paidad_index source
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
      (select rel_integr_proc.dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 3 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_paidad_index') valid_from, 
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
      tmp_ua_load_paidad_index
    where
      dml_type in ('U','I');



analyze crm_integration_anlt.t_lkp_paidad_index;
	  

	
update crm_integration_anlt.t_lkp_paidad_index
set cod_paidad_index_related = lkp.cod_paidad_index
from tmp_ua_load_paidad_index source, crm_integration_anlt.t_lkp_paidad_index lkp, crm_integration_anlt.t_lkp_source_system ss
where coalesce(source.opr_paidad_index_related,-1) = lkp.opr_paidad_index
and source.cod_source_system = ss.cod_source_system
and ss.cod_country = 3
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
    and rel_country_integr.cod_country = 3 -- Ukraine
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
last_processing_datetime = coalesce((select max(operation_timestamp) from tmp_ua_load_paidad_index),last_processing_datetime)
/*from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc, crm_integration_anlt.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_paidad_index'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 3
  ) source*/
from crm_integration_anlt.t_lkp_scai_process proc 
where t_rel_scai_integration_process.cod_process = proc.cod_process
and t_rel_scai_integration_process.cod_status = 2
and t_rel_scai_integration_process.cod_country = 3
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
    where proc.dsc_process_short = 't_lkp_atlas_user'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 3
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
    and rel_country_integr.cod_country = 3 -- Ukraine
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
	and rel_country_integr.ind_active = 1
	and rel_integr_proc.ind_active = 1
	and proc.dsc_process_short = 't_lkp_atlas_user';

--$$$
	
-- #############################################
-- # 		      ATLAS - Ukraine              #
-- #	      LOADING t_lkp_atlas_user     	   #
-- #############################################


create temp table tmp_ua_load_atlas_user 
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
	  when target.cod_atlas_user is null or (source_table.hash_atlas_user != target.hash_atlas_user and target.valid_from = (select dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 3 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_atlas_user')) then 'I'
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
        crm_integration_stg.stg_ua_db_atlas_verticals_users a,
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
            and rel_integr_proc.cod_country = 3
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
      and b.cod_country = 3 -- Ukraine
		--and 1 = 0
	  union all
	  SELECT
		id opr_atlas_user,
        'olxua' opr_source_system,
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
		cast(null as bigint) flg_uses_crm, 
		scai_execution.cod_execution
	  FROM
		crm_integration_stg.stg_ua_db_atlas_olxua_users,
     (
      select
        max(fac.cod_execution) cod_execution
      from
        crm_integration_anlt.t_lkp_scai_process proc,
        crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc,
        crm_integration_anlt.t_fac_scai_execution fac
      where
        rel_integr_proc.cod_process = proc.cod_process
        and rel_integr_proc.cod_country = 3
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

analyze tmp_ua_load_atlas_user;	


	
delete from crm_integration_anlt.t_lkp_atlas_user
using tmp_ua_load_atlas_user
where 
	tmp_ua_load_atlas_user.dml_type = 'I' 
	and t_lkp_atlas_user.opr_atlas_user = tmp_ua_load_atlas_user.opr_atlas_user
	and t_lkp_atlas_user.valid_from = (select dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 3 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_atlas_user');
	

	
update crm_integration_anlt.t_lkp_atlas_user
set valid_to = (select rel_integr_proc.dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 3 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_atlas_user') 
from tmp_ua_load_atlas_user source
where source.cod_atlas_user = crm_integration_anlt.t_lkp_atlas_user.cod_atlas_user
and crm_integration_anlt.t_lkp_atlas_user.valid_to = 20991231
and source.dml_type in('U','D');

	
insert into crm_integration_anlt.t_lkp_atlas_user
	 select
       case
        when dml_type = 'I' then case when valid_from = (select dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc
														where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 3 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_atlas_user')
									then cod_atlas_user else max_cod + new_cod end
        when dml_type = 'U' then cod_atlas_user
      end cod_atlas_user,
	  opr_atlas_user,
	  dsc_atlas_user,
	  (select rel_integr_proc.dat_processing from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc where rel_integr_proc.cod_process = proc.cod_process and rel_integr_proc.cod_country = 3 and rel_integr_proc.cod_integration = 30000 and rel_integr_proc.ind_active = 1 and proc.dsc_process_short = 't_lkp_atlas_user') valid_from, 
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
      tmp_ua_load_atlas_user
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
    and rel_country_integr.cod_country = 3 -- Ukraine
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
last_processing_datetime = coalesce((select max(operation_timestamp) from tmp_ua_load_atlas_user),last_processing_datetime)
/*from
  (
    select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration
    from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc, crm_integration_anlt.t_rel_scai_country_integration rel_country_integr
    where proc.dsc_process_short = 't_lkp_atlas_user'
    and proc.cod_process = rel_integr_proc.cod_process
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_country = 3
  ) source*/
from crm_integration_anlt.t_lkp_scai_process proc 
where t_rel_scai_integration_process.cod_process = proc.cod_process
and t_rel_scai_integration_process.cod_status = 2
and t_rel_scai_integration_process.cod_country = 3
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
    and rel_integr_proc.cod_country = 3
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
    and rel_country_integr.cod_country = 3 -- Ukraine
    and rel_country_integr.cod_integration = rel_integr_proc.cod_integration
    and rel_country_integr.cod_country = rel_integr_proc.cod_country
    and rel_integr_proc.cod_process = proc.cod_process
    and rel_integr_proc.cod_status = 2
  and rel_country_integr.ind_active = 1
  and rel_integr_proc.ind_active = 1
  and proc.dsc_process_short = 't_lkp_contact_upd_atlas_user';

--$$$

-- ##########################################################
-- #        ATLAS / BASE - Ukraine                          #
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
          and rel_integr_proc.cod_country = 3
          and rel_integr_proc.cod_integration = 30000
          and rel_integr_proc.ind_active = 1
          and proc.dsc_process_short = 't_lkp_contact'
      ) scai_valid_from,
      crm_integration_anlt.t_lkp_atlas_user atlas_user,
      crm_integration_anlt.t_lkp_contact base_contact
    where
      atlas_user.cod_source_system = 21
      and atlas_user.valid_to = 20991231
      and lower(base_contact.email) = lower(atlas_user.dsc_atlas_user)
	  and trim(base_contact.email) != ''
      and base_contact.cod_source_system = 22
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
        and rel_integr_proc.cod_country = 3
        and rel_integr_proc.cod_integration = 30000
        and rel_integr_proc.ind_active = 1
        and proc.dsc_process_short = 't_lkp_contact'
    )
and cod_source_system in (select cod_source_system from crm_integration_anlt.t_lkp_source_system where cod_country = 3);

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
    and rel_country_integr.cod_country = 3 -- Ukraine
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
and t_rel_scai_integration_process.cod_country = 3
and proc.dsc_process_short = 't_lkp_contact_upd_atlas_user'
and t_rel_scai_integration_process.ind_active = 1;


	 
