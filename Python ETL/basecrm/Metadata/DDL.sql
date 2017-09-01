
--DROP TABLE "rdl_basecrm"."stg_d_base_contacts_xx_yy";
CREATE TABLE IF NOT EXISTS "rdl_basecrm_v2"."stg_d_base_contacts"
(
	"base_account_country" VARCHAR(10) ENCODE lzo
	,"base_account_category" VARCHAR(10) ENCODE lzo
	,"meta_event_type" VARCHAR(10) ENCODE lzo
	,"meta_event_time" TIMESTAMP ENCODE lzo
	,"id" INTEGER NOT NULL  ENCODE lzo
	,"creator_id" INTEGER   ENCODE lzo
	,"contact_id" INTEGER   ENCODE lzo
	,"created_at" DATE   ENCODE lzo
	,"updated_at" DATE   ENCODE delta
	,"title" VARCHAR(100)   ENCODE lzo
	,"name" VARCHAR(500)   ENCODE lzo
	,"first_name" VARCHAR(100)   ENCODE lzo
	,"last_name" VARCHAR(100)   ENCODE lzo
	,"description" VARCHAR(1000)   ENCODE lzo
	,"industry" VARCHAR(100)   ENCODE lzo
	,"website" VARCHAR(1000)   ENCODE lzo
	,"email" VARCHAR(500)   ENCODE lzo
	,"phone" VARCHAR(100)   ENCODE lzo
	,"mobile" VARCHAR(100)   ENCODE lzo
	,"fax" VARCHAR(100)   ENCODE lzo
	,"twitter" VARCHAR(100)   ENCODE lzo
	,"facebook" VARCHAR(100)   ENCODE lzo
	,"linkedin" VARCHAR(100)   ENCODE lzo
	,"skype" VARCHAR(200)   ENCODE lzo
	,"owner_id" VARCHAR(100)   ENCODE lzo
	,"is_organization" boolean
	,"address" VARCHAR(1000)   ENCODE lzo
	,"custom_fields" VARCHAR(1000)   ENCODE lzo
	,"customer_status" VARCHAR(100)   ENCODE lzo
	,"prospect_status" VARCHAR(100)   ENCODE lzo
	,"tags" VARCHAR(600)   ENCODE lzo
)
DISTSTYLE KEY
DISTKEY ("id")
SORTKEY ("id","meta_event_time","created_at", "owner_id")
;



--DROP TABLE "rdl_basecrm"."stg_d_base_deals_xx_yy";
CREATE TABLE IF NOT EXISTS "rdl_basecrm_v2"."stg_d_base_deals"
(
	"base_account_country" VARCHAR(10) ENCODE lzo
	,"base_account_category" VARCHAR(10) ENCODE lzo
	,"meta_event_type" VARCHAR(10) ENCODE lzo
	,"meta_event_time" TIMESTAMP ENCODE lzo
	,"id" INT  ENCODE lzo
	,"last_activity_at" TIMESTAMP   ENCODE delta
	,"contact_id" INT   ENCODE lzo
	,"source_id" INT   ENCODE lzo
	,"estimated_close_date" DATE   ENCODE delta
	,"dropbox_email" VARCHAR(100)   ENCODE lzo
	,"creator_id" INT   ENCODE lzo
	,"loss_reason_id" INT   ENCODE lzo
	,"currency" VARCHAR(10)   ENCODE lzo
	,"updated_at" TIMESTAMP ENCODE delta
	,"organization_id" INT   ENCODE lzo
	,"last_stage_change_at" TIMESTAMP   ENCODE delta
	,"name" VARCHAR(1000)   ENCODE lzo
	,"owner_id" INT   ENCODE lzo
	,"value" DOUBLE PRECISION   ENCODE bytedict
	,"created_at" TIMESTAMP   ENCODE lzo
	,"hot" boolean
	,"last_stage_change_by_id" INT   ENCODE lzo
	,"stage_id" INT   ENCODE lzo
	,"custom_fields" VARCHAR(8000)   ENCODE lzo
	,"tags" VARCHAR(600)   ENCODE lzo
)
DISTSTYLE KEY
DISTKEY ("id")
SORTKEY ("id","meta_event_time","stage_id", "last_stage_change_at")
;


--DROP TABLE "rdl_basecrm"."stg_d_base_leads_xx_yy";
CREATE TABLE IF NOT EXISTS "rdl_basecrm_v2"."stg_d_base_leads"
(
	"base_account_country" VARCHAR(10) ENCODE lzo
	,"base_account_category" VARCHAR(10) ENCODE lzo
	,"meta_event_type" VARCHAR(10) ENCODE lzo
	,"meta_event_time" TIMESTAMP ENCODE lzo
	,"id" INTEGER NOT NULL  ENCODE lzo
	,"first_name" VARCHAR(500)   ENCODE lzo
	,"last_name" VARCHAR(500)   ENCODE lzo
	,"owner_id" INTEGER   ENCODE lzo
	,"source_id" INTEGER   ENCODE lzo
	,"created_at" TIMESTAMP   ENCODE delta
	,"updated_at" TIMESTAMP   ENCODE delta
	,"twitter" VARCHAR(500)   ENCODE lzo
	,"phone" VARCHAR(500)   ENCODE lzo
	,"mobile" VARCHAR(500)   ENCODE lzo
	,"facebook" VARCHAR(500)   ENCODE lzo
	,"email" VARCHAR(500)   ENCODE lzo
	,"title" VARCHAR(500)   ENCODE lzo
	,"skype" VARCHAR(500)   ENCODE lzo
	,"linkedin" VARCHAR(500)   ENCODE lzo
	,"description" VARCHAR(500)   ENCODE lzo
	,"industry" VARCHAR(500)   ENCODE lzo
	,"fax" VARCHAR(500)   ENCODE lzo
	,"website" VARCHAR(500)   ENCODE lzo
	,"address" VARCHAR(1000)   ENCODE lzo
	,"status" VARCHAR(500)   ENCODE lzo
	,"creator_id" INTEGER   ENCODE lzo
	,"organization_name" VARCHAR(500)   ENCODE lzo
	,"custom_fields" VARCHAR(8000)   ENCODE lzo
	,"tags" VARCHAR(500)   ENCODE lzo
)
DISTSTYLE KEY
DISTKEY ("id")
SORTKEY ("id","meta_event_time","created_at","owner_id")
;


CREATE TABLE IF NOT EXISTS "rdl_basecrm_v2"."stg_d_base_users"
(
	"base_account_country" VARCHAR(10) ENCODE lzo
	,"base_account_category" VARCHAR(10) ENCODE lzo
	,"meta_event_type" VARCHAR(10) ENCODE lzo
	,"meta_event_time" TIMESTAMP ENCODE lzo
	,"id" INTEGER NOT NULL  ENCODE lzo
	,"name" VARCHAR(100)   ENCODE lzo
	,"email" VARCHAR(100)   ENCODE lzo
	,"role" VARCHAR(100)   ENCODE lzo
	,"status" VARCHAR(100)   ENCODE lzo
	,"confirmed" BOOLEAN
	,"created_at" TIMESTAMP   ENCODE lzo
	,"updated_at" TIMESTAMP   ENCODE lzo
	,"deleted_at" TIMESTAMP   ENCODE lzo
)
DISTSTYLE ALL
SORTKEY ("id","meta_event_time")
;



CREATE TABLE IF NOT EXISTS "rdl_basecrm_v2"."stg_d_base_stages"
(
	"base_account_country" VARCHAR(10) ENCODE lzo
	,"base_account_category" VARCHAR(10) ENCODE lzo
	,"meta_event_type" VARCHAR(10) ENCODE lzo
	,"meta_event_time" TIMESTAMP ENCODE lzo
	,"id" INTEGER NOT NULL  ENCODE lzo
	,"name" VARCHAR(100)   ENCODE lzo
	,"position" INTEGER   ENCODE lzo
	,"category" VARCHAR(100)   ENCODE lzo
	,"likelihood" INTEGER   ENCODE lzo
	,"active" BOOLEAN
	,"pipeline_id" INTEGER   ENCODE lzo
	,"created_at" TIMESTAMP   ENCODE lzo
	,"updated_at" TIMESTAMP   ENCODE lzo
)
DISTSTYLE ALL
SORTKEY ("id","meta_event_time")
;

CREATE TABLE IF NOT EXISTS "rdl_basecrm_v2"."stg_d_base_calls"
(
	"base_account_country" VARCHAR(10) ENCODE lzo
	,"base_account_category" VARCHAR(10) ENCODE lzo
	,"meta_event_type" VARCHAR(10) ENCODE lzo
	,"meta_event_time" TIMESTAMP ENCODE lzo
	,"id" INTEGER NOT NULL  ENCODE lzo
	,"user_id" INTEGER   ENCODE lzo
	,"phone_number" VARCHAR(100)   ENCODE lzo
	,"missed" BOOLEAN
	,"associated_deal_ids" VARCHAR(1000)   ENCODE lzo
	,"resource_id" INTEGER   ENCODE lzo
	,"updated_at" TIMESTAMP   ENCODE lzo
	,"made_at" TIMESTAMP   ENCODE lzo
	,"summary" VARCHAR(10000)   ENCODE lzo
	,"outcome_id" INTEGER   ENCODE lzo
	,"duration" INTEGER   ENCODE lzo
	,"incoming" BOOLEAN
	,"recording_url" VARCHAR(1000)   ENCODE lzo
	,"resource_type" VARCHAR(100)   ENCODE lzo
)
DISTSTYLE KEY
DISTKEY ("id")
SORTKEY ("meta_event_time","user_id", "updated_at")
;


CREATE TABLE IF NOT EXISTS "rdl_basecrm_v2"."stg_d_base_call_outcomes"
(
	"base_account_country" VARCHAR(10) ENCODE lzo
	,"base_account_category" VARCHAR(10) ENCODE lzo
	,"meta_event_type" VARCHAR(10) ENCODE lzo
	,"meta_event_time" TIMESTAMP ENCODE lzo
	,"id" INTEGER NOT NULL  ENCODE lzo
	,"name" varchar(100)   ENCODE lzo
	,"creator_id" INTEGER   ENCODE lzo
	,"created_at" TIMESTAMP   ENCODE lzo
	,"updated_at" TIMESTAMP   ENCODE lzo
)
DISTSTYLE ALL
SORTKEY ("id","meta_event_time")
;


CREATE TABLE IF NOT EXISTS "rdl_basecrm_v2"."stg_d_base_loss_reasons"
(
	"base_account_country" VARCHAR(10) ENCODE lzo
	,"base_account_category" VARCHAR(10) ENCODE lzo
	,"meta_event_type" VARCHAR(10) ENCODE lzo
	,"meta_event_time" TIMESTAMP ENCODE lzo
	,"id" INTEGER NOT NULL  ENCODE lzo
	,"name" varchar(100)   ENCODE lzo
	,"creator_id" INTEGER   ENCODE lzo
	,"created_at" TIMESTAMP   ENCODE lzo
	,"updated_at" TIMESTAMP   ENCODE lzo
)
DISTSTYLE ALL
SORTKEY ("id","meta_event_time")
;

CREATE TABLE IF NOT EXISTS "rdl_basecrm_v2"."stg_d_base_pipelines"
(
	"base_account_country" VARCHAR(10) ENCODE lzo
	,"base_account_category" VARCHAR(10) ENCODE lzo
	,"meta_event_type" VARCHAR(10) ENCODE lzo
	,"meta_event_time" TIMESTAMP ENCODE lzo
	,"id" INTEGER NOT NULL  ENCODE lzo
	,"name" varchar(100)   ENCODE lzo
	,"created_at" TIMESTAMP   ENCODE lzo
	,"updated_at" TIMESTAMP   ENCODE lzo
	,"disabled" BOOLEAN
)
DISTSTYLE ALL
SORTKEY ("id","meta_event_time")
;


CREATE TABLE IF NOT EXISTS "rdl_basecrm_v2"."stg_d_base_sources"
(
	"base_account_country" VARCHAR(10) ENCODE lzo
	,"base_account_category" VARCHAR(10) ENCODE lzo
	,"meta_event_type" VARCHAR(10) ENCODE lzo
	,"meta_event_time" TIMESTAMP ENCODE lzo
	,"id" INTEGER NOT NULL  ENCODE lzo
	,"name" varchar(100)   ENCODE lzo
	,"created_at" TIMESTAMP   ENCODE lzo
	,"updated_at" TIMESTAMP   ENCODE lzo
	,"resource_type" varchar(100)   ENCODE lzo
)
DISTSTYLE ALL
SORTKEY ("id","meta_event_time")
;


CREATE TABLE IF NOT EXISTS "rdl_basecrm_v2"."stg_d_base_tags"
(
	"base_account_country" VARCHAR(10) ENCODE lzo
	,"base_account_category" VARCHAR(10) ENCODE lzo
	,"meta_event_type" VARCHAR(10) ENCODE lzo
	,"meta_event_time" TIMESTAMP ENCODE lzo
	,"id" INTEGER NOT NULL  ENCODE lzo
	,"name" varchar(100)   ENCODE lzo
	,"creator_id" INTEGER   ENCODE lzo
	,"created_at" TIMESTAMP   ENCODE lzo
	,"updated_at" TIMESTAMP   ENCODE lzo
	,"resource_type" varchar(100)   ENCODE lzo
)
DISTSTYLE ALL
SORTKEY ("id","meta_event_time")
;


CREATE TABLE IF NOT EXISTS "rdl_basecrm_v2"."stg_d_base_tasks"
(
	"base_account_country" VARCHAR(10) ENCODE lzo
	,"base_account_category" VARCHAR(10) ENCODE lzo
	,"meta_event_type" VARCHAR(10) ENCODE lzo
	,"meta_event_time" TIMESTAMP ENCODE lzo
	,"id" INTEGER NOT NULL  ENCODE lzo
	,"creator_id" INTEGER   ENCODE lzo
	,"owner_id" INTEGER ENCODE lzo
	,"resource_type" VARCHAR(100)
	,"resource_id" INTEGER ENCODE lzo
	,"completed" BOOLEAN
	,"completed_at" TIMESTAMP ENCODE lzo
	,"due_date" TIMESTAMP ENCODE lzo
	,"overdue" BOOLEAN
	,"remind_at" TIMESTAMP ENCODE lzo
	,"content" VARCHAR(1000) ENCODE lzo
	,"created_at" TIMESTAMP   ENCODE lzo
	,"updated_at" TIMESTAMP   ENCODE lzo
)
DISTSTYLE KEY
DISTKEY ("id")
SORTKEY ("meta_event_time","created_at", "owner_id")
;



CREATE TABLE IF NOT EXISTS "rdl_basecrm_v2"."stg_d_base_orders"
(
	"base_account_country" VARCHAR(10) ENCODE lzo
	,"base_account_category" VARCHAR(10) ENCODE lzo
	,"meta_event_type" VARCHAR(10) ENCODE lzo
	,"meta_event_time" TIMESTAMP ENCODE lzo
	,"id" INTEGER ENCODE lzo
	,"deal_id" INTEGER ENCODE lzo
	,"discount" INTEGER ENCODE lzo
	,"created_at" TIMESTAMP   ENCODE lzo
	,"updated_at" TIMESTAMP   ENCODE lzo
)
DISTSTYLE KEY
DISTKEY ("id")
SORTKEY ("id","deal_id","meta_event_time","created_at")
;


CREATE TABLE IF NOT EXISTS "rdl_basecrm_v2"."stg_d_base_line_items"
(
	"base_account_country" VARCHAR(10) ENCODE lzo
	,"base_account_category" VARCHAR(10) ENCODE lzo
	,"meta_event_type" VARCHAR(10) ENCODE lzo
	,"meta_event_time" TIMESTAMP ENCODE lzo
	,"id" INTEGER ENCODE lzo
	,"sku" VARCHAR(100) ENCODE lzo
	,"description" VARCHAR(5000) ENCODE lzo
	,"order_id" INTEGER ENCODE lzo
	,"deal_id" INTEGER ENCODE lzo
	,"value" DOUBLE PRECISION   ENCODE bytedict
	,"price" DOUBLE PRECISION   ENCODE bytedict
	,"currency" VARCHAR(10) ENCODE lzo
	,"variation" DOUBLE PRECISION   ENCODE bytedict
	,"quantity" INTEGER ENCODE lzo
	,"name" VARCHAR(1000) ENCODE lzo
	,"created_at" TIMESTAMP   ENCODE lzo
	,"updated_at" TIMESTAMP   ENCODE lzo
)
DISTSTYLE KEY
DISTKEY ("order_id")
SORTKEY ("order_id","meta_event_time","created_at")
;

