create or replace function personal_balance(parm1 bigint, parm2 date)
  returns table (Participant text,
                 Date date,
                 Balance numeric(38,2),
                 Balance_For_Day numeric(38,2))
as
$body$
  select 
      initcap(concat_ws('', lrs.first_name, lrs.last_name)) as Participant,
      cf.operation_timestamp::DATE as date,
      sum(CASE WHEN cf.lender = $1 THEN cf.amount ELSE (-1*cf.amount) END) 
        over (
          partition by concat_ws('', lrs.first_name, lrs.last_name) 
          order by operation_timestamp::DATE asc
          ) as Balance,
      sum(CASE WHEN cf.lender = $1 THEN cf.amount ELSE (-1*cf.amount) END) as Balance_For_Day
  from Cash_Flows.Cash_Flow cf
  join Cash_Flows.Participants lrs on (cf.lender = lrs.person_id)
  join Cash_Flows.Participants lds on (cf.lendee = lds.person_id)
  where lrs.person_id = $1 and lds.person_id = $1 and cf.operation_timestamp::DATE <= $2
  group by operation_timestamp::DATE
  order by operation_timestamp::DATE asc
$body$
language sql;


create or replace function personal_balance_all(parm1 bigint, parm2 date)
  returns table (Participant text,
                 Date date,
                 Balance numeric(38,2),
                 Balance_For_Day numeric(38,2))
as
$body$
  select 
      initcap(concat_ws('', lrs.first_name, lrs.last_name)) as Participant,
      sum(CASE WHEN cf.lender = $1 THEN cf.amount ELSE (-1*cf.amount) END) as Balance_For_Day
  from Cash_Flows.Cash_Flow cf
  join Cash_Flows.Participants lrs on (cf.lender = lrs.person_id)
  join Cash_Flows.Participants lds on (cf.lendee = lds.person_id)
  where lrs.person_id = $1 and lds.person_id = $1 and cf.operation_timestamp::DATE <=$2
  group by initcap(concat_ws('', lrs.first_name, lrs.last_name)), initcap(concat_ws('', lds.first_name, lds.last_name))
$body$
language sql;

-- tables
-- Table: Cash_Flow
CREATE TABLE Cash_Flows.Cash_Flow (
    flow_id bigserial  NOT NULL,
    operation_timestamp timestamptz  NOT NULL,
    lender bigint  NOT NULL,
    lendee bigint  NOT NULL,
    amount numeric(38,2)  NOT NULL,
    comment text  NOT NULL,
    CONSTRAINT Cannot_Lend_To_Himself CHECK (lender!=lendee) NOT DEFERRABLE INITIALLY IMMEDIATE,
    CONSTRAINT Flow_PK PRIMARY KEY (flow_id)
);

-- Table: Participants
CREATE TABLE Cash_Flows.Participants (
    person_id bigserial  NOT NULL,
    first_name varchar(80)  NULL,
    middle_name varchar(100)  NULL,
    last_name varchar(80)  NOT NULL,
    CONSTRAINT Person_PK PRIMARY KEY (person_id)
);

-- foreign keys
-- Reference: lendee_person_FK (table: Cash_Flow)
ALTER TABLE Cash_Flows.Cash_Flow ADD CONSTRAINT lendee_person_FK
    FOREIGN KEY (lendee)
    REFERENCES Cash_Flows.Participants (person_id) 
    ON UPDATE  CASCADE 
    DEFERRABLE 
    INITIALLY DEFERRED
;

-- Reference: lender_person_FK (table: Cash_Flow)
ALTER TABLE Cash_Flows.Cash_Flow ADD CONSTRAINT lender_person_FK
    FOREIGN KEY (lender)
    REFERENCES Cash_Flows.Participants (person_id) 
    ON UPDATE  CASCADE 
    DEFERRABLE 
    INITIALLY DEFERRED
;
-- End of file.