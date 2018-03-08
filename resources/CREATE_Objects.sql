create or replace function pair_balance(lender bigint, lendee bigint, to_date date)
  returns TABLE(Lender          TEXT,
                Lendee          TEXT,
                Date            DATE,
                Balance_By_Day  NUMERIC(38, 2),
                Balance_To_Date NUMERIC(38, 2),
                is_up_to_date   BOOLEAN
  )
as
$body$
with src as (
    SELECT
      initcap(concat_ws(' ', lrs.first_name, lrs.last_name)) AS Lender,
      initcap(concat_ws(' ', lds.first_name, lds.last_name)) AS Lendee,
      cf.operation_timestamp :: DATE                         AS date,
      sum(CASE WHEN cf.lender = $1
        THEN cf.amount
          ELSE (-1 * cf.amount) END)                         AS Balance_For_Day
    FROM Cash_Flows.Cash_Flow cf
      JOIN Cash_Flows.Participants lrs ON (cf.lender = lrs.person_id)
      JOIN Cash_Flows.Participants lds ON (cf.lendee = lds.person_id)
    WHERE ((lrs.person_id = $1 AND lds.person_id = $2) OR (lrs.person_id = $2 AND lds.person_id = $1))
          AND cf.operation_timestamp :: DATE <= $3 :: DATE
    GROUP BY
      initcap(concat_ws(' ', lrs.first_name, lrs.last_name)),
      initcap(concat_ws(' ', lds.first_name, lds.last_name)),
      cf.lender,
      operation_timestamp :: DATE
)
select src.*,
  sum(src.Balance_For_Day) over (order by src.date asc) as to_date_balance,
  CASE WHEN lead(Balance_For_Day) over (
    ORDER BY date ASC
    ) is null
    THEN True
  ELSE False END as Is_Last
from src
order by src.date
$body$
language sql;


create or replace function pair_balance_all(lender bigint, to_date date)
  returns table (Lender text,
                 Lendee text,
                 Date date,
                 Balance_By_Day numeric(38,2),
                 Balance_To_Date numeric(38,2),
                 is_up_to_date boolean)
as
$body$
with src as (
    SELECT
      initcap(concat_ws(' ', lrs.first_name, lrs.last_name)) AS Lender,
      initcap(concat_ws(' ', lds.first_name, lds.last_name)) AS Lendee,
      cf.operation_timestamp :: DATE                         AS date,
      CASE WHEN cf.lender = $1
        THEN TRUE
      ELSE FALSE END                                            AS Is_Lender,
      sum(CASE WHEN cf.lender = $1
        THEN cf.amount
          ELSE (-1 * cf.amount) END) over (PARTITION BY cf.lender, cf.lendee, cf.operation_timestamp::DATE
        order by cf.operation_timestamp :: DATE asc)
        AS Balance_For_Day
    FROM Cash_Flows.Cash_Flow cf
      JOIN Cash_Flows.Participants lrs ON (cf.lender = lrs.person_id)
      JOIN Cash_Flows.Participants lds ON (cf.lendee = lds.person_id)
    WHERE (lrs.person_id = $1 OR lds.person_id = $1)
          AND cf.operation_timestamp :: DATE <= $2::DATE
)
select
  src.Lender,
  src.Lendee,
  src.date,
  src.Balance_For_Day as operation_amount,
  sum(Balance_For_Day) over (PARTITION BY
    CASE WHEN Is_Lender THEN lender ELSE Lendee END,
    CASE WHEN Is_Lender THEN lendee ELSE Lender END
    order by date asc) as cumulative_balance,
  CASE WHEN lead(Balance_For_Day) over (PARTITION BY
    CASE WHEN Is_Lender THEN lender ELSE Lendee END,
    CASE WHEN Is_Lender THEN lendee ELSE Lender END
    order by date asc) is null then True ELSE False end as Is_Last
from src
order by CASE WHEN Is_Lender THEN lender ELSE Lendee END
$body$
language sql;

create or replace function pair_balance_to_date(lender bigint, lendee bigint, to_date date)
  returns TABLE(Lender          TEXT,
                Lendee          TEXT,
                Last_Operation_Date DATE,
                Provided_Date DATE,
                Current_Balance  NUMERIC(38, 2)
  )
as
$body$
SELECT
  CASE WHEN balance_by_day<0 THEN src.lendee ELSE src.lender END as Lender,
  CASE WHEN balance_by_day<0 THEN src.lender ELSE src.lendee END as Lendee,
  date,
  $3::DATE,
  Balance_To_Date  from pair_balance($1, $2, $3) src
WHERE is_up_to_date is TRUE
$body$
language sql;

create or replace function pair_balance_all_to_date(lender bigint, to_date date)
  returns TABLE(Lender          TEXT,
                Lendee          TEXT,
                Last_Operation_Date DATE,
                Provided_Date DATE,
                Current_Balance  NUMERIC(38, 2)
  )
as
$body$
SELECT
  CASE WHEN balance_by_day<0 THEN src.lendee ELSE src.lender END as Lender,
  CASE WHEN balance_by_day<0 THEN src.lender ELSE src.lendee END as Lendee,
  date,
  $2::DATE,
  Balance_To_Date
from pair_balance_all($1, $2) src
WHERE is_up_to_date is TRUE
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