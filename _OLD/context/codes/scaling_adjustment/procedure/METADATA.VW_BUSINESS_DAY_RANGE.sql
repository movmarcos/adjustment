create or replace view METADATA.VW_BUSINESS_DAY_RANGE(
	ORIGINAL_COBID,
	COBID,
	BUSINESS_DAYS_IN_PAST
) as
select d1.cobid as original_cobid ,d2.cobid as cobid,row_number() over (PARTITION BY d1.cobid order by d2.cobid desc) as business_days_in_past
from metadata.date d1
inner join metadata.date d2 on d2.cobid <= d1.cobid
where NOT d2.is_weekend AND NOT d2.is_bank_holiday;