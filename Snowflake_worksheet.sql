SELECT DATE,COUNTY,STATE,CASES,CASES_SINCE_PREV_DAY,DEATHS,DEATHS_SINCE_PREV_DAY
FROM NYT_US_COVID19
WHERE COUNTY = 'Abbeville'
AND date= last_day(Date);
AND MONTH(DATE) = '3';

select current_account();

-- MONTHLY CASE
SELECT DATE,COUNTY,STATE,CASES,DEATHS --CASES_SINCE_PREV_DAY,DEATHS,DEATHS_SINCE_PREV_DAY
FROM NYT_US_COVID19
WHERE COUNTY = 'Abbeville'
AND date= last_day(Date)
--AND MONTH(DATE) = 12 AND YEAR(DATE) = 2021
ORDER BY DATE;

select count(*) from COVID_US;

SELECT *
FROM NYT_US_COVID19
WHERE COUNTY = 'Abbeville'
AND date= last_day(Date)
--AND MONTH(DATE) = 12 AND YEAR(DATE) = 2021
ORDER BY DATE;


--DEMOGRAPHICS

SELECT STATE,COUNTY,TOTAL_POPULATION,TOTAL_FEMALE_POPULATION,TOTAL_MALE_POPULATION FROM DEMOGRAPHICS
where COUNTY='Hughes';

SELECT * FROM DEMOGRAPHICS
WHERE STATE='NY';

--state identification
select distinct state from DEMOGRAPHICS order by 1;

SELECT distinct ISO3166_2 FROM  NYT_US_COVID19 order by 1;

--county identification

select STATE,county,SUBSTR(COUNTY, 1, charindex(' ', COUNTY, 1)-1),charindex(' ', COUNTY, 1),* 
from DEMOGRAPHICS where  state= 'NE' and COUNTY  LIKE'Logan%' ORDER BY 1;



SELECT *--DISTINCT last_day(Date),COUNTY,STATE,ISO3166_2 
FROM  NYT_US_COVID19 
WHERE  COUNTY LIKE 'Logan%' and State= 'Nebraska' and last_day(Date) = Date order by 1; -- ISO3166_2='NE'  --ORDER BY 1;

SELECT last_day(us.Date),us.Date,US.COUNTY,US.STATE,US.CASES,US.DEATHS,
SUBSTR(d.COUNTY, 1, charindex(' ', d.COUNTY, 1)) as county,d.STATE,
d.total_population,d.total_female_population,d.total_male_population
FROM demographics d 
left join NYT_US_COVID19 us
on us.county=SUBSTR(d.COUNTY, 1, charindex(' ', d.COUNTY, 1) - 1)
and us.ISO3166_2=d.state
where last_day(us.Date) = us.Date 
and us.county = 'Logan' and us.state ='Nebraska'
order by 2;
--COUNTY = 'Abbeville'


 ---Create db for all the databasets

use database COVID19_EPIDEMIOLOGICAL_DATA;
describe table COVID19_EPIDEMIOLOGICAL_DATA.Public.NYT_US_COVID19 
describe table COVID19_EPIDEMIOLOGICAL_DATA.Public.demographics 
 
Create database COVID_ANALYSIS;

use database COVID_ANALYSIS;
--Insert data into US_COVID_COUNTY

DROP TABLE  public.COVID_US;

Create Table public.COVID_US(
state VARCHAR(16777216),
county VARCHAR(16777216),
latitude FLOAT,
longitude FLOAT,
deaths NUMBER(38,0),
cases NUMBER(38,0),
Death_Per_Case_Perc FLOAT,
Last_Date_Of_Month  DATE,
total_female_population NUMBER(38,0),
total_male_population NUMBER(38,0),
total_population NUMBER(38,0)
);

insert into COVID_ANALYSIS.PUBLIC.COVID_US

SELECT US.STATE,US.COUNTY,d.latitude,d.longitude,US.CASES,US.DEATHS, (US.DEATHS/US.CASES) * 100 Death_Per_Case_Perc
,last_day(us.Date) as Last_Date_Of_Month,d.total_female_population
,d.total_male_population,d.total_population
FROM COVID19_EPIDEMIOLOGICAL_DATA.Public.demographics d 
left join COVID19_EPIDEMIOLOGICAL_DATA.Public.NYT_US_COVID19 us
on us.county=SUBSTR(d.COUNTY, 1, charindex(' ', d.COUNTY, 1) - 1)
and us.ISO3166_2=d.state
where last_day(us.Date) = us.Date;



SELECT * FROM COVID_ANALYSIS.PUBLIC.COVID_US;


--SCS_BE_DETAILED_MORTALITY;

use database covid_analysis;
use schema public;

create table test_1( cdatetime  varchar(50));

insert into test_1 values ('1/1/06 0:00'),('1/11/06 0:00') ;
insert into test_1 values ('1/14/06 0:00'),('1/29/06 0:00') ;
insert into test_1 values ('1/18/06 9:00'),('1/25/06 11:00') ;
insert into test_1 values ('1/29/06 6:00'),('1/25/06 19:00') ;


insert into test_1 values ('1/21/06 14:00'),('1/22/06 16:00') ;
select * from test_1;



select case 
        when left(Time_Of_Day, charindex(':',Time_Of_Day,1)) between 0 and 5 then 'Night 00:00-6:00'
        when left(Time_Of_Day, charindex(':',Time_Of_Day,1)) between 6 and 11 then 'Morning 6:00-12:00'
        when left(Time_Of_Day, charindex(':',Time_Of_Day,1)) between 12 and 17 then 'Day 12:00-18:00'
else 'Night 00:00-6:00' end as Time_Of_Day, 
Num_Of_Crimes 
from
(select substring(cdatetime, charindex(' ',cdatetime,1) +1, len(cdatetime) - charindex(' ',cdatetime,1)) as Time_Of_Day, 
count(*) Num_Of_Crimes
from test_1 
group by substring(cdatetime, charindex(' ',cdatetime,1) +1, len(cdatetime) - charindex(' ',cdatetime,1) )
) x
order by Num_Of_Crimes desc

--select replace(left(Time_Of_Day, charindex(':',Time_Of_Day,1)),':','') 

select 
 case    when replace(left(Time_Of_Day, charindex(':',Time_Of_Day,1)),':','') between 0 and 5 then 'Night 00:00-6:00'
        when replace(left(Time_Of_Day, charindex(':',Time_Of_Day,1)),':','') between 6 and 11 then 'Morning 6:00-12:00'
        when replace(left(Time_Of_Day, charindex(':',Time_Of_Day,1)),':','') between 12 and 17 then 'Day 12:00-18:00'
else 'Night 00:00-6:00' end as Time_Of_Day, 
sum(Num_Of_Crimes) as Num_Of_Crimes
from (
select substring(cdatetime, charindex(' ',cdatetime,1) +1, len(cdatetime) - charindex(' ',cdatetime,1)) as Time_Of_Day, 
count(*) Num_Of_Crimes
from test_1 
group by substring(cdatetime, charindex(' ',cdatetime,1) +1, len(cdatetime) - charindex(' ',cdatetime,1) )
) x
group by case    when replace(left(Time_Of_Day, charindex(':',Time_Of_Day,1)),':','') between 0 and 5 then 'Night 00:00-6:00'
        when replace(left(Time_Of_Day, charindex(':',Time_Of_Day,1)),':','') between 6 and 11 then 'Morning 6:00-12:00'
        when replace(left(Time_Of_Day, charindex(':',Time_Of_Day,1)),':','') between 12 and 17 then 'Day 12:00-18:00'
else 'Night 00:00-6:00' end 
order by 2 desc


select replace(substr(cdatetime,3,2),'/','')
from test_1;



show grants to role accountadmin;

select * from information_schema.usage_Privileges;
select * from information_schema.object_Privileges;

-- List all account level privileges granted to roles.
show grants on account;

show users;

show grants to user kumariamrita;

-- CREATE NEW USER--

create user amrita_k18
password = 'Amritasingh18'
must_change_password =TRUE;

CREATE TABLE MY_COVID_19.PUBLIC.US_Cases_Deaths_BY_State_County AS
with deaths as ( select state,county,max(cases) as cases, max(deaths) as deaths from NYT_US_COVID19 group by state,county),
     beds as (select State,county,sum(hospitals) as hospitals_in_County, sum(icu_beds) as icu_beds_in_county from KFF_US_ICU_BEDS group by State,county),
     demo as (SELECT 'USA',abbr.State,replace(d.County,' County','') as County,TOTAL_POPULATION
                        FROM DEMOGRAPHICS d left join MY_COVID_19.PUBLIC.US_STATE_ABBR abbr on d.state = abbr.abbr)      

Select d.state, d.county,d.deaths,d.cases, b.hospitals_in_County,b.icu_beds_in_county,total_population,
d.deaths/d.cases*100 as death_per_cases_perc,
d.cases/demo.total_population*100 as case_by_popl_perc,
d.deaths/demo.total_population*100 as death_by_popl_perc
from deaths d inner join beds b on d.state = b.state and d.county = b.county 
inner join demo on d.state = demo.state and  d.county = demo.county;


--2
--Number of deaths and cases by state in USA

CREATE TABLE MY_COVID_19.PUBLIC.US_Cases_Deaths_BY_State AS
with deaths as ( select state,max(cases) as cases, max(deaths) as deaths from NYT_US_COVID19 group by state),
     hosp as (select PROVINCE_STATE as STATE,TOTAL_HOSPITAL_BEDS AS HOSP_BEDS_IN_STATE,HOSPITAL_BEDS_PER_1000_POPULATION AS HOSP_BEDS_PER_1000_POPL_IN_STATE from KFF_HCP_CAPACITY),
     demo as (SELECT abbr.State,
                        sum(TOTAL_FEMALE_POPULATION) as F_POPL,sum(TOTAL_MALE_POPULATION )AS M_POPL,sum(TOTAL_POPULATION) AS POPL
                        FROM DEMOGRAPHICS d left join MY_COVID_19.PUBLIC.US_STATE_ABBR abbr on d.state = abbr.abbr group by abbr.State)      

Select d.state,d.deaths,d.cases,h.HOSP_BEDS_IN_STATE,h.HOSP_BEDS_PER_1000_POPL_IN_STATE,demo.POPL,
       d.deaths/demo.POPL*100 as death_by_popl_perc, 
       d.cases/demo.POPL*100 as case_by_popl_perc,
       h.HOSP_BEDS_IN_STATE/demo.POPL*100 as bed_by_popl_perc,
       h.HOSP_BEDS_IN_STATE/d.cases as bed_by_case_perc,
       ((d.cases - d.deaths)/demo.POPL)*100 as perc_popl_affected
       
from deaths d 
inner join demo on d.state = demo.state 
inner join hosp h on d.state = h.state;

------Python trend worksheet
import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, expr, sum as sum_
import pandas as pd
from statsmodels.tsa.arima.model import ARIMA

def main(session: snowpark.Session): 
    # Assuming 'JHU_COVID_19' is your table name
    table_name = 'JHU_COVID_19'

    # Create a DataFrame from the table
    jhu_df = session.table(table_name)

    # Filter the DataFrame based on your conditions
    filtered_df = jhu_df.filter(
        (col("COUNTRY_REGION").isin(['New Zealand', 'Mexico', 'Australia', 'Viet Nam', 'China', 'India', 'United Kingdom', 'United States'])) &
        (expr("DAY(DATE)") == 15)
    )

    # Group by country and date, and sum the cases
    grouped_df = filtered_df.groupBy(
        col("COUNTRY_REGION"), 
        col("DATE")
    ).agg(
        sum_(col("CASES")).alias("TOTAL_CASES")
    )

    # Print a sample of the DataFrame to standard output.
    grouped_df.show()

    # Return the grouped DataFrame
    return grouped_df
------------
select * --last_day(Date),total,positive,negative,
from CT_US_COVID_TESTS where month(date) = 4-- in ('2020-03-30', '2020-03-31','2020-04-01','2020-04-02')
and province_state='Alabama' order by date;
--LIMIT 100;

select province_state,year(date) as year, month(date)
,HOSPITALIZED,DEATH,positive,negative 
from CT_US_COVID_TESTS
where date= last_day(Date) and province_state='Alabama' 
order by 2,3;
-- , monthname(date);

select * from CT_US_COVID_TESTS
where date= last_day(Date);

select last_day(Date),* from CT_US_COVID_TESTS where date in (  '2020-03-30' , '2020-03-31','2020-04-01','2020-04-02')
and province_state='Alabama' 
LIMIT 100;


; with abcd as (
select province_state as state,year(date) as year_name, month(date) as month_name,
total,hospitalized,DEATH,positive,negative
from CT_US_COVID_TESTS
where date= last_day(Date) and province_state='Alabama'
)
select x.state,y.year_name,x.year_name as yearnumber,y.month_name,x.month_name as month_number,y.death, x.death,y.death - x.death as monthly_death
from abcd x
left join abcd y
ON y.state = x.state 
and y.year_name = x.year_name 
and y.month_name= x.month_name+1
order by 3,5;



select last_day(Date),sum(positive),sum(positive_since_previous_day)
from CT_US_COVID_TESTS where date <= '2020-04-01' --, '2020-03-31','2020-04-01','2020-04-02')
and province_state='Alabama' 
group by last_day(Date);


select continentexp as continent,country_region as country,year(date) as year_name, month(date) as month_name,CASES,deaths,population
FROM ECDC_GLOBAL
WHERE  date= last_day(Date)
and country_region= 'Algeria'
ORDER BY 1,2, 3,4;

SELECT *
--DISTINCT "TABLE","DESCRIPTION" 
FROM METADATA
where --"COLUMN" IS NULL AND 
"TABLE" = 'NYT_US_COVID19';

SELECT * FROM METADATA
WHERE  "TABLE" = 'NYT_US_COVID19';

select *
--continentexp as continent,country_region as country,year(date) as year_name, month(date) as month_name,date,CASES,deaths,population
FROM ECDC_GLOBAL
WHERE  
--date= last_day(Date) and
 country_region= 'Algeria'
 and year(date)= 2020
 and month(date) = 4
ORDER BY date;

select * from DEMOGRAPHICS limit 100;

select CURRENT_ACCOUNT()