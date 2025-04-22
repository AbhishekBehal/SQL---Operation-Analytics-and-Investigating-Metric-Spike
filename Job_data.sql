/* Jobs Reviewed Over Time:
Objective: Calculate the number of jobs reviewed per hour for each day in November 2020.
Your Task: Write an SQL query to calculate the number of jobs reviewed per hour for each day in November 2020.
Throughput Analysis:
Objective: Calculate the 7-day rolling average of throughput (number of events per second).
Your Task: Write an SQL query to calculate the 7-day rolling average of throughput. Additionally, explain whether you prefer using the daily metric or the 7-day rolling average for throughput, and why.
Language Share Analysis:
Objective: Calculate the percentage share of each language in the last 30 days.
Your Task: Write an SQL query to calculate the percentage share of each language over the last 30 days.
Duplicate Rows Detection:
Objective: Identify duplicate rows in the data.
Your Task: Write an SQL query to display duplicate rows from the job_data table.

Case Study 2: Investigating Metric Spike
You will be working with three tables:

users: Contains one row per user, with descriptive information about that user’s account.
events: Contains one row per event, where an event is an action that a user has taken (e.g., login, messaging, search).
email_events: Contains events specific to the sending of emails.
Tasks:

Weekly User Engagement:
Objective: Measure the activeness of users on a weekly basis.
Your Task: Write an SQL query to calculate the weekly user engagement.
User Growth Analysis:
Objective: Analyze the growth of users over time for a product.
Your Task: Write an SQL query to calculate the user growth for the product.
Weekly Retention Analysis:
Objective: Analyze the retention of users on a weekly basis after signing up for a product.
Your Task: Write an SQL query to calculate the weekly retention of users based on their sign-up cohort.
Weekly Engagement Per Device:
Objective: Measure the activeness of users on a weekly basis per device.
Your Task: Write an SQL query to calculate the weekly engagement per device.
Email Engagement Analysis:
Objective: Analyze how users are engaging with the email service.
Your Task: Write an SQL query to calculate the email engagement metrics. */









create database Project3;
Use Project3;
drop table users;
create table users (
user_id int, created_at varchar(30),
company_id int,
language varchar(30),
activated_at varchar(100),
state varchar(50))
;
drop table users;

select * from users;

Show variables like 'secure_file_priv';

Load data infile "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/users.csv"
into table users
fields terminated by ','
enclosed by  '"'
lines terminated by '\n'
ignore 1 rows;

select * from users;

alter table users add column temp_created_at datetime;

SET SQL_SAFE_UPDATES = 0;
SET SQL_SAFE_UPDATES = 1;

UPDATE users
SET temp_created_at = STR_TO_DATE(created_at, '%d-%m-%Y %H:%i')
;
Alter table users drop column created_at;

alter table users change column temp_created_at  created_at datetime;


Create table email_events (
user_id int,
occurred_at varchar(100),
action varchar(100),
user_type int);

Show variables like 'secure_file_priv';

Load data infile "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/email_events.csv"
into table email_events
fields terminated by ','
enclosed by  '"'
lines terminated by '\n'
ignore 1 rows;
Select * from email_events;

alter table email_events add column temp_created_at datetime;

SET SQL_SAFE_UPDATES = 0;
SET SQL_SAFE_UPDATES = 1;

UPDATE email_events
SET temp_created_at = STR_TO_DATE(occurred_at, '%d-%m-%Y %H:%i')
;
Alter table email_events drop column occurred_at;

alter table email_events change column temp_created_at  created_at datetime;


create table events (
user_id int,
occured_at varchar(100),
event_type varchar(100),
event_name varchar(100),
location varchar(100),
device varchar(100),
user_type int
);
drop table events;




Show variables like 'secure_file_priv';

Load data infile "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/events.csv"
into table events
fields terminated by ','
enclosed by  '"'
lines terminated by '\n'
ignore 1 rows;

alter table events add column temp_created_at datetime;

select* from events;
drop table events;
SET SQL_SAFE_UPDATES = 0;
SET SQL_SAFE_UPDATES = 1;

UPDATE events
SET temp_created_at = STR_TO_DATE(occured_at, '%d-%m-%Y %H:%i')
;
Alter table events drop column occured_at;

alter table events change column temp_created_at  created_at datetime;


use Project3;

Show tables from project3;

select * from users;

select * from events;


WITH weekly_activity AS (
    SELECT 
        YEARWEEK(created_at, 1) AS week_start, -- Grouping by ISO week
        user_id,
        COUNT(DISTINCT action) AS email_actions_count,
        0 AS event_types_count -- Placeholder for UNION ALL compatibility
    FROM email_events
    GROUP BY week_start, user_id

    UNION ALL

    SELECT 
        YEARWEEK(created_at, 1) AS week_start,
        user_id,
        0 AS email_actions_count, -- Placeholder for UNION ALL compatibility
        COUNT(DISTINCT event_type) AS event_types_count
    FROM events
    GROUP BY week_start, user_id
)
SELECT 
    week_start,
    COUNT(DISTINCT user_id) AS active_users,
    SUM(email_actions_count) AS total_email_actions,
    SUM(event_types_count) AS total_event_types
FROM weekly_activity
GROUP BY week_start
ORDER BY week_start;

SET @cumulative_users := 0;

WITH user_growth AS (
    SELECT 
        DATE(created_at) AS creation_date, -- Group by daily user activation
        COUNT(user_id) AS new_users -- Count of users activated on that day
    FROM users
    WHERE created_at IS NOT NULL
    GROUP BY creation_date
    ORDER BY creation_date
)
SELECT 
    creation_date,
    new_users,
    (@cumulative_users := @cumulative_users + new_users) AS cumulative_users -- Calculate cumulative user growth
FROM user_growth;

select* from users;

WITH signup_cohorts AS (
    SELECT 
        user_id,
        YEARWEEK(created_at, 1) AS signup_week -- User cohort based on signup week
    FROM users
    WHERE created_at IS NOT NULL
),
user_activity AS (
    SELECT 
        user_id,
        YEARWEEK(created_at, 1) AS activity_week -- Week of activity
    FROM (
        SELECT user_id, created_at FROM email_events
        UNION ALL
        SELECT user_id, created_at FROM events
    ) all_events
),
cohort_activity AS (
    SELECT 
        sc.signup_week,
        ua.activity_week,
        COUNT(DISTINCT ua.user_id) AS active_users
    FROM signup_cohorts sc
    JOIN user_activity ua
    ON sc.user_id = ua.user_id
    GROUP BY sc.signup_week, ua.activity_week
),
retention_summary AS (
    SELECT 
        signup_week,
        activity_week,
        TIMESTAMPDIFF(WEEK, STR_TO_DATE(CONCAT(SUBSTR(signup_week, 1, 4), '-', SUBSTR(signup_week, 5, 2), '-1'), '%X-%V-%w'), 
        STR_TO_DATE(CONCAT(SUBSTR(activity_week, 1, 4), '-', SUBSTR(activity_week, 5, 2), '-1'), '%X-%V-%w')) AS week_number,
        active_users
    FROM cohort_activity
)
SELECT 
    signup_week,
    week_number,
    active_users
FROM retention_summary
WHERE week_number >= 0 -- Ensure we only calculate retention for weeks after signup
ORDER BY signup_week, week_number;

use project3;
select * from email_events;

WITH weekly_user_activity AS (
    SELECT
        e.device,
        DATE_FORMAT(e.created_at, '%Y-%u') AS week, -- Extract year-week (e.g., 2014-18 for the 18th week of 2014)
        COUNT(DISTINCT e.user_id) AS active_users
    FROM
        events e
    WHERE
        e.created_at BETWEEN '2014-05-01' AND '2014-08-31' -- Filter by the specified date range
    GROUP BY
        e.device,
        DATE_FORMAT(e.created_at, '%Y-%u')
)
SELECT
    device,
    week,
    active_users
FROM
    weekly_user_activity
ORDER BY
    week DESC, device ASC;

select* from events;
WITH email_metrics AS (
    SELECT
        ee.action,
        COUNT(*) AS total_actions,                -- Total number of actions performed
        COUNT(DISTINCT ee.user_id) AS unique_users, -- Number of distinct users performing the actions
        ee.user_type
    FROM
        email_events ee
    GROUP BY
        ee.action, ee.user_type
),
email_summary AS (
    SELECT
        SUM(total_actions) AS total_actions_overall,
        SUM(unique_users) AS unique_users_overall
    FROM
        email_metrics
)
SELECT
    em.action,
    em.user_type,
    em.total_actions,
    em.unique_users,
    (em.total_actions / es.total_actions_overall) * 100 AS action_percentage,
    (em.unique_users / es.unique_users_overall) * 100 AS user_percentage
FROM
    email_metrics em
CROSS JOIN
    email_summary es
ORDER BY
    em.action, em.user_type;












