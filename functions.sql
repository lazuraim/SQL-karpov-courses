-------------------------- 1 -------------------------
-- Write a function that returns the TransferredPoints table 
-- in a more human-readable form:
-- Peer's nickname 1,
-- Peer's nickname 2,
-- number of transferred peer points.
-- The number is negative if peer 2 received more points from peer 1.

CREATE
    OR REPLACE FUNCTION points_amount()
    RETURNS TABLE
            (
                "Peer1"        VARCHAR(255),
                "Peer2"        text,
                "PointsAmount" INT
            )
AS
$$
BEGIN
    RETURN QUERY
        SELECT tp1.checkingpeer                           AS Peer1,
               tp1.checkedpeer                            AS Peer2,
               -1 * (tp2.pointsamount - tp1.pointsamount) AS PointsAmount
        FROM transferredpoints AS tp1
                 JOIN transferredpoints AS tp2 ON tp1.checkingpeer = tp2.checkedpeer
            AND tp1.checkedpeer = tp2.checkingpeer
        WHERE -1 * (tp2.pointsamount - tp1.pointsamount) != 0;
END;
$$
    LANGUAGE plpgsql;

SELECT *
FROM points_amount();

-------------------------------- 2 ---------------------------------
-- Write a function that returns a table of the following form: 
-- user name,
-- name of the checked task,
-- number of XP received
-- Include in the table only tasks that have successfully passed the check (according to the Checks table).
-- One task can be completed successfully several times. In this case, include all successful checks in the table.

CREATE
    OR REPLACE FUNCTION peer_task_xp()
    RETURNS TABLE
            (
                "Peer1" VARCHAR(255),
                "Task"  VARCHAR(255),
                "XP"    INT
            )
AS
$$
BEGIN
    RETURN QUERY
        SELECT peer,
               task,
               xp.xpamount
        FROM checks
                 JOIN xp ON checks.id = xp.checkid
                 JOIN p2p ON checks.id = p2p.checkid
        WHERE p2p.status = 'Success'
        ORDER BY peer;
END;
$$
    LANGUAGE plpgsql;

SELECT *
FROM peer_task_xp();


-------------------------------- 3 ---------------------------------
-- Write a function that finds the peers who have not left campus for the whole day
-- Function parameters: day, for example 12.05.2022.
-- The function returns only a list of peers.

CREATE
    OR REPLACE FUNCTION hardworking_peers(day DATE)
    RETURNS TABLE
            (
                "Peer" VARCHAR(255)
            )
AS
$$
BEGIN
    RETURN QUERY
        SELECT tt1.peer
        FROM timetracking AS tt1
                 JOIN timetracking AS tt2 ON tt1.peer = tt2.peer
            AND tt1.date = tt2.date
        WHERE tt1.date = day
          AND tt1.status = 1
          AND tt2.status = 2
          AND (tt2.time - tt1.time) > interval '10 hours';
END;
$$
    LANGUAGE plpgsql;

SELECT *
FROM hardworking_peers('2022-08-08');


-------------------------------- 4 ---------------------------------
-- Calculate the change in the number of peer points of each peer using the TransferredPoints table
-- Output the result sorted by the change in the number of points.
-- Output format: 
--     peer's nickname, 
--     change in the number of peer points

CREATE
    OR REPLACE FUNCTION change_in_points()
    RETURNS TABLE
            (
                "Peer"         VARCHAR(255),
                "PointsChange" BIGINT
            )
AS
$$
BEGIN
    RETURN QUERY
        SELECT DISTINCT peer_name, SUM(points_change)
        FROM (SELECT tp.checkingpeer                    as peer_name,
                     tp.pointsamount - tp2.pointsamount as points_change
              FROM transferredpoints AS tp
                       JOIN transferredpoints AS tp2 ON tp.checkingpeer = tp2.checkedpeer
                  AND tp.checkedpeer = tp2.checkingpeer
             ) AS points
        WHERE points_change != 0
        GROUP BY 1
        ORDER BY 2 DESC;
END;
$$
    LANGUAGE plpgsql;

SELECT *
FROM change_in_points();

-------------------------------- 5 ---------------------------------

-- Calculate the change in the number of peer points of each peer using the table returned by the first function from Part 3
-- Output the result sorted by the change in the number of points.
-- Output format: 
-- peer's nickname,
-- change in the number of peer points

CREATE
    OR REPLACE FUNCTION change_in_points_2()
    RETURNS TABLE
            (
                "Peer"         VARCHAR(255),
                "PointsChange" BIGINT
            )
AS
$$
BEGIN
    RETURN QUERY
        SELECT points."Peer1",
               SUM(points."PointsAmount") AS PointsChange
        FROM points_amount() AS points
        GROUP BY 1
        ORDER BY 2 DESC;
END;
$$
    LANGUAGE plpgsql;

SELECT *
FROM change_in_points_2();

-------------------------------- 6 ---------------------------------
-- Find the most frequently checked task for each day
-- If there is the same number of checks for some tasks in a certain day, output all of them.
-- Output format: 
-- day,
-- task name

CREATE
    OR REPLACE FUNCTION most_frequently_checked_task()
    RETURNS TABLE
            (
                "Day"      DATE,
                "TaskName" VARCHAR(255)
            )
AS
$$
BEGIN
    RETURN QUERY WITH tasks AS (
        SELECT task,
               date,
               COUNT(task) AS number_of_tasks
        FROM checks
        GROUP BY 1, 2
    )
                 SELECT date, task
                 FROM tasks
                 WHERE number_of_tasks = (
                     SELECT MAX(number_of_tasks)
                     FROM tasks AS tasks2
                     WHERE tasks.date = tasks2.date
                 )
                 ORDER BY 1;
END;
$$
    LANGUAGE plpgsql;

SELECT *
FROM most_frequently_checked_task();


-------------------------------- 7 ---------------------------------

-- Find all peers who have completed the whole given block of tasks and the completion date of the last task
-- Procedure parameters: 
-- name of the block, for example “CPP”.
-- The result is sorted by the date of completion.
-- Output format: 
-- peer's name,
-- date of completion of the block (i.e. the last completed task from that block)

CREATE
    OR REPLACE FUNCTION completed_block(block TEXT)
    RETURNS TABLE
            (
                "Peer" VARCHAR(255),
                "Day"  DATE
            )
AS
$$
BEGIN
    RETURN QUERY
        SELECT peer, date
        FROM checks
                 JOIN p2p
                      ON checks.id = p2p.checkid
        WHERE p2p.status = 'Success'
          AND task = (
            SELECT MAX(title)
            FROM tasks
            WHERE title SIMILAR TO (block || '[0-9]')
        )
        ORDER BY date;
END;
$$
    LANGUAGE plpgsql;

SELECT *
FROM completed_block('SQL');


-------------------------------- 8 ---------------------------------

-- Determine which peer each student should go to for a check.
-- You should determine it according to the recommendations of the peer's friends, 
-- i.e. you need to find the peer with the greatest number of friends who recommend to be checked by him.
-- Output format: 
-- peer's nickname,
-- nickname of the checker found

CREATE
    OR REPLACE FUNCTION recommended_checker()
    RETURNS TABLE
            (
                "Peer"            VARCHAR(255),
                "RecommendedPeer" VARCHAR(255)
            )
AS
$$
BEGIN
    RETURN QUERY WITH all_recommendations AS (
        SELECT ffp.nickname, ffp.peer2, recommendedpeer
        FROM recommendations AS rec
                 JOIN
             (SELECT nickname, friends.peer2
              FROM peers
                       JOIN friends ON peer1 = nickname
             ) AS ffp ON ffp.peer2 = rec.peer
    ),
                      peer_mentions AS (
                          SELECT nickname, recommendedpeer, COUNT(recommendedpeer) AS mentions
                          FROM all_recommendations
                          GROUP BY recommendedpeer, nickname
                      )
                 SELECT DISTINCT nickname, recommendedpeer
                 FROM peer_mentions AS pm
                 WHERE pm.mentions = (
                     SELECT MAX(mentions)
                     FROM peer_mentions AS pm2
                     WHERE pm.nickname = pm2.nickname
                 )
                   AND nickname != recommendedpeer;
END;
$$
    LANGUAGE plpgsql;

SELECT *
FROM recommended_checker();

-------------------------------- 9 ---------------------------------

-- Determine the percentage of peers who:
-- Started only block 1
-- Started only block 2
-- Started both
-- Have not started any of them
-- A peer is considered to have started a block if he has at least one check of any task 
-- from this block (according to the Checks table)

-- Procedure parameters: 
-- name of block 1, for example SQL,
-- name of block 2, for example A.
-- Output format: 
-- percentage of those who started only the first block,
-- percentage of those who started only the second block,
-- percentage of those who started both blocks,
-- percentage of those who did not started any of them

CREATE
    OR REPLACE FUNCTION percentage_two_blocks(block1 TEXT, block2 TEXT)
    RETURNS TABLE
            (
                "StartedBlock1"      NUMERIC,
                "StartedBlock2"      NUMERIC,
                "StartedBothBlocks"  NUMERIC,
                "DidntStartAnyBlock" NUMERIC
            )
AS
$$
DECLARE
    total NUMERIC := (SELECT COUNT(*)
                      FROM peers);
    started_block1
          NUMERIC := (SELECT COUNT(*)
                      FROM (
                               SELECT DISTINCT peer
                               FROM checks
                               WHERE task SIMILAR TO (block1 || '1')
                           ) AS bl1
    );
    started_block2
          NUMERIC := (SELECT COUNT(*)
                      FROM (
                               SELECT DISTINCT peer
                               FROM checks
                               WHERE task SIMILAR TO (block2 || '1')
                           ) AS bl2
    );
    started_both
          NUMERIC := (SELECT COUNT(*)
                      FROM (
                               SELECT DISTINCT peer
                               FROM checks
                               WHERE task SIMILAR TO (block1 || '1')
                               INTERSECT
                               SELECT DISTINCT peer
                               FROM checks
                               WHERE task SIMILAR TO (block2 || '1')
                           ) AS bth
    );
    not_started
          NUMERIC := total - (started_block1 + started_block2 - started_both);
BEGIN
    RETURN QUERY
        SELECT ROUND(started_block1 / total * 100),
               ROUND(started_block2 / total * 100),
               ROUND(started_both / total * 100),
               ROUND(not_started / total * 100);
END;
$$
    LANGUAGE plpgsql;

SELECT *
FROM percentage_two_blocks('SQL', 'A');

-------------------------------- 10 ---------------------------------

-- Determine the percentage of peers who have ever successfully passed a check on their birthday
-- Also determine the percentage of peers who have ever failed a check on their birthday.
-- Output format: 
-- percentage of peers who have ever successfully passed a check on their birthday,
-- percentage of peers who have ever failed a check on their birthday


CREATE
    OR REPLACE FUNCTION check_on_birthday()
    RETURNS TABLE
            (
                "SuccessfulChecks"   NUMERIC,
                "UnsuccessfulChecks" NUMERIC
            )
AS
$$
DECLARE
    total NUMERIC := (SELECT COUNT(*)
                      FROM peers);
    successful
          NUMERIC := (SELECT COUNT(*)
                      FROM (
                               SELECT peer, date
                               FROM checks
                                        JOIN peers ON nickname = peer
                                        JOIN p2p ON checks.id = p2p.checkid
                                        JOIN verter ON checks.id = verter.checkid
                               WHERE to_char(checks.date, 'MM-DD') = to_char(peers.birthday, 'MM-DD')
                                 AND p2p.status = 'Success'
                                 AND verter.status = 'Success'
                           ) AS success_on_birthday
    );
    failure
          NUMERIC := (SELECT COUNT(*)
                      FROM (
                               SELECT peer, date
                               FROM checks
                                        JOIN peers ON nickname = peer
                                        JOIN p2p ON checks.id = p2p.checkid
                                        JOIN verter ON checks.id = verter.checkid
                               WHERE to_char(checks.date, 'MM-DD') = to_char(peers.birthday, 'MM-DD')
                                 AND (p2p.status = 'Failure' OR verter.status = 'Failure')
                           ) AS failure_on_birthday
    );
BEGIN
    RETURN QUERY
        SELECT ROUND(successful / total * 100),
               ROUND(failure / total * 100);
END;
$$
    LANGUAGE plpgsql;

SELECT *
FROM check_on_birthday();

-------------------------------- 11 ---------------------------------

-- Determine all peers who did the given tasks 1 and 2, but did not do task 3
-- Procedure parameters: 
-- names of tasks 1, 2 and 3.
-- Output format: 
-- list of peers

CREATE
    OR REPLACE FUNCTION two_yes_third_not(one TEXT, two TEXT, three TEXT)
    RETURNS TABLE
            (
                "Peer" VARCHAR(255)
            )
AS
$$
BEGIN
    RETURN QUERY
        SELECT peer
        FROM checks
        WHERE task = one
        UNION
        SELECT peer
        FROM checks
        WHERE task = two
        EXCEPT
        SELECT peer
        FROM checks
        WHERE task = three;
END;
$$
    LANGUAGE plpgsql;

SELECT *
FROM two_yes_third_not('C1', 'DO1', 'SQL1');

-------------------------------- 12 ---------------------------------

-- Using recursive common table expression, output the number of preceding tasks for each task
-- I. e. How many tasks have to be done, based on entry conditions, to get access to the current one.
-- Output format: 
-- task name,
-- number of preceding tasks


CREATE
    OR REPLACE FUNCTION count_preceding_tasks(current_task TEXT, OUT counter INT)
    RETURNS INTEGER
AS
$$
BEGIN
    WITH RECURSIVE r AS (
        SELECT title,
               0 AS iteration
        FROM tasks
        WHERE title = current_task

        UNION ALL

        SELECT t.parenttask,
               r.iteration + 1
        FROM tasks t
                 JOIN r ON t.title = r.title
    )
    SELECT INTO counter MAX(iteration) - 1
    FROM r
    WHERE title IS NULL;
END;
$$
    LANGUAGE plpgsql;


CREATE
    OR REPLACE FUNCTION preceding_tasks()
    RETURNS TABLE
            (
                "Task"      VARCHAR(255),
                "PrevCount" INT
            )
AS
$$
BEGIN
    RETURN QUERY
        SELECT title, count_preceding_tasks(title)
        FROM tasks;
END;
$$
    LANGUAGE plpgsql;

SELECT *
FROM preceding_tasks();

-------------------------------- 13 ---------------------------------

-- Find "lucky" days for checks. A day is considered "lucky" if it has at least N consecutive successful checks
-- Parameters of the procedure: 
-- the N number of consecutive successful checks .
-- The time of the check is the start time of the P2P step.
-- Successful consecutive checks are the checks with no unsuccessful checks in between.
-- The amount of XP for each of these checks must be at least 80% of the maximum.
-- Output format: 
-- list of days

CREATE
    OR REPLACE FUNCTION lucky_days(N INT)
    RETURNS TABLE
            (
                "Day" DATE
            )
AS
$$
DECLARE
    successful_checks INT = 0;
    first_fail
                      INT = 0;
    last_counter
                      INT := 0;
    cur_date
                      DATE;
    current_check
                      INT;
    check_info
                      RECORD;
BEGIN
    FOR cur_date IN (
        SELECT DISTINCT date
        FROM checks
                 JOIN xp ON checks.id = xp.checkid
                 JOIN tasks AS t ON t.title = checks.task
        WHERE xp.xpamount >= 0.8 * t.maxxp
        ORDER BY 1
    )
        LOOP
            successful_checks = 0;
            first_fail
                = 0;
            FOR check_info IN (
                SELECT checks.id, date, status, time
                FROM checks
                         JOIN p2p ON p2p.checkid = checks.id
                WHERE checks.date = cur_date
                  AND p2p.status != 'Start'
            )
                LOOP
                    IF check_info.status = 'Success' THEN
                        successful_checks := successful_checks + 1;
                    ELSEIF
                        check_info.status = 'Failure' THEN
                        first_fail := successful_checks + 1;
                        successful_checks
                            := 0;
                    END IF;

                END LOOP;

            IF
                successful_checks >= N OR first_fail > N THEN
                RETURN QUERY
                    SELECT cur_date;
            END IF;

        END LOOP;
END;
$$
    LANGUAGE plpgsql;

SELECT lucky_days(2);


-------------------------------- 14 ---------------------------------

-- Find the peer with the highest amount of XP
-- Output format: 
-- peer's nickname,
-- amount of XP

CREATE
    OR REPLACE FUNCTION max_xp()
    RETURNS TABLE
            (
                "Peer" VARCHAR(255),
                "XP"   BIGINT
            )
AS
$$
BEGIN
    RETURN QUERY
        SELECT peer, SUM(xpamount)
        FROM (
                 SELECT peer, xpamount
                 FROM checks
                          JOIN xp ON checks.id = xp.checkid
             ) AS all_peers
        GROUP BY peer
        ORDER BY 2 DESC
        LIMIT 1;
END;
$$
    LANGUAGE plpgsql;

SELECT *
FROM max_xp();

-------------------------------- 15 ---------------------------------

-- Determine the peers that came before the given time 
-- at least N times during the whole time
-- Procedure parameters: 
-- time,
-- N number of times .
-- Output format: 
-- list of peers

CREATE
    OR REPLACE FUNCTION came_before(day DATE, N INT)
    RETURNS TABLE
            (
                "Peer" VARCHAR(255)
            )
AS
$$
BEGIN
    RETURN QUERY
        SELECT peer
        FROM (
                 SELECT peer, COUNT(*) AS times
                 FROM timetracking
                 WHERE date < day
                   AND status = 1
                 GROUP BY peer
             ) AS all_visits
        WHERE times >= N;

END;
$$
    LANGUAGE plpgsql;

SELECT *
FROM came_before('2023-12-12', 2);

-------------------------------- 16 ---------------------------------

-- Determine the peers who left the campus more than M times during the last N days
-- Procedure parameters: 
-- N number of days,
-- M number of times .
-- Output format: 
-- list of peers

CREATE
    OR REPLACE FUNCTION left_campus(num_of_days INT, M_times INT)
    RETURNS TABLE
            (
                "Peer" VARCHAR(255)
            )
AS
$$
BEGIN
    RETURN QUERY
        SELECT peer
        FROM (
                 SELECT peer, COUNT(*) AS times
                 FROM timetracking
                 WHERE date > (CURRENT_DATE - num_of_days)
                   AND date < CURRENT_DATE
                   AND status = 2
                 GROUP BY peer
             ) AS all_visits
        WHERE times >= M_times;
END;
$$
    LANGUAGE plpgsql;

SELECT *
FROM left_campus(10, 1);


-------------------------------- 17 ---------------------------------

-- Determine for each month the percentage of early entries
-- For each month, count how many times people born in that month 
-- came to campus during the whole time (we'll call this the total number of entries).

-- For each month, count the number of times people born in that month 
-- have come to campus before 12:00 in all time (we'll call this the number of early entries).

-- For each month, count the percentage of early entries to campus relative to the total number of entries.
-- Output format: 
-- month,
-- percentage of early entries

CREATE
    OR REPLACE FUNCTION early_entries()
    RETURNS TABLE
            (
                "Month"        TEXT,
                "EarlyEntries" NUMERIC
            )
AS
$$
DECLARE
BEGIN
    RETURN QUERY WITH birthmonths AS (
        SELECT nickname, to_char(peers.birthday, 'Month') AS month
        FROM peers
    ),
                      birthmonth_entries AS (
                          SELECT to_char(tt.date, 'Month')  AS month,
                                 EXTRACT(hour FROM tt.time) AS time,
                                 COUNT(*)                   AS num
                          FROM timetracking AS tt
                                   JOIN birthmonths AS bm ON bm.nickname = tt.peer
                          WHERE bm.month = to_char(tt.date, 'Month')
                            AND status = 1
                          GROUP BY 1, 2
                      ),
                      early_entries AS (
                          SELECT month, COUNT(*) AS num
                          FROM birthmonth_entries AS be
                          WHERE time < 12
                          GROUP BY 1
                      ),
                      months AS (
                          SELECT to_char(DATE '2023-01-01' +
                                         (interval '1' month * generate_series(0, 11)), 'Month') AS month
                      )

                 SELECT months.month, COALESCE(ROUND(ee.num::numeric / be.num::numeric * 100), 0) AS EarlyEntries
                 FROM early_entries AS ee
                          JOIN birthmonth_entries AS be ON ee.month = be.month
                          FULL JOIN months ON months.month = ee.month;
END;
$$
    LANGUAGE plpgsql;

SELECT *
FROM early_entries();



-------------------------------- 18 ---------------------------------

-- Let’s create a Database Trigger Function with the name `fnc_trg_person_insert_audit` 
-- that should process `INSERT` DML traffic and make a copy of a new row to the person_audit table.


CREATE TABLE person_audit (
    created TIMESTAMP WITH TIME ZONE,
    type_event VARCHAR(1) DEFAULT 'I' NOT NULL,
    row_id BIGINT NOT NULL,
    name VARCHAR,
    age INT,
    gender VARCHAR,
    address VARCHAR,
    CONSTRAINT ch_type_event CHECK (type_event IN ('I', 'U', 'D'))
);

CREATE OR REPLACE FUNCTION fnc_trg_person_insert_audit()
RETURNS trigger AS
$BODY$
BEGIN
INSERT INTO person_audit
VALUES (current_timestamp, 'I', NEW.id, NEW.name, NEW.age, NEW.gender, NEW.address);
RETURN NULL;
END;
$BODY$
LANGUAGE plpgsql;

CREATE TRIGGER trg_person_insert_audit
    AFTER INSERT ON person
    FOR EACH ROW
    EXECUTE PROCEDURE fnc_trg_person_insert_audit();

INSERT INTO person(id, name, age, gender, address)
VALUES (10,'Damir', 22, 'male', 'Irkutsk');


-------------------------------- 19 ---------------------------------

-- Create a pl/pgsql function  `fnc_person_visits_and_eats_on_date` based on SQL statement that finds the 
-- names of pizzerias which person (`IN` pperson parameter with default value is ‘Dmitriy’) visited and in which he 
-- could buy pizza for less than the given sum in rubles (`IN` pprice parameter with default value is 500) on the specific date 
-- (`IN` pdate parameter with default value is 8th of January 2022).

CREATE OR REPLACE FUNCTION fnc_person_visits_and_eats_on_date(pperson VARCHAR DEFAULT 'Dmitriy',
                        pprice NUMERIC DEFAULT 500, pdate DATE DEFAULT '2022-01-08')
RETURNS SETOF VARCHAR AS
$BODY$
BEGIN
    RETURN QUERY
    SELECT pizzeria.name
    FROM pizzeria
    JOIN menu ON menu.pizzeria_id = pizzeria.id
    JOIN person_visits pv ON pv.pizzeria_id = pizzeria.id
    JOIN person ON pv.person_id = person.id
    WHERE person.name = pperson AND menu.price < pprice AND pv.visit_date = pdate;
RETURN;
END;
$BODY$
LANGUAGE plpgsql;


SELECT *
FROM fnc_person_visits_and_eats_on_date(pprice := 800);

SELECT *
FROM fnc_person_visits_and_eats_on_date(pperson := 'Anna',pprice := 1300,pdate := '2022-01-01');


-------------------------------- 20 ---------------------------------
-- Write a SQL or pl/pgsql function `func_minimum` 
-- that has an input parameter is an array of numbers and the function should return a minimum value.


CREATE OR REPLACE FUNCTION func_minimum(VARIADIC arr NUMERIC[])
RETURNS NUMERIC AS $$
    SELECT MIN(arr[i]) FROM generate_subscripts(arr, 1) AS g(i);
$$
LANGUAGE SQL;

SELECT func_minimum(VARIADIC arr => ARRAY[10.0, -1.0, 5.0, 4.4]);



-------------------------------- 21 ---------------------------------
-- Write a SQL or pl/pgsql function `fnc_fibonacci` that has an input parameter pstop with type integer 
-- (by default is 10) and the function output is a table with all Fibonacci numbers less than pstop.


CREATE OR REPLACE FUNCTION fnc_fibonacci(pstop INTEGER DEFAULT 10)
RETURNS TABLE (f INT) AS $$
    WITH RECURSIVE fibonacci(a, b) AS (
        SELECT 0 AS a, 1 AS b
        UNION ALL
        (SELECT b, a + b
        FROM fibonacci
        WHERE b < pstop)
    )
    SELECT a FROM fibonacci;
$$
LANGUAGE SQL;

SELECT * FROM fnc_fibonacci(100);
SELECT * FROM fnc_fibonacci();