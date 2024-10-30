/* Отметьте в отдельной таблице тех курьеров, которые доставили в сентябре 2022 года заказов больше, чем в среднем все курьеры.*/

SELECT courier_id,
       delivered_orders,
       round(avg_delivered_orders, 2) as avg_delivered_orders,
       case when delivered_orders < avg_delivered_orders then 0
            else 1 end as is_above_avg
FROM   (SELECT courier_id,
               count(order_id) as delivered_orders,
               avg(count(order_id)) OVER() as avg_delivered_orders
        FROM   courier_actions
        WHERE  date_part('month', time) = 9
           and date_part('year', time) = 2022
           and action = 'deliver_order'
        GROUP BY courier_id) t1


/* Из таблицы courier_actions отберите топ 10% курьеров по количеству доставленных за всё время заказов. 
У курьера, доставившего наибольшее число заказов, порядковый номер должен быть равен 1, 
а у курьера с наименьшим числом заказов — числу, равному десяти процентам от общего количества курьеров в таблице courier_actions. */

with ten_persent as (SELECT count(distinct courier_id) as top
                     FROM   courier_actions)
SELECT courier_id,
       orders_count,
       courier_rank
FROM   (SELECT courier_id,
               count(distinct order_id) as orders_count,
               row_number() OVER (ORDER BY count(distinct order_id) desc, courier_id) as courier_rank
        FROM   courier_actions
        WHERE  action = 'deliver_order'
        GROUP BY 1
        ORDER BY orders_count desc, courier_id) t1
WHERE  courier_rank <= ((SELECT *
                         FROM   ten_persent) * 0.1)::integer



/*Для каждой записи в таблице user_actions с помощью оконных функций и предложения FILTER посчитайте, 
сколько заказов сделал и сколько отменил каждый пользователь на момент совершения нового действия.

Колонки с накопительными суммами числа оформленных и отменённых заказов назовите соответственно created_orders и 
canceled_orders. На основе этих двух колонок для каждой записи пользователя посчитайте показатель cancel_rate, 
т.е. долю отменённых заказов в общем количестве оформленных заказов. Колонку с ним назовите cancel_rate. */

SELECT user_id,
       order_id,
       action,
       time,
       created_orders,
       canceled_orders,
       round(canceled_orders / created_orders ::numeric, 2) as cancel_rate
FROM   (SELECT user_id,
               order_id,
               action,
               time,
               count(order_id) filter (WHERE action = 'create_order') OVER w as created_orders,
               count(order_id) filter (WHERE action = 'cancel_order') OVER w as canceled_orders
        FROM   user_actions window w as (
        PARTITION BY user_id
        ORDER BY time)
        ORDER BY user_id, order_id, time limit 1000) t1



/*Для каждого дня посчитайте долю первых и повторных заказов. 
Сохраните структуру полученной ранее таблицы и добавьте только одну новую колонку с посчитанными значениями.
Колонку с долей заказов каждой категории назовите orders_share.*/

SELECT date,
       order_type,
       count(order_type) as orders_count,
       round(count(order_type) / sum(count(order_type)) OVER (PARTITION BY date),
             2) as orders_share
FROM   (SELECT date(time) as date,
               case when row_number() OVER (PARTITION BY user_id
                                            ORDER BY time) = '1' then 'Первый'
                    else 'Повторный' end as order_type
        FROM   user_actions
        WHERE  order_id not in (SELECT order_id
                                FROM   user_actions
                                WHERE  action = 'cancel_order')) t1
GROUP BY date, order_type
ORDER BY date, order_type

/* На основе таблицы orders сформируйте новую таблицу с общим числом заказов по дням.
Посчитайте скользящее среднее числа заказов.
При подсчёте числа заказов не учитывайте отменённые заказы (их можно определить по таблице user_actions). 
Скользящее среднее для каждой записи считайте по трём предыдущим дням. */

with canceled as (SELECT order_id
                  FROM   user_actions
                  WHERE  action = 'cancel_order'), total_orders as (SELECT count(order_id) as orders_count,
                                                         date(creation_time) as date
                                                  FROM   orders
                                                  WHERE  order_id not in (SELECT *
                                                                          FROM   canceled)
                                                  GROUP BY date)
SELECT date,
       orders_count,
       round(avg(orders_count) OVER(ORDER BY date rows between 3 preceding and 1 preceding),
             2) as moving_avg
FROM   total_orders


/* Для каждого пользователя рассчитайте, сколько в среднем времени проходит между его заказами. 
Посчитайте этот показатель только для тех пользователей, которые за всё время оформили более одного неотмененного заказа.*/

with t1 as (SELECT user_id,
                   order_id,
                   time,
                   time - lag(time, 1) OVER(PARTITION BY user_id
                                            ORDER BY time) as lag_time
            FROM   user_actions
            WHERE  order_id not in (SELECT order_id
                                    FROM   user_actions
                                    WHERE  action = 'cancel_order'))
SELECT user_id,
       (extract(epoch
FROM   avg(lag_time)) / 3600)::integer as hours_between_orders
FROM   t1
GROUP BY user_id having count(*) > 1
ORDER BY user_id limit 1000


/* Для каждого заказа каждого пользователя рассчитайте, сколько времени прошло с момента предыдущего заказа. */

SELECT user_id,
       order_id,
       time,
       row_number() OVER(PARTITION BY user_id
                         ORDER BY time) as order_number,
       lag(time, 1) OVER(PARTITION BY user_id rows 1 preceding) as time_lag,
       time - lag(time,
                                1) OVER(PARTITION BY user_id rows between 1 preceding and current row) as time_diff
FROM   user_actions
WHERE  order_id not in (SELECT order_id
                        FROM   user_actions
                        WHERE  action = 'cancel_order')
ORDER BY user_id, order_number limit 1000