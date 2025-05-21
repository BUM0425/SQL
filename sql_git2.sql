Задача 1
Необходимо получить список сотрудников в формате: «Иванова — Наталья – Юрьевна». ФИО должно быть прописано в одном столбике, разделение —.
Вывести: новое поле, назовем его fio, birth_dt.

select birth_dt, concat(first_nm, '-', middle_nm, '-', last_nm) as fio
from Employees



Задача 2
Вывести %% дозвона для каждого дня. Период с 01.10.2020 по текущий день (%% дозвона – это доля принятых звонков (dozv_flg=1) от всех поступивших звонков 
(dozv_flg = 1 or dozv_flg = 0)).
Вывести: date, sla (%% дозвона)

select start_dttm::timstamp as date, count(*) filter(dozv_flg = 1) / count(*) as sla
from Calls
where start_dttm::timestamp >= '2020-10-01'
group by date_trunc('day', start_dttm::timestamp)

Задача 3
Дана таблица clinets:
id клиента
calendar_at - дата входа в мобильное приложение
Нужно написать запрос для расчета MAU.


with st_dt as (
  select *, min(calendar_at) over(partition by id) as start_dt
  from clinets)
year_month as (
  select *, case
                when date_trunc('year', start_dt) = date_trunc('year', calendar_at) and date_trunc('month', start_dt) = date_trunc('month', calendar_at) 
                then 1
                else 0
            end as grp
  from st_dt)
select id, date_trunc('month', calendar_at) as month, count(*) as count_new_id
from year_month
where grp = 1
group by date_trunc('month', calendar_at), id





