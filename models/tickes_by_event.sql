with events as (
  select * 
  from {{ source('ticket_tailor', 'events') }}
),
orders as (
  select *
  from {{ source('ticket_tailor', 'orders') }}
),
issued_tickets as (
    select *
    from {{ source('ticket_tailor', 'issued_tickets') }}
),
tickets_by_event as (
    select T.status, T.event_id, COUNT(T.ID) as tickets_count
    from issued_tickets AS T
    left join orders AS O on T.order_id=O.id
    group by 1,2
),
refunded_amount_and_sold_tickets_by_event as (
    select T.status, T.event_id, E.name, COUNT(T.ID) as tickets_count, sum(refund_amount) as refunded_amount
    from issued_tickets AS T
    inner join events AS E ON T.event_id=E.id
    left join orders AS O on T.order_id=O.id
    group by 1,2,3
)
 
SELECT status, SUM(tickets_count) AS total_tickets_count, SUM(refunded_amount) AS total_refunded_amount
FROM refunded_amount_and_sold_tickets_by_event
GROUP BY 1
