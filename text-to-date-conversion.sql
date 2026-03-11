ALTER TABLE category_ref
MODIFY create_date DATE;


ALTER TABLE category_ref
MODIFY update_date DATE;
  
  
  ALTER TABLE organization_det
MODIFY created_date DATE;

  
  ALTER TABLE organization_det
MODIFY update_date DATE;

  
  ALTER TABLE purchase_orders
MODIFY reciept_date DATE;


  
  ALTER TABLE sales_det
MODIFY booked_date date;



  ALTER TABLE sales_det
MODIFY shipped_date date;


    ALTER TABLE sales_det
MODIFY invoice_date date;

 ALTER TABLE sales_det
MODIFY promise_date date;


 ALTER TABLE sales_det
MODIFY needby_date date;


update sales_det set shipped_date=NULL where trim(shipped_date)='';



update sales_det set invoice_date=NULL where trim(invoice_date)='';


ALTER TABLE warehouse
MODIFY created_date date;



ALTER TABLE warehouse
MODIFY update_date date;

select * from purchase_orders as po
left join warehouse as wh
on po.warehouse_id=wh.warehouse_id
;


select * from purchase_orders where po_number=87687;
select po_id,count(po_id) from purchase_orders
group by po_id
having count(po_id)>1;
select * from purchase_orders ;


select po_id,count(po_id) from purchase_orders 
group by po_id
having
count(po_id)>1;

select * from purchase_orders as po
left join 
warehouse as wh
on po.warehouse_id=wh.warehouse_id
union
select * from purchase_orders as po
right join 
warehouse as wh
on po.warehouse_id=wh.warehouse_id;


create table new_warehouse 
select * from(
SELECT *,
         ROW_NUMBER() OVER (
           PARTITION BY warehouse_id
           ORDER BY update_date DESC) as row_num
  FROM warehouse
)t where row_num=1;


select * from new_warehouse;
 alter table new_warehouse drop column row_num;
 
 
 
 select * from(
SELECT *,
         ROW_NUMBER() OVER (
           PARTITION BY warehouse_id
           ORDER BY update_date DESC) as row_num
  FROM new_warehouse
)t where row_num=1;


select * from customer2 where customer_id=810;
  select * from customer2 where customer_name='Sarah Brown'
   and customer_id like '2%';
   
create table customer as
select *, row_number() over(
order by customer_id,customer_name,customer_city
) as customer_key from customer2
;

select * from customer;

create table customer as
SELECT s.*, c.customer_name, c.customer_city, c.customer_state, c.customer_country, c.customer_status
FROM sales_det s
LEFT JOIN customer2 c
ON s.cust_id = c.customer_id;

UPDATE customer
SET customer_status = NULL
WHERE TRIM(customer_status) = '';

select * from customer;

rename table customer to sales_det_customer;

select * from customer;



select * from sales_det_customer;

alter table sales_det_customer drop column customer_state;

select * from sales_det_customer;






select * from customer2 where customer_id=810;


select * from sales_det;


select count(*) from new_warehouse;





select * from customer2 where customer_id=810;






SELECT COUNT(*) AS inactive_duplicate_count
FROM (
    SELECT ROW_NUMBER() OVER (
               PARTITION BY customer_id
               ORDER BY customer_id
           ) AS row_num
    FROM customer2
    WHERE customer_status = 'inactive'
) t
WHERE row_num > 1;
 

drop table customer2;



select * from customer2;

delete from customer2 where customer_id in(
SELECT customer_id
FROM (
    SELECT *, ROW_NUMBER() OVER (
               PARTITION BY customer_id
               ORDER BY customer_id
           ) AS row_num
    FROM customer2
    WHERE customer_status = 'inactive'
) t
WHERE row_num > 1
);


delete from customer2 where customer_id in(
SELECT customer_id
FROM (
    SELECT *, ROW_NUMBER() OVER (
               PARTITION BY customer_id
               ORDER BY customer_id
           ) AS row_num
    FROM customer2
    WHERE customer_status = 'inactive'
) t
WHERE row_num > 1
);
   
   
   
   select * from customer2;
   
   
   delete from customer2 where customer_id in(
SELECT customer_id
FROM (
    SELECT *, ROW_NUMBER() OVER (
               PARTITION BY customer_id
               ORDER BY customer_id
           ) AS row_num
    FROM customer2
    WHERE customer_status = 'active'
) t
WHERE row_num > 1
);



select * from customer2 
order by customer_status;

SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (
               PARTITION BY customer_id
               ORDER BY customer_id
           ) AS row_num
    FROM customer2
    WHERE customer_status = 'inactive' 
) t
WHERE row_num > 1;

select * from customer2;

select count(customer_id),customer_id
from customer2
group by customer_id;


select *,row_number() over(partition by customer_id order by customer_status) as row_num
from customer2;

DELETE FROM customer2
WHERE (customer_id, customer_name, customer_city, customer_status) IN (
    SELECT customer_id, customer_name, customer_city, customer_status
    FROM (
        SELECT customer_id,
               customer_name,
               customer_city,
               customer_status,
               ROW_NUMBER() OVER (
                   PARTITION BY customer_id
                   ORDER BY customer_status
               ) AS row_num
        FROM customer2
    ) t
    WHERE row_num > 1
);


select * from customer2;

select * from customer2 where customer_id=817;
select count(customer_id),customer_id
from customer2
group by customer_id;


select * from customer2;

select * ,row_number() over( partition by item_id order by item_id)
from purchase_orders;

	
select * from sales_det;



-- finally worked code
-- customer imported again

select * from customer;


with cte as(
select *,row_number()  over(partition by customer_id order by customer_status) as row_num
from customer
)
select * from cte where row_num>1;


WITH cte AS (
SELECT customer_id,
customer_name,
customer_city,
customer_country,
customer_status,
ROW_NUMBER() OVER (
PARTITION BY customer_id
ORDER BY customer_status
) AS row_num
FROM customer
)
DELETE c
FROM customer c
JOIN cte
ON c.customer_id = cte.customer_id
AND c.customer_name = cte.customer_name
AND c.customer_city = cte.customer_city
AND c.customer_country = cte.customer_country
AND c.customer_status = cte.customer_status
WHERE cte.row_num > 1;



select * from customer;