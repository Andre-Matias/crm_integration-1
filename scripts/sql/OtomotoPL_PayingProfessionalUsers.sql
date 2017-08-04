-- Paying Professional Users Otomoto PL ---------------------------------------
select
  date_format(date, '%Y-%m') as time, u.is_business,
  count(distinct p.id_user) as cnt
from paidads_user_payments p
inner join users u on u.id = p.id_user
where
date >= '2015-04-01'
and is_removed_from_invoice = 0
and is_invalid_item = 0
and payment_provider = 'postpay'
and price<0 and not (
  u.email LIKE "%@sunfra.%"
  OR u.email LIKE "%@olx.%"
  OR u.email LIKE "%@tablica.%"
  OR u.email LIKE "%@fixeads.%"
  OR u.email LIKE "%@otomoto.%"
  OR u.email LIKE "%@otodom.%"
  OR u.email LIKE "%@slando.%")
and u.is_business=1
group by 1,2;