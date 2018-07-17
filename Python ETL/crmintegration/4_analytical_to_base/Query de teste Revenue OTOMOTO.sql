insert into forbi.calculation
select w.id, w.id_index, w.date, w.category, w.category_no, w.user_id, w.invoice, w.package, w.diff, w.rank  from
(select pup.id AS id, pup.date, pup.id_index, 
CASE when (ad.sup_category_id = 65 and pup.id_index in (115, 117)) then 'M'
when ad.sup_category_id = 161 then 'P'
when ad.sup_category_id is null then 'C'
else 'C' end as category,
ad.sup_category_id as category_no, 
pup.id_user AS user_id,
DATE_SUB(ub.next_invoice_date, INTERVAL -(TIMESTAMPDIFF(MONTH, ub.next_invoice_date, pup.DATE)) MONTH) as invoice,
CASE when bp.package_id > 3 then bp.package_id - 3 else coalesce(bp.package_id,'1') end as package,
TIMESTAMPDIFF(MONTH, ub.next_invoice_date, pup.DATE) as diff,
cast(0 as unsigned) AS rank
FROM otomotopl.paidads_user_payments pup                   
JOIN otomotopl.users_business ub ON pup.id_user=ub.id
left join forbi.ads_categories ad on pup.id_ad = ad.id
left join otomotopl.billing_periods as bp on pup.id_user = bp.user_id and
(DATE_SUB(ub.next_invoice_date, INTERVAL -(TIMESTAMPDIFF(MONTH, ub.next_invoice_date, pup.DATE)) MONTH) = bp.ending_time OR
DATE_SUB(DATE_SUB(ub.next_invoice_date, INTERVAL 1 DAY), INTERVAL -(TIMESTAMPDIFF(MONTH, ub.next_invoice_date, pup.DATE)) MONTH) = bp.starting_time OR
DATE_SUB(DATE_SUB(ub.next_invoice_date, INTERVAL 2 DAY), INTERVAL -(TIMESTAMPDIFF(MONTH, ub.next_invoice_date, pup.DATE)) MONTH) = bp.starting_time OR
DATE_SUB(DATE_SUB(ub.next_invoice_date, INTERVAL 3 DAY), INTERVAL -(TIMESTAMPDIFF(MONTH, ub.next_invoice_date, pup.DATE)) MONTH) = bp.starting_time)
WHERE payment_provider='postpay' AND pup.DATE>=DATE(DATE_SUB(ub.next_invoice_date, INTERVAL 5 MONTH)) 
AND pup.DATE<DATE(DATE_SUB(DATE_SUB(ub.next_invoice_date, INTERVAL 0 MONTH), INTERVAL 1 DAY))
AND is_removed_from_invoice=0 AND pup.is_invalid_item=0 AND pup.id_index in (51,55,115,117))w
order by w.user_id ASC, w.diff ASC, w.category, w.id ASC;