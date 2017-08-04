USE autovitro;

SELECT
  date_format(fi.created_at, '%Y-%m')as time,
  count(distinct ub.id) as cnt
FROM fk_invoices fi
INNER JOIN users_business ub on ub.id = fi.user_id
WHERE fi.created_at >= '2016-01-01'
and fi.gross_amount_cents > 0
and ub.id not in(521225, 521223, 521213, 521215, 522479,
522495, 24280, 419691, 522665, 18964, 1030125, 522513,
1057085, 1058565, 1066171, 1285659, 1285679, 829479,
17628)
and not (ub.email LIKE "%@autovit%"
         OR ub.email LIKE"%@sunfra.%"
         OR ub.email LIKE "%@olx.%"
         OR ub.email LIKE "%@tablica.%"
         OR ub.email LIKE "%@fixeads.%"
         OR ub.email LIKE "%@otomoto.%"
         OR ub.email LIKE "%@otodom.%"
         OR ub.email LIKE "%@slando.%"
)
group by 1
order by 1 ASC;