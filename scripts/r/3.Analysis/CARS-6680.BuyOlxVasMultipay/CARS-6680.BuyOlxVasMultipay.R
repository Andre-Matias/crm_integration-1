function main() {
  return Events({
    from_date: '2018-05-16',
    to_date:   '2018-05-23'
  })
  .filter(
    e =>
      e.name.indexOf("ab_test_multipay") > -1
    && e.properties.experiments
    && e.properties.experiments.toString().indexOf("10677703738:") > -1
  )
  .groupBy(["name", "properties.buy_vas_olx"], mixpanel.reducer.count());
}