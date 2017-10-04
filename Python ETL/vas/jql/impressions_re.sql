function getDay(event) {return (new Date(event.time)).toISOString().split('T')[0];}
function main() {
  return Events({
    from_date: 'FROM_DATE_REPLACE',
    to_date:   'TO_DATE_REPLACE'
  })
  .filter(e=>e.name==='ads_view' || (e.name==='listing' && e.properties.mp_lib ==='web'))
  .groupBy([getDay,mixpanel.multiple_keys(function(event){
    if (event.properties.mp_lib==='android'){return Number(event.properties.ad_impressions)}
    else if (event.properties.mp_lib==='iphone' && event.properties.ad_impressions.substring(event.properties.ad_impressions.length-1) === ']'){return JSON.parse(event.properties.ad_impressions)}
    else if (event.properties.mp_lib==='iphone' && event.properties.ad_impressions.substring(event.properties.ad_impressions.length-1) === ','){
      removeComma = event.properties.ad_impressions.slice(0,-1);
      return JSON.parse(removeComma + ']')}
    else if (event.properties.mp_lib==='web'){return event.properties.ad_impressions}
  })],mixpanel.reducer.count())
    .map(function(item){
    var obj = {};
    obj.date = item.key[0];
    obj.id = item.key[1];
    obj.views = item.value;
    return obj;
  })
  .groupBy([
    function(i){var date = i.date;delete i.date;return date},
    function(i){var id = i.id;delete i.id;return id}
    ], mixpanel.reducer.object_merge())
  .filter(e=>e.key[1]>0)
  .map(function(item){
    var obj = {};
    obj.date = item.key[0];
    obj.id = item.key[1];
    obj.impressions = item.value.views;
    return obj;
  })
}