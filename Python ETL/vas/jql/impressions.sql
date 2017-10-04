function getDay(event) {return (new Date(event.time)).toISOString().split('T')[0];}
function main() {
  return Events({
    from_date: 'FROM_DATE_REPLACE',
    to_date:   'TO_DATE_REPLACE'
  })
  .filter(e=>e.name==='ads_impression')
  .groupBy([getDay,mixpanel.multiple_keys(function(event){
    if (event.properties.mp_lib==='android'){return Number(event.properties.ad_impressions)}
    else if (event.properties.mp_lib==='iphone'){return JSON.parse(event.properties.ad_impressions)}
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