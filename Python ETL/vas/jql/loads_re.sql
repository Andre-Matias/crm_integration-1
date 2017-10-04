function getDay(event) {return (new Date(event.time)).toISOString().split('T')[0];}
function main() {
  return Events({
    from_date: 'FROM_DATE_REPLACE',
    to_date:   'TO_DATE_REPLACE'
  })
  .filter(e=>e.name==='ad_page')
  .filter(e=>e.properties.ad_id!==undefined)
  .filter(e=>e.properties.ad_id!==null)
  .groupBy([
    getDay,
    'properties.ad_id',
    function(item){return Number(item.properties.ad_photo)},
    function(item){return Number(item.properties.ad_price)}
    ], mixpanel.reducer.count())
  .map(function(item){
    var obj = {};
    obj.date = item.key[0];
    obj.id = item.key[1];
    obj.nb_pictures = item.key[2];
    obj.price = item.key[3];
    obj.loads = item.value;
    return obj;
  });
}