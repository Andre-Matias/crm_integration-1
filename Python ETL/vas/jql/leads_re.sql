repliesReference = ['reply_phone_show','reply_message_sent','reply_chat_sent','reply_phone_call','reply_phone_sms'];
function getDay(event) {return (new Date(event.time)).toISOString().split('T')[0];}
function main() {
  return Events({
    from_date: '2017-10-02',
    to_date:   '2017-10-02'
  })
  .filter(e=>_.contains(repliesReference,e.name)===true)
  .filter(e=>e.properties.ad_id!==undefined)
  .filter(e=>e.properties.ad_id!==null)
  .groupBy([
    getDay,
    'properties.ad_id'
    ],mixpanel.reducer.count())
    .map(function(item){
      var obj = {};
      obj.date = item.key[0];
      obj.id = item.key[1];
      obj.leads = item.value;
      return obj;
    });
}