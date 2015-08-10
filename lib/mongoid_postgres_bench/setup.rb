require 'mongoid'
require 'pg'
require_relative "documents/event"
require_relative "documents/url"
require_relative "documents/publisher"


module MongoidPostgresBench
  conn = PG::Connection.connect(dbname: 'evvnt-dev', host: 'localhost')
  res = conn.query(' select urls.order_id as index, event_id, urls.value, urls.real_url, urls.bitly_referrers_json, urls.id
                     from urls
                     where urls.event_id = 25
                     group by event_id, urls.id, urls.bitly_referrers_json
                     order by event_id asc, urls.id asc;')
end