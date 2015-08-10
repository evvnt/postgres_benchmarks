conn = PG::Connection.connect(dbname: 'evvnt-dev', host: 'localhost')
puts "Opened connection to postgres"

# res = conn.query('select events.id, bitly_referrers_json from urls join events on events.id = urls.event_id;')

#fetching the query
puts 'select urls.order_id as index, event_id, urls.value, urls.real_url, urls.bitly_referrers_json, urls.id
                 from urls
                 group by event_id, urls.id, urls.bitly_referrers_json
                 order by event_id asc, urls.order_id asc, urls.id asc;'


res = conn.query('select urls.order_id as index, event_id, urls.value, urls.real_url, urls.bitly_referrers_json, urls.id
                 from urls
                 group by event_id, urls.id, urls.bitly_referrers_json
                 order by event_id asc, urls.order_id asc, urls.id asc;')


json_data = []
puts "inserting each row"
res_count = res.count 
res.each do |row|
  puts "row #{row['id']} of #{res_count.to_s}"
  e = Event.find_or_create_by(id: row['event_id'] )
  e.urls.create!(bitly_referrers_json: JSON.parse(row['bitly_referrers_json']||'[]'), value: row['real_url'], index: row['index'])
end

puts "Finished"
