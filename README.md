# PostgresBenchmarks

## Description

The gem is designed to allow the user to insert the number of records they wish to benchmark database performance against.

For the needs of evvnt, we wish to test the performance of storing our click data in a json column or in a normalised form.

We will benchmark insert, update, and read along with various queries for these methods and see which is better for us.

## Usage

```
pry -r ./app.rb
```

```ruby
[1] pry(main)> PostgresBenchmarks::Database::Test.setup
[2] pry(main)> data = PostgresBenchmarks::Data.new
[3] pry(main)> data.generate
[4] pry(main)> Reading.new(events: 5, publishers: 1, urls: 1, num_referrers: 1, num_days: 5, times: 1).run
```

`data.generate` populates the database with the data we can use to query.

Tests:
```
Inserting
Reading
Updating
Querying
```

All but the querying should be working at the moment. Querying relies on a large and reliable data set being generated for querying. This work has not been completed yet.


### Defaults

The number of these is the product of the number of urls on the event and the number of publishers.

The default number of urls is 3 and the default number of publishers is 38. This is derived from the average numbers we see in the application at this time.

These defaults can be overridden when running a test.

`Reading.new(events: 5, publishers: 1, urls: 1, num_referrers: 1, num_days: 5, times: 1).run`

## Debugging adnd Development workflow

I will run the pry command requireing the app file and load the data in pry like:

```
data = data = PostgresBenchmarks::Data.new(events:50, publishers:30, urls:3, num_referrers:10, num_days:10, times:2)
data.generate
```
