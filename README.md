# PostgresBenchmarks

TODO: Write a gem description

## Installation

Add this line to your application's Gemfile:

    gem 'postgres-jsonb-bench'

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install postgres-jsonb-bench

## Usage

The gem is designed to allow the user to insert teh number of records they wish to benchmark database performance against.

For the needs of evvnt, we wish to test the performance of storing our click data in a json column or in a normalised form.

We will benchmark insert, update, and read for these methods and see which is better for us.


`pry -r ./app.rb`
[1] pry(main)> PostgresBenchmarks::Database::Test.setup
[1] pry(main)> Reading.new.run

### Defaults

when generating a test, we will create EventPublisherUrls. Each of these will represent a url that represents a publisher on an event.

the number of these is the product of the number of urls on the event and the number of publishers.

the default number of urls is 3 and the default number of publishers is 38. This is derived from the average numbers we see in the application at this time.

These defaults can be overridden when running a test.

Reading.new(publishers: 1, urls: 1, num_referrers: 1, times: 1).run

## Contributing

1. Fork it ( https://github.com/[my-github-username]/postgres-jsonb-bench/fork )
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request
