# Redis::Ick ![TravisCI](https://travis-ci.org/ProsperWorks/redis-ick.svg?branch=master)

Ick: An Indexing QUeue.

Redis::Ick implements a priority queue in Redis with two-phase commit
and write-folding aka write-combining semantics.

Icks are well-suited for dirty lists in data sync systems.

A Redis-based queue-like data structure for message passing in a
many-producer/single-consumer pattern.

Ick is compatible with Redis Cluster and RedisLabs Enterprise Cluster.
Each Ick has a master key and any other Redis keys it uses use a
prescriptive hash based on the master key.

Ick offers write-folding semantics in which re-adding a member already
in queue does not increase the size of the queue.  It may, or may not,
rearrange that member's position within the queue.

Ick is batchy on the consumer side with reliable delivery semantics
using a two-phase protocol: it supports for reserving batches and
later committing all, some, or none of them.

Note that members held in the reserve buffer by the consumer do *not*
write-fold against members being added by producers.

Ick offers atomicity among producer and consumer operations by virtue
of leveraging Lua-in-Redis.

Ick offers starvation-free semantics when scores are approximately the
current time.  When Ick performs write-folding, it always preserves
the *lowest* score seen for a given message.  Thus, in both the
producer set and the consumer set, entries never move further away
from the poppy end.

Ick supports only a single consumer: there is only one buffer for the
two-phase pop protocol.  If you need more than one consumer, shard
messages across multiple Icks each of which routes to one consumer.

incept: 2015-10-01
arch:   https://goo.gl/V1g9I8

```ruby
gem 'redis-ick'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install redis-ick

## Usage

Usage example for producers:

    # one producer
    #
    ick = Ick.new(redis)
    ick.ickadd("mykey",123,"foo",20151001,"bar")

    # another producer
    #
    ick = Ick.new(redis)
    ick.ickadd("mykey",12.8,"foo")
    ick.ickadd("mykey",123.4,"baz")

Usage example for consumer:

    ick     = Ick.new(redis)
    batch   = ick.ickreserve("mykey",BATCH_SIZE)
    members = batch.map { |i| i[0] }
    scores  = batch.map { |i| i[1] }
    members.each do |member|
      something_with(member)
    end
    ick.ickcommit("mykey",*members)

Usage example for statistician:

    ick   = Ick.new(redis)
    stats = ick.ickstats("mykey")
    puts stats['ver']        # string, version of Ick data structure in Redis
    puts stats['cset_size']  # integer, number of elements in consumer set
    puts stats['pset_size']  # integer, number of elements in producer set
    puts stats['total_size'] # integer, number of elements in all sets
    puts stats               # other stuff also maybe or in future

## Development

After checking out the repo, run `bin/setup` to install
dependencies. Then, run `rake test` to run the tests. You can also run
`bin/console` for an interactive prompt that will allow you to
experiment.

To install this gem onto your local machine, run `bundle exec rake
install`. To release a new version, update the version number in
`version.rb`, and then run `bundle exec rake release`, which will
create a git tag for the version, push git commits and tags, and push
the `.gem` file to [rubygems.org](https://rubygems.org).

## Contributing

Bug reports and pull requests are welcome on GitHub at
https://github.com/ProsperWorks/redis-ick.
