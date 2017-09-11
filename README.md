# Redis::Ick An Indexing Queue ![TravisCI](https://travis-ci.org/ProsperWorks/redis-ick.svg?branch=master)

Redis::Ick implements a priority queue in Redis with two-phase commit
and write-folding.

Icks are similar to (and built from) Redis sorted sets, and are
well-suited for dirty lists in data sync systems.

Icks have been used in production at ProsperWorks extensively to
manage our PG-to-ES, PG-to-Neo4j, data migration and repair, and a
variety of other systems since 2015-10-21.

## Background: The Original Pattern

Before Ick, our indexer queue was a simple Redis sorted set which used
current time for scores.  It looked like:

    # in any process: whenever a document is dirtied
    Redis.current.zadd(primary_key,Time.now.to_f,summary_key)

    # in the indexer process: critical section starts here
    batch = Redis.current.zrangebyrank(primary_key,0,batch_size)
    begin
      process_batch_slowly(batch)
    rescue ex
      # failover from primary to the bilge queue
      Redis.current.zadd(bilge_key,*batch)
    ensure
      Redis.current.zrem(primary_key,*members_of(batch))
      # critical section ends here
    end

**Big Advantage**: Failover.  When we blow up in
process_batch_slowly(), such as via SIGKILL, all the elements in the
batch are still in the primary_key in Redis.  When we relaunch, they
will be waiting for us in our next batch.

**Big Advantage**: Write Folding.  When a document is already dirty,
and we dirty it again, we don’t end up with 2 entries in the queue.
We only change the timestamp.  Thus, the queues grow only in the
number of dirty documents per unit time, not in the number of dirty
operations per unit time.  As we fall behind more, we fall behind
slower.

**Big Problem**: the Forgotten Dirtiness Problem.  If some document is
dirtied after process_batch_slowly() starts, we will remove that
document when process_batch_slowly() ends.  Thus, the document will be
dirty but no longer in the queue!

**Small Problem**: the Hot Data Starvation Problem.  We pop from the
cold end of the queue: but a hot document will always be percolating
toward the hot end of the queue.  If the queue is big enough and/or a
document is hot enough, it will never be popped out into a batch.

## Background: The Current Pattern

In Aug 2014, Gerald made a huge improvement in the Forgotten Dirtiness
Problem:

    # in any process: whenever a document is dirtied
    Redis.current.zadd(primary_key,Time.now.to_f,summary_key)

    # in the indexer process:
    batch = Redis.current.zrangebyrank(primary_key,0,batch_size)
    begin
      process_batch_slowly(batch)
    rescue ex
      # failover from primary to the bilge queue
      Redis.current.zadd(bilge_key,*batch)
    ensure
      # critical section starts here
      batch2 = Redis.current.zrangebyrank(primary_key,0,batch_size)
      unchanged_keys = batch1 - keys_changed_between(batch1,batch2)
      Redis.current.zrem(primary_key,*members_of(unchanged_keys))
      # critical section ends here
    end

With Gerald’s change, we take a second snapshot of the cold end of the
queue after process_batch_slowly().  Only documents which did not
change between the two snapshots are deleted.

The Forgotten Dirtiness Problem is still there, but it has now shrunk
100x.  In practice process_batch_slowly() can take minutes, but the
current critical section never takes more than 3 seconds - and then
only in extreme situations.

## What is Ick?

An Ick is a collection of three Redis keys which all live on the same
Redis hash slot:

* version flag, a string
* producer set, a sorted set into which we flag keys as dirty with timestamps
* consumer set, a sorted set from which the indexer pulls batches to index

### Ick defines 5 operations on this data via Lua on Redis:

* **ickdel**: removes all keys associated with a given Ick structure
* **ickstats**: returns a hash of stats including version and size
* **ickadd**: add a batch of members with scores to the producer set
** implements write-folding: a message can only appear once in the producer set
** when a member is re-added, it takes the lowest of 2 scores
* **ickreserve**: moves members from to the producer set to the consumer set
** moves members from the producer set into the consumer set until the consumer set is size N or the producer set is empty
** implements write-folding: a message can only appear once in the consumer set
as **ickadd**, when a member-is re-added it takes the lowest of 2 scores
** returns the results as an array
* **ickcommit**: deletes members from the consumer set

Reminder: all Redis commands are atomic and transactional, including
Lua scripts which we write.  This property is critical, but we
leverage it only implicitly.


## Proposal: The Ick Pattern

    # in any process: whenever a document is dirtied
    Ick.ickadd(redis,primary_key,Time.now.to_f,summary_key)

    # in the indexer process:
    batch = Ick.ickreserve(redis,primary_key,batch_size)
    begin
      process_batch_slowly(batch)
      # burn down the primary it the batch succeeded
      Ick.ickcommit(redis,primary_key,*members_of(batch))
    rescue ex
      # failover from primary to the bilge queue
      Ick.ickadd(redis,bilge_key,batch)
      # burn down the primary if we successfully wrote to the bilge
      Ick.ickcommit(redis,primary_key,*members_of(batch))
    end
    # Note: if the batch failed and the add-to-bilge failed, we do not
    # burn down the primary.  Thus, we either succeed or writing into
    # the bilge, we never fail without writing into the bilge (thus
    # forgetting things).

TODO: primary-vs-bilge not relevant to the public!!

    # in the bilge aka retry indexer:
    batch = Ick.ickreserve(redis,bilge_key,batch_size)
    process_batch_slowly(batch)
    # only burn down the bilge when successful
    Ick.ickcommit(redis,bilge_key,*members_of(batch))

Ick solves for failover via 2-Phase Commit protocol through
**ickreserve** and **ickcommit**.  If there is a failure during
process_batch_slowly(batch), the next time time we call **ickreserve**
we will just get the same batch - it will have resided unchanged in
the consumer set until we get happy and call **ickcommit**.

Ick solves the Forgotten Dirtiness Problem by virtue of
**ickreserve**’s implicit atomicity and the fact that **ickcommit** is
only ever called from the indexer and producers do not mutate the
consumer set.

Ick solves the Hot Data Starvation Problem by tracking not the
most-recent dirty time of members, but rather their first-known dirty
time.  The longer entries stay in the consumer set, the more they
implicitly percolate toward the poppy end regardless of how many
updates they receive.  Ditto in the consumer set.  Provided nobody is
calling **ickadd** with scores in the past, entries with the oldest
will eventually end up in a reserved batch.


## Fabulous Diagram:

Here’s a coarse dataflow for members moving through an Ick.

    app
    |
    +-- **ickadd** --> producer set
                   |
                   +-- **ickreserve** --> consumer set
                                      |
                                      +-- **ickcommit** --> forgotten

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
