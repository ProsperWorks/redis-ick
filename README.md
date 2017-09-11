# Redis::Ick An Indexing Queue ![TravisCI](https://travis-ci.org/ProsperWorks/redis-ick.svg?branch=master)

Redis::Ick implements a priority queue in Redis which supports:

* multiple producers
* write-folding
* two-phase commit for a single consumer

Ick solves a variety of race condition and starvation issues which can
arise between the producers and the consumer.

Icks are similar to (and built from) Redis sorted sets.  They are
well-suited for dirty lists in data sync systems.

Ick has been live in production at ProsperWorks since 2015-10-21.  We
use them at the heart of our PG-to-ES and PG-to-Neo4j pipelines, for
data migration and repair, and a variety of other crawler systems.

## Background: The Original Pattern

Long before Ick, our indexer queue was a simple Redis sorted set which
used current time for scores.  It looked like:

    # in any process whenever a document is dirtied
    redis.zadd(queue_key,Time.now.to_f,document_id)

    # in the indexer process
    batch = redis.zrangebyrank(queue_key,0,batch_size)  # critical section start
    process_batch_slowly(batch)
    # burn down the queue only if the batch succeeded
    redis.zrem(queue_key,*members_of(batch))            # critical section end

**Big Advantage**: Failover.  Because we defer ZREM until after
success, when we fail in process_batch_slowly() (such as via an
exception or SIGKILL), all document_ids in the batch are still in
Redis.  When the indexer process resumes, those document_ids will run
again.

**Big Advantage**: Write Folding.  Because we use Redis sorted sets,
when a document is dirtied twice in quick succession, we only get 1
entry in ther queue.  We change the timestamp but we do not end up
with 2 entries in the queue.  Thus, the queue grows only in the number
of dirty _documents_ per unit time, not in the number of dirty
_operations_ per unit time.  In a sense, the more we fall behind the
slower we fall.

**Big Problem**: The Forgotten Dirtiness Problem.  If some document is
dirtied a second time after the start of process_batch_slowly(), when
process_batch_slowly() end we will drop that document from the queue.
Thus, the document will be dirty but no longer in the queue!

**Small Problem**: The Hot Data Starvation Problem.  Because we score
by time-of-dirtiness and we use ZRANGEBYRANK starting at 0, each batch
is the _coldest_ dirty documents.  Most of the time this is a good
proxy for what we really care about: the _oldest_ dirty documents.
But when a document is re-dirtied, its old timestamp is replaced with
a new timestamp.  In effect, it jumps from the cold end of the queue
to the hot end of the queue.  If the queue is big enough that it is
always larger than one batch and a document is hot enough that it gets
updated in queue more often than our batches, the document will never
be popped out into a batch.

## Background: The Intermediate Pattern

A year before Ick, August 2014, Gerald made a huge improvement which
mostly mitigated the Forgotten Dirtiness Problem:

    # in any process: whenever a document is dirtied
    redis.zadd(queue_key,Time.now.to_f,document_id)

    # in the indexer process:
    batch1 = redis.zrangebyrank(queue_key,0,batch_size)
    process_batch_slowly(batch)
    # burn down the queue only if the batch succeeded
    batch2 = redis.zrangebyrank(queue_key,0,batch_size) # critical section start
    unchanged_keys = batch1.keys - keys_whose_score_changed_in(batch1,batch2)
    redis.zrem(queue_key,*members_of(unchanged_keys))   # critical section end

Gerald changed it so a second snapshot of the cold end of the queue is
taken after process_batch_slowly().  Only documents whose timestamps
did not change between the two snapshots are removed from the queue.

Notice how the critical section no longer includes
process_batch_slowly().  Instead it only spans two Redis ops and some
local set arithmetic which.

The critical section and the Forgotten Dirtiness Problem which it
causes is still there, but is much smaller.  In practice we have
process_batch_slowly() taking minutes, but even in extreme situations
this critical section never took more than 3 seconds.

## Proposal: The Ick Pattern

In October 2015, while reviewing the Forgotten Dirtiness problem, we
identified the Hot Data Starvation Problem.  We developed Ick and
switched to this almost familiar pattern:

    # in any process: whenever a document is dirtied
    Ick.ickadd(redis,queue_key,Time.now.to_f,document_id)

    # in the indexer process:
    batch = Ick.ickreserve(redis,queue_key,batch_size)
    process_batch_slowly(batch)
    # burn down the queue only if the batch succeeded
    Ick.ickcommit(redis,queue_key,*members_of(batch))   # critical section gone


Ick solves for failover via a two phase commit protocol between
**ickreserve** and **ickcommit**.  If there is a failure during
process_batch_slowly(batch), the next time time we call **ickreserve**
we will just get the same batch - it will have resided unchanged in
the consumer set until we get happy and call **ickcommit**.

Ick solves the Forgotten Dirtiness Problem by virtue of
**ickreserve**’s implicit atomicity and the fact that **ickcommit** is
only ever called from the indexer and producers do not mutate the
consumer set.

Ick solves the Hot Data Starvation Problem by a subtle change in
ICKADD.  Unlike ZADD, which overwrites the old score when a message is
re-added, or ZADD NX which always preserves the old score, ICKADD
always takes the _min_ of the old and new scores.  Thus, Ick tracks
the first-known ditry time for a message even when there is time skew
in the producers.  The longer entries stay in the consumer set, the
more they implicitly percolate toward the cold end regardless of how
many updates they receive.  Ditto in the consumer set.  Provided that
all producers make a best effort to use only current or future
timestamps when they call ICKADD, the ICKRESERVE batch will always
include the oldest entries and there will be no starvation.

Apology: I know that [Two-Phase
Commit](https://en.wikipedia.org/wiki/Two-phase_commit_protocol) has a
different technical meaning than what Ick does.  Unfortunately I can't
find a better name for this very common failsafe queue pattern.  I
suppose we could the Redis sorted set as the coordinator and the
consumer process as the (single) participant node and, generously,
Two-Phase Commit might be taken to describe Ick.


## What is Ick?

An Ick is a collection of three Redis keys which all live on the same
[Redis hash slot](https://redis.io/topics/cluster-spec):

* version flag, a string
* producer set, a sorted set into which we flag keys as dirty with timestamps
* consumer set, a sorted set from which the indexer pulls batches to index

### Ick defines 5 operations on this data via Lua on Redis:

* **ickdel**: removes all keys associated with a given Ick structure
* **ickstats**: returns a hash of stats including version and size
* **ickadd**: add a batch of members with scores to the producer set
** implements write-folding: a message can only appear once in the producer set
** when a member is re-added, it takes the lowest of 2 scores
* **ickreserve**: moves members from to the producer set to the consumer set until the consumer set is size N or the producer set is empty
** implements write-folding: a message can only appear once in the consumer set
as **ickadd**, when a member-is re-added it takes the lowest of 2 scores
** returns the results as an array
* **ickcommit**: deletes members from the consumer set

Reminder: In general with few exceptions, all Redis commands are
atomic and transactional.  This includes any Lua scripts such as those
which implement Ick.  This atomicity guarantee is important to the
correctness of Ick, but because it is inherent in Redis/Lua, does not
appear explicitly in any of the Ick sources.

## Fabulous Diagram

Here is a coarse dataflow for members moving through an Ick.

    app
    |
    +-- **ickadd** --> producer set
                   |
                   +-- **ickreserve** --> consumer set
                                      |
                                      +-- **ickcommit** --> forgotten

## Miscellanea

### Ready for Redis Cluster

Even though one Ick uses three Redis keys, Ick is compatible with
Redis Cluster.  At ProsperWorks we use it with RedisLabs Enterprise
Cluster.

Ick does some very tricky things to compute the producer set and
consumer set keys from the master key in a way which puts them all on
the same slot in both Redis Cluster and with RLEC's default
prescriptive hashing algorithm.

See [redis-key_hash](https://github.com/ProsperWorks/redis-key_hash)
for how test this.

### Scalability

Ick supports only a single consumer: there is only one consumer set.

If your application need more than one consumer for throughput or
other reasons, you should shard across multiple Icks, each with one
consumer loop each.

This is exactly how we use Icks at ProsperWorks.  Our application code
does not simply push to an individual Ick.  We push to a bit of code
which knows that one "channel" is really N Icks.  To select an Ick,
that code does a stable hash of our document_ids, modulo N.

This way, each Ick is able to dedupe across only its dedicated subset
of all messages.

We considered a more complicated Ick which supported multiple
consumers, but a lot of new problems come up once we take that step:
can one message be in multiple consumer sets?  If not, what happens
when one consumer halts?  How do we prevent the cold end of the
producer set from getting clogged up with messages destined for the
idle consumer?

We prefer handling those issues in higher-level code.  Thus, Ick by
itself does not attempt to solve scalability.


### Some Surprises Which Can Be Gotchas in Test

Because ICKADD uses write-folding semantics over the producer set,
ICKADD might or might not grow the total size of the queue.

ICKRESERVE is not a read-only operation.  It can mutate both the
producer set and the consumer set.  Because ICKRESERVE uses
write-folding semantics between the producer set and the consumer set,
ICKRESERVE(N) might:

* shrink the producer set by N and grow the consumer set by N
* shrink the producer set by 0 and grow the consumer set by 0
* shrink the producer set by N and grow the consumer set by 0
* or anything in between

Because Ick always uses the min when multiple scores are present for
one message, ICKADD can rearrange the order of the producer set and
ICKRESERVE can rearrange the order of the consumer set in surprising
ways.

ICKADD write-folds in the producer set but not in the consumer set.
Thus, one message can appear in both the producer set and the consumer
set.  At first this seems wrong and inefficient, but in fact it is a
desirable property.  When a message is in both sets, it means it was
included in a batch by ICKRESERVE, then added by ICKADD, but has yet
to be ICKCOMMITed.  The interpretation for this is that the consumer
is actively engaged in updating the downstream systems.  But that
means, at the Ick it is indeterminate whether the message is still
dirty or has been cleaned.  That is, being in both queues corresponds
exactly to a message being in the critical section where a race
condition is possible.  Thus, we _want_ it to still be dirty and to
appear in a future batch.

None of these surprises is a bug in Ick: they are all consistent with
the design and the intent.  But they are surprises nonetheless and can
(and have) led to bugs in code which makes an unwarranted assumption.

## Installation

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
    ick.ickadd("mykey",Time.now.to_f,"bang")   # Time.now recommended for scores

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
