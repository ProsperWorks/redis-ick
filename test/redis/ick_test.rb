require 'test_helper'
require 'redis'
require 'redis/key_hash'
require 'redis/namespace'

class Redis
  class IckTest < Minitest::Test

    # A mock of the limited aspects of the statsd-ruby gem interface
    # which are important to redis-ick.
    #
    class MockStatsd
      def increment(metric)
        @log ||= []
        @log << [:increment, metric]
      end
      def timing(metric,timing)
        @log ||= []
        @log << [:timing, metric, timing]
      end
      def time(metric)
        @log ||= []
        @log << [:time, metric]
        yield
      end
      def flush
        log = @log
        @log = nil
        log
      end
    end

    # When ENV['REDIS_URL'] is set, we run a greatly expanded test suite
    # which actually talk to Redis.
    #
    # When ENV['REDIS_URL'] is unset, only a smaller set run.
    #
    def self.redis
      ENV['REDIS_URL'] ? (@redis ||= Redis.new(:url => ENV['REDIS_URL'])) : nil
    end
    def redis
      self.class.redis
    end
    def self.ick
      @ick ||= redis ? ::Redis::Ick.new(redis,statsd: MockStatsd.new) : nil
    end
    def ick
      self.class.ick
    end

    # Make sure each test starts with a fresh mocks and such.
    #
    def setup
      chars    = ('a'..'z').to_a + ('A'..'Z').to_a
      @ick_key = "IckTest-#{(0...32).map{ chars.sample }.join}"
    end

    def test_that_it_has_a_version_number
      refute_nil ::Redis::Ick::VERSION
    end

    def test_initialize
      return if !redis
      assert_raises(ArgumentError) do
        ::Redis::Ick.new(nil)
      end
      ick = ::Redis::Ick.new(redis)
      assert_equal redis,   ick.redis
      assert_nil            ick.statsd
      [
        nil,
        MockStatsd.new,
      ].each do |happy_statsd|
        ick = ::Redis::Ick.new(redis, statsd: happy_statsd)
        assert_equal redis,        ick.redis
        assert_equal happy_statsd, ick.statsd if happy_statsd
        assert_nil                 ick.statsd if !happy_statsd
      end
      [
        'nope',
        0,
        [],
      ].each do |unhappy_statsd|
        assert_raises(ArgumentError) do
          ::Redis::Ick.new(redis,statsd: unhappy_statsd)
        end
      end
    end

    def test__floatify
      [
        '', ' ', 'abc', '123abc', 123, [], {}, nil,
      ].each do |junk_case|
        assert_raises(ArgumentError) do
          ::Redis::Ick._floatify(junk_case)
        end
      end
      [
        0, 0.1, -123, -1.23, 1023.4, 1024 ** 5,
      ].each do |simple_case|
        assert_equal simple_case, ::Redis::Ick._floatify(simple_case.to_s)
      end
      {
        '-inf'  => -Float::INFINITY,
        'inf'   => Float::INFINITY,
      }.each do |explicit_case,value|
        assert_equal value,       ::Redis::Ick._floatify(explicit_case)
      end
    end

    def test_statsd_wrappers
      #
      # With ':statsd => nil' everything works without crashing.
      #
      # In particular, the ::Redis::Ick._statsd_increment returns the value of
      # the block.
      #
      return if !ick || !redis
      ick._statsd_increment('foo')
      ick._statsd_timing('foo',123)
      assert_equal 'hello',  ick._statsd_time('foo') { 'hello' }
      #
      # With a non-degenerate :statsd, everything works without crashing
      # as above plus we get metrics.
      #
      sleep_s = 0.123
      ick     = ::Redis::Ick.new(redis,statsd: MockStatsd.new)
      ick._statsd_increment('foo')
      ick._statsd_timing('foo',123)
      expected = [
        [ :increment, 'foo'      ],
        [ :timing,    'foo', 123 ],
        [ :time,      'foo'      ],
      ]
      assert_equal 'hi',     ick._statsd_time('foo') { sleep sleep_s; 'hi' }
      assert_equal expected, ick.statsd.flush
    end

    def test_calls_with_bogus_redis_arg_or_key_fail_with_argument_error
      #
      # Test that a bunch of invalid calls to Ick fail with appropriate
      # ArgumentErrors.
      #
      return if !ick || !redis
      [
        :ickdel,
        :ickstats,
        :ickadd,
        :ickcommit,
        :ickreserve,
        :ickexchange,
      ].each do |method|
        assert_raises(ArgumentError,"#{method} with bogus ick_key") do
          ick.send(method,nil)
        end
      end
      [
        :ickreserve,    # takes 2 args, the last an optional nonnegative int
      ].each do |method|
        assert_raises(ArgumentError,"#{method} with bogus ick_key") do
          ick.send(method,0)
        end
        assert_raises(ArgumentError,"#{method} with bogus max_size") do
          ick.send(method,@ick_key,-1)
        end
        assert_raises(ArgumentError,"#{method} with bogus max_size") do
          ick.send(method,@ick_key,nil)
        end
        assert_raises(ArgumentError,"#{method} with bogus max_size") do
          ick.send(method,@ick_key,'')
        end
        assert_raises(ArgumentError,"#{method} with bogus max_size") do
          ick.send(method,@ick_key,[])
        end
      end
      [
        :ickexchange,   # takes 2+ args, the second a mandatory nonnegative int
      ].each do |method|
        assert_raises(ArgumentError,"#{method} with bogus ick_key") do
          ick.send(method,0)
        end
        assert_raises(ArgumentError,"#{method} with bogus reserve_size") do
          ick.send(method,@ick_key,-1)
        end
        assert_raises(ArgumentError,"#{method} with bogus reserve_size") do
          ick.send(method,@ick_key,nil)
        end
        assert_raises(ArgumentError,"#{method} with bogus reserve_size") do
          ick.send(method,@ick_key,'')
        end
        assert_raises(ArgumentError,"#{method} with bogus reserve_size") do
          ick.send(method,@ick_key,[])
        end
      end
    end

    def test_legit_empty_calls_on_empty_ick_have_expected_return_results
      return if !ick || !redis
      assert_equal 0,     ick.ickdel(@ick_key)
      assert_nil          ick.ickstats(@ick_key)
      assert_equal [0,0], ick.ickadd(@ick_key)
      assert_equal [],    ick.ickreserve(@ick_key)
      assert_equal 0,     ick.ickcommit(@ick_key)
      assert_equal [],    ick.ickexchange(@ick_key,0)
    end

    def test_ickadd_with_some_ickstats_and_ickdel
      #
      # ickstats of nonexistant Ick returns nil, otherwise a struct with
      # certain contents
      #
      return if !ick || !redis

      assert_nil          ick.ickstats(@ick_key)                 # none

      assert_equal [0,0], ick.ickadd(@ick_key)                   # created
      assert_equal     0, ick.ickstats(@ick_key)['pset_size']
      assert_equal     0, ick.ickstats(@ick_key)['total_size']

      assert_equal [1,0], ick.ickadd(@ick_key,5,'foo')           # 1 new
      assert_equal     1, ick.ickstats(@ick_key)['pset_size']
      assert_equal     1, ick.ickstats(@ick_key)['total_size']

      assert_equal [1,0], ick.ickadd(@ick_key,10,'foo',13,'bar') # 1 new
      assert_equal     2, ick.ickstats(@ick_key)['pset_size']
      assert_equal     2, ick.ickstats(@ick_key)['total_size']

      assert_equal [1,1], ick.ickadd(@ick_key,4.5,'foo',1,'baz') # new+change
      assert_equal     3, ick.ickstats(@ick_key)['pset_size']
      assert_equal     3, ick.ickstats(@ick_key)['total_size']

      assert_equal [3,0], ick.ickadd(@ick_key,7,'a',8,'b',9,'c') # 3 new
      assert_equal     6, ick.ickstats(@ick_key)['pset_size']
      assert_equal     6, ick.ickstats(@ick_key)['total_size']

      assert_equal [1,1], ick.ickadd(@ick_key,1,'a',8,'b',3,'x') # new+change
      assert_equal     7, ick.ickstats(@ick_key)['pset_size']
      assert_equal     7, ick.ickstats(@ick_key)['total_size']

      assert_equal     2, ick.ickdel(@ick_key)                   # ver & cset
      assert_equal [3,0], ick.ickadd(@ick_key,1,'a',2,'b',3,'x') # all new
      assert_equal     3, ick.ickstats(@ick_key)['pset_size']
      assert_equal     3, ick.ickstats(@ick_key)['total_size']
    end

    def test_ickreserve_and_ickcommit_with_some_ickadd_and_ickstats
      return if !ick || !redis
      assert_equal [3,0],       ick.ickadd(@ick_key,7,'a',8,'b',9,'c') # 3 new
      assert_equal [['a',7.0]], ick.ickreserve(@ick_key,1)             # get 1
      assert_equal [['a',7.0],['b',8.0]], ick.ickreserve(@ick_key,2)   # get 2
      assert_equal [['a',7.0],['b',8.0]], ick.ickreserve(@ick_key,2)   # same 2
      assert_equal 1,           ick.ickcommit(@ick_key,'b')            # burn 1
      assert_equal [['a',7.0]], ick.ickreserve(@ick_key,1)             # same 1
      assert_equal [['a',7.0],['c',9.0]], ick.ickreserve(@ick_key,2)   # diff 2
      assert_equal 2,           ick.ickcommit(@ick_key,'c','a','b')    # burn
      assert_equal [],          ick.ickreserve(@ick_key,2)             # get 0
      assert_equal [2,0],       ick.ickadd(@ick_key,10,'A',2,'B')      # 2 new
      assert_equal [['B',2.0]], ick.ickreserve(@ick_key,1)             # get 1
      assert_equal 1,           ick.ickstats(@ick_key)['cset_size']    # :)
      assert_equal 1,           ick.ickstats(@ick_key)['pset_size']    # :)
      assert_equal 2,           ick.ickstats(@ick_key)['total_size']   # :)
    end

    def test_ickexchange_with_some_ickadd_and_ickstats
      return if !ick || !redis
      assert_equal [3,0],       ick.ickadd(@ick_key,7,'a',8,'b',9,'c')  # 3 new
      assert_equal [['a',7.0]], ick.ickexchange(@ick_key,1)             # get 1
      assert_equal [['a',7.0],['b',8.0]], ick.ickexchange(@ick_key,2)   # get 2
      assert_equal [['a',7.0],['b',8.0]], ick.ickexchange(@ick_key,2)   # same 2
      assert_equal [['a',7.0]], ick.ickexchange(@ick_key,1,'b')         # burn 1
      assert_equal [['a',7.0]], ick.ickexchange(@ick_key,1)             # same 1
      assert_equal [['a',7.0],['c',9.0]], ick.ickexchange(@ick_key,2)   # diff 2
      assert_equal [],          ick.ickexchange(@ick_key,0,'c','a','b') # burn
      assert_equal [],          ick.ickexchange(@ick_key,2)             # get 0
      assert_equal [2,0],       ick.ickadd(@ick_key,10,'A',2,'B')       # 2 new
      assert_equal [['B',2.0]], ick.ickexchange(@ick_key,1)             # get 1
      assert_equal 1,           ick.ickstats(@ick_key)['cset_size']     # :)
      assert_equal 1,           ick.ickstats(@ick_key)['pset_size']     # :)
      assert_equal 2,           ick.ickstats(@ick_key)['total_size']    # :)
    end

    def test_ickexchange_does_commit_then_reserve
      #
      # It is important that ickexchange remove elements from the cset
      # but _not_ from the pset.
      #
      # Our concurrency model is that messages for which processing is
      # possible are in the cset.  ickadd only dedupes with the cset,
      # not with the pset, because otherwise there could be a race
      # condition where do not know whether a previously-reserved
      # message was processed before or after it was added but dropped
      # as a dupe.
      #
      # Thus, when we commit we are committing messages which we
      # previously reserved (which by assumption are still in the
      # cset) but we do not want to commit messages which were in the
      # pset (which by assumption could have been added while we were
      # processing).
      #
      ick.ickadd(@ick_key,7,'a',8,'b',9,'c')
      assert_equal ['a','b'], ick.ickexchange(@ick_key,2).map(&:first)
      assert_equal ['a','b'], ick.ickexchange(@ick_key,2).map(&:first)
      ick.ickadd(@ick_key,70,'a',80,'b',90,'c')
      assert_equal ['a','b'], ick.ickexchange(@ick_key,2,'c').map(&:first)
      #
      # If the reserve happens erroneously before the commit, the next
      # line will return [] instead of ['c','a'] because the reserve
      # will do nothing because the cset is already size 2 when we
      # make this call.
      #
      assert_equal ['c','a'], ick.ickexchange(@ick_key,2,'a','b').map(&:first)
      assert_equal ['c','b'], ick.ickexchange(@ick_key,2,'a','b').map(&:first)
      assert_equal ['c'],     ick.ickexchange(@ick_key,2,'a','b').map(&:first)
      assert_equal ['c'],     ick.ickexchange(@ick_key,2,'a','b').map(&:first)
      assert_equal [],        ick.ickexchange(@ick_key,2,'c').map(&:first)
    end

    def test_ickreserve_0_does_not_pick_up_a_past_ickreserve_n
      #
      # On 2016-07-20 I found a long-standing bug in Ick which had never
      # come up: if the cset is not empty, ickreserve(0) returns the
      # entire cset!
      #
      return if !ick || !redis
      assert_equal [],          ick.ickreserve(@ick_key,0) # empty pset
      ick.ickadd(@ick_key,1,'a',2,'b',3,'c')
      assert_equal [['a',1.0]], ick.ickreserve(@ick_key,1) # valid non-0
      assert_equal [],          ick.ickreserve(@ick_key,0) # yikes!
    end

    def test_ickdel_with_some_ickadd_and_ickreserve_and_ickstats
      #
      # ickdel of nonexistant Ick returns 0, otherwise an int > 1
      #
      return if !ick || !redis
      assert_equal 0, ick.ickdel(@ick_key) # does not exist
      assert_equal 0, ick.ickdel(@ick_key) # still does not exist
      ick.ickadd(@ick_key,0,'foo')         # creates ver and cset
      assert_equal 2, ick.ickdel(@ick_key) # deletes ver and cset
      assert_equal 0, ick.ickdel(@ick_key) # does not exist
      ick.ickreserve(@ick_key)             # creates ver
      assert_equal 1, ick.ickdel(@ick_key) # deletes ver and cset
      assert_equal 0, ick.ickdel(@ick_key) # does not exist
      ick.ickadd(@ick_key,0,'foo',2,'x')   # creates ver and cset
      ick.ickreserve(@ick_key,1)           # creates pset
      assert_equal 3, ick.ickdel(@ick_key) # deletes ver, cset, and pset
      assert_equal 0, ick.ickdel(@ick_key) # does not exist
    end

    def test_ickstats_with_some_ickadd_and_ickdel
      #
      # ickstats of nonexistant Ick returns nil, otherwise a struct with
      # certain contents
      #
      return if !ick || !redis
      #
      # nonexistant ==> ickstats returns nil
      #
      assert_nil              ick.ickstats(@ick_key)
      #
      # existant ==> ickstats returns object with some data
      #
      ick.ickadd(@ick_key,0,'foo')
      got = ick.ickstats(@ick_key)
      assert_equal 'ick.v1',  got['ver']
      assert_equal  @ick_key, got['key']
      assert_equal        1,  got['pset_size']
      assert_equal        0,  got['cset_size']
      assert_equal        1,  got['total_size']
      #
      ick.ickadd(@ick_key,12,'foo',123,'bar')
      got = ick.ickstats(@ick_key)
      assert_equal 'ick.v1',  got['ver']
      assert_equal  @ick_key, got['key']
      assert_equal        2,  got['pset_size']
      assert_equal        0,  got['cset_size']
      assert_equal        2,  got['total_size']
      #
      ick.ickadd(@ick_key,16,'bang')
      got = ick.ickstats(@ick_key)
      assert_equal 'ick.v1',  got['ver']
      assert_equal  @ick_key, got['key']
      assert_equal        3,  got['pset_size']
      assert_equal        0,  got['cset_size']
      assert_equal        3,  got['total_size']
      #
      ick.ickadd(@ick_key,16,'bang')
      got = ick.ickstats(@ick_key)
      assert_equal 'ick.v1',  got['ver']
      assert_equal  @ick_key, got['key']
      assert_equal        3,  got['pset_size']
      assert_equal        0,  got['cset_size']
      assert_equal        3,  got['total_size']
      #
      # deleted ==> nonexistant ==> ickstats returns nil
      #
      ick.ickdel(@ick_key)
      assert_nil              ick.ickstats(@ick_key)
    end

    def test_ickstats_ickadd_ickdel_from_within_pipelines
      #
      # ickstats of nonexistant Ick returns nil, otherwise a struct with
      # certain contents
      #
      return if !ick || !redis
      future_stats = nil
      future_add   = nil
      future_del   = nil
      #
      # nonexistant ==> ickstats returns nil
      #
      redis.pipelined do
        future_stats = ick.ickstats(@ick_key)
      end
      assert_equal Redis::Future, future_stats.class
      assert_nil                  future_stats.value
      #
      # existant ==> ickstats returns object with some data
      #
      redis.pipelined do
        future_add   = ick.ickadd(@ick_key,0,'foo')
        future_stats = ick.ickstats(@ick_key)
      end
      assert_equal Redis::Future, future_add.class
      assert_equal [1, 0],        future_add.value
      assert_equal Redis::Future, future_stats.class
      assert_equal 'ick.v1',      future_stats.value['ver']
      assert_equal @ick_key,      future_stats.value['key']
      assert_equal 1,             future_stats.value['pset_size']
      assert_equal 0,             future_stats.value['cset_size']
      assert_equal 1,             future_stats.value['total_size']
      #
      redis.pipelined do
        future_add   = ick.ickadd(@ick_key,12,'foo',123,'bar')
        future_stats = ick.ickstats(@ick_key)
      end
      assert_equal Redis::Future, future_add.class
      assert_equal [1, 0],        future_add.value
      assert_equal Redis::Future, future_stats.class
      assert_equal 'ick.v1',      future_stats.value['ver']
      assert_equal @ick_key,      future_stats.value['key']
      assert_equal 2,             future_stats.value['pset_size']
      assert_equal 0,             future_stats.value['cset_size']
      assert_equal 2,             future_stats.value['total_size']
      #
      redis.pipelined do
        future_add   = ick.ickadd(@ick_key,16,'bang')
        future_stats = ick.ickstats(@ick_key)
      end
      assert_equal Redis::Future, future_add.class
      assert_equal [1, 0],        future_add.value
      assert_equal Redis::Future, future_stats.class
      assert_equal 'ick.v1',      future_stats.value['ver']
      assert_equal @ick_key,      future_stats.value['key']
      assert_equal 3,             future_stats.value['pset_size']
      assert_equal 0,             future_stats.value['cset_size']
      assert_equal 3,             future_stats.value['total_size']
      #
      redis.pipelined do
        future_add   = ick.ickadd(@ick_key,16,'bang')
        future_stats = ick.ickstats(@ick_key)
      end
      assert_equal Redis::Future, future_add.class
      assert_equal [0, 0],        future_add.value
      assert_equal Redis::Future, future_stats.class
      assert_equal 'ick.v1',      future_stats.value['ver']
      assert_equal @ick_key,      future_stats.value['key']
      assert_equal 3,             future_stats.value['pset_size']
      assert_equal 0,             future_stats.value['cset_size']
      assert_equal 3,             future_stats.value['total_size']
      #
      # deleted ==> nonexistant ==> ickstats returns nil
      #
      redis.pipelined do
        future_del   = ick.ickdel(@ick_key)
        future_stats = ick.ickstats(@ick_key)
      end
      assert_equal Redis::Future, future_del.class
      assert_equal 2,             future_del.value
      assert_equal Redis::Future, future_stats.class
      assert_nil                  future_stats.value
    end

    def test_ickadd_ickreserve_ickcommit_from_within_pipelines
      return if !ick || !redis
      scores_and_members = [12.3,'foo',10,'bar',100,'baz',1.23,'x']
      members_and_scores = [['x',1.23],['bar',10.0],['foo',12.3],['baz',100.0]]
      size               = scores_and_members.size / 2
      future_add         = nil
      future_reserve     = nil
      future_commit      = nil
      ick.redis.pipelined do
        future_add       = ick.ickadd(@ick_key,*scores_and_members)
        future_reserve   = ick.ickreserve(@ick_key,size)
      end
      assert_equal Redis::Future,      future_add.class
      assert_equal [size, 0],          future_add.value
      assert_equal Redis::Future,      future_reserve.class
      assert_equal members_and_scores, future_reserve.value
      ick.redis.pipelined do
        future_commit    =
          ick.ickcommit(@ick_key,*members_and_scores.map(&:first))
      end
      assert_equal Redis::Future,      future_commit.class
      assert_equal size,               future_commit.value
    end

    def test_ickadd_ickexchange_from_within_pipelines
      return if !ick || !redis
      scores_and_members = [12.3,'foo',10,'bar',100,'baz',1.23,'x']
      future_add         = nil
      future_exchange    = nil
      ick.redis.pipelined do
        future_add       = ick.ickadd(@ick_key,*scores_and_members)
        future_exchange  = ick.ickexchange(@ick_key,2)
      end
      assert_equal Redis::Future,      future_add.class
      assert_equal [4, 0],             future_add.value
      assert_equal Redis::Future,      future_exchange.class
      assert_equal ['x','bar'],        future_exchange.value.map(&:first)
      ick.redis.pipelined do
        future_exchange  = ick.ickexchange(@ick_key,2,'x')
      end
      assert_equal Redis::Future,      future_exchange.class
      assert_equal ['bar','foo'],      future_exchange.value.map(&:first)
    end

    def test_ickstats_with_scores_and_some_fractional_scores
      #
      # ickstats of nonexistant Ick returns nil, otherwise a struct with
      # certain contents
      #
      return if !ick || !redis
      ick.ickadd(@ick_key,5,'a')
      expect = {
        'ver'        => 'ick.v1',
        'key'        => @ick_key,
        'pset_key'   => "#{@ick_key}/ick/{#{@ick_key}}/pset",
        'cset_key'   => "#{@ick_key}/ick/{#{@ick_key}}/cset",
        'pset_size'  => 1,
        'pset_min'   => 5,        # whole computed numbers stay whole
        'pset_max'   => 5,
        'cset_size'  => 0,
        'total_size' => 1,
        'total_min'  => 5,
        'total_max'  => 5,
      }
      assert_equal expect, ick.ickstats(@ick_key)
      ick.ickadd(@ick_key,6.6,'b',4.4,'c')
      expect = {
        'ver'        => 'ick.v1',
        'key'        => @ick_key,
        'pset_key'   => "#{@ick_key}/ick/{#{@ick_key}}/pset",
        'cset_key'   => "#{@ick_key}/ick/{#{@ick_key}}/cset",
        'pset_size'  => 3,
        'pset_min'   => 4.4,      # fractional computed numbers stay fractional
        'pset_max'   => 6.6,
        'cset_size'  => 0,
        'total_size' => 3,
        'total_min'  => 4.4,
        'total_max'  => 6.6,
      }
      assert_equal expect, ick.ickstats(@ick_key)
      ick.ickreserve(@ick_key,1)
      expect = {
        'ver'        => 'ick.v1',
        'key'        => @ick_key,
        'pset_key'   => "#{@ick_key}/ick/{#{@ick_key}}/pset",
        'cset_key'   => "#{@ick_key}/ick/{#{@ick_key}}/cset",
        'pset_size'  => 2,
        'pset_min'   => 5,
        'pset_max'   => 6.6,
        'cset_size'  => 1,
        'cset_min'   => 4.4,
        'cset_max'   => 4.4,
        'total_size' => 3,
        'total_min'  => 4.4,
        'total_max'  => 6.6,
      }
      assert_equal expect, ick.ickstats(@ick_key)
      #
      # Check queue which exists, but is empty:
      #
      ick.ickdel(@ick_key)
      assert_nil           ick.ickstats(@ick_key)
      ick.ickadd(@ick_key,123,'a')
      ick.ickreserve(@ick_key,1).each do |member,_score|
        ick.ickcommit(@ick_key,member)
      end
      expect = {
        'ver'        => 'ick.v1',
        'key'        => @ick_key,
        'pset_key'   => "#{@ick_key}/ick/{#{@ick_key}}/pset",
        'cset_key'   => "#{@ick_key}/ick/{#{@ick_key}}/cset",
        'pset_size'  => 0,
        'cset_size'  => 0,
        'total_size' => 0,
        #'total_min'  => 'total_max', # this bug before major_2015_11_soylent!
      }
      assert_equal expect, ick.ickstats(@ick_key)
    end

    def test_ickadd_plus_ickreserve_look_a_lot_like_zadd_plus_zrange_withscores
      return if !ick || !redis

      zset_key           = "#{@ick_key}/zset"

      scores_and_members = [12.3,'foo',10,'bar',100,'baz',1.23,'x']
      members_and_scores = [['x',1.23],['bar',10.0],['foo',12.3],['baz',100.0]]
      size               = scores_and_members.size / 2

      zset_add_result    = redis.zadd(zset_key,scores_and_members)
      zset_get_result    = redis.zrange(zset_key,0,-1,:withscores => true)

      ick_add_result     = ick.ickadd(@ick_key,*scores_and_members)
      ick_get_result     = ick.ickreserve(@ick_key,size)

      assert_equal size,               zset_add_result
      assert_equal [size, 0],          ick_add_result

      assert_equal members_and_scores, zset_get_result
      assert_equal members_and_scores, ick_get_result
    end

    def test_defensiveness_around_broken_ick_objects_in_redis
      #
      # Hacking here under the hood, corrupting some of the keys in
      # Redis which comprise an ick.
      #
      return if !ick || !redis
      ick_ver_key  = @ick_key
      ick_pset_key = "#{@ick_key}/ick/{#{@ick_key}}/pset"
      ick_cset_key = "#{@ick_key}/ick/{#{@ick_key}}/cset"
      #
      # No keys exist initially, and ickstats changes nothing.
      #
      assert_nil                ick.ickstats(@ick_key)
      assert_equal 'none',      redis.type(ick_ver_key)
      assert_equal 'none',      redis.type(ick_pset_key)
      assert_equal 'none',      redis.type(ick_cset_key)
      #
      # ickadd creates ver, and also creates pset if non-empty, and we
      # confirm we know all the proper keys to manipulate.
      #
      assert_equal [0,0],       ick.ickadd(@ick_key)
      assert_equal 'string',    redis.type(ick_ver_key)
      assert_equal 'none',      redis.type(ick_pset_key)
      assert_equal 'none',      redis.type(ick_cset_key)
      assert_equal [1,0],       ick.ickadd(@ick_key,1,'x')
      assert_equal 'string',    redis.type(ick_ver_key)
      assert_equal 'zset',      redis.type(ick_pset_key)
      assert_equal 'none',      redis.type(ick_cset_key)
      #
      # ickreserve creates ver, and also creates cset if non-empty, and
      # may also consume the pset.
      #
      assert_equal [],          ick.ickreserve(@ick_key,0)
      assert_equal 'string',    redis.type(ick_ver_key)
      assert_equal 'zset',      redis.type(ick_pset_key)
      assert_equal 'none',      redis.type(ick_cset_key)
      assert_equal [['x',1.0]], ick.ickreserve(@ick_key,1)
      assert_equal 'string',    redis.type(ick_ver_key)
      assert_equal 'none',      redis.type(ick_pset_key)
      assert_equal 'zset',      redis.type(ick_cset_key)
      #
      # saving a non-string to ick_ver_key leads to a state where all
      # ick commands break
      #
      redis.del(ick_ver_key)
      redis.hset(ick_ver_key,'foo','bar')
      assert_equal 'hash',      redis.type(ick_ver_key)
      assert_equal 'none',      redis.type(ick_pset_key)
      assert_equal 'zset',      redis.type(ick_cset_key)
      assert_raises(Redis::CommandError) do
        ick.ickstats(@ick_key)
      end
      assert_raises(Redis::CommandError) do
        ick.ickadd(@ick_key)
      end
      assert_raises(Redis::CommandError) do
        ick.ickreserve(@ick_key,0)
      end
      assert_raises(Redis::CommandError) do
        ick.ickcommit(@ick_key)
      end
      assert_raises(Redis::CommandError) do
        ick.ickdel(@ick_key)
      end
      #
      # Clobbering ick_ver_key but keeping non-empty pset or cset leads
      # to a state where all ick commands break.
      #
      # Deleting both the rogue pset and cset fixes things.
      #
      redis.del(ick_ver_key,ick_pset_key,ick_cset_key)
      assert_equal [2,0],       ick.ickadd(@ick_key,1,'x',2,'y')
      assert_equal [['x',1.0]], ick.ickreserve(@ick_key,1)
      assert_equal 'string',    redis.type(ick_ver_key)
      assert_equal 'zset',      redis.type(ick_pset_key)
      assert_equal 'zset',      redis.type(ick_cset_key)
      redis.del(ick_ver_key)
      assert_equal 'none',      redis.type(ick_ver_key)
      assert_equal 'zset',      redis.type(ick_pset_key)
      assert_equal 'zset',      redis.type(ick_cset_key)
      assert_raises(Redis::CommandError) do
        ick.ickstats(@ick_key)
      end
      assert_raises(Redis::CommandError) do
        ick.ickadd(@ick_key)
      end
      assert_raises(Redis::CommandError) do
        ick.ickreserve(@ick_key,0)
      end
      assert_raises(Redis::CommandError) do
        ick.ickcommit(@ick_key)
      end
      assert_raises(Redis::CommandError) do
        ick.ickdel(@ick_key)
      end
      redis.del(ick_pset_key)
      assert_equal 'none',      redis.type(ick_ver_key)
      assert_equal 'none',      redis.type(ick_pset_key)
      assert_equal 'zset',      redis.type(ick_cset_key)
      assert_raises(Redis::CommandError) do
        ick.ickstats(@ick_key)
      end
      assert_raises(Redis::CommandError) do
        ick.ickadd(@ick_key)
      end
      assert_raises(Redis::CommandError) do
        ick.ickreserve(@ick_key,0)
      end
      assert_raises(Redis::CommandError) do
        ick.ickcommit(@ick_key)
      end
      assert_raises(Redis::CommandError) do
        ick.ickdel(@ick_key)
      end
      redis.del(ick_cset_key)
      assert_equal 'none',      redis.type(ick_ver_key)
      assert_equal 'none',      redis.type(ick_pset_key)
      assert_equal 'none',      redis.type(ick_cset_key)
      ick.ickstats(@ick_key)
      ick.ickadd(@ick_key)
      ick.ickreserve(@ick_key,0)
      ick.ickcommit(@ick_key)
      ick.ickdel(@ick_key)
      #
      # A junk string value at ick_ver_key leads to a state where all
      # ick commands break.
      #
      redis.del(ick_ver_key,ick_pset_key,ick_cset_key)
      assert_equal [2,0],       ick.ickadd(@ick_key,1,'x',2,'y')
      assert_equal [['x',1.0]], ick.ickreserve(@ick_key,1)
      assert_equal 'string',    redis.type(ick_ver_key)
      assert_equal 'zset',      redis.type(ick_pset_key)
      assert_equal 'zset',      redis.type(ick_cset_key)
      redis.del(ick_ver_key)
      redis.set(ick_ver_key,'bogus-ick-version')
      assert_equal 'string',    redis.type(ick_ver_key)
      assert_equal 'zset',      redis.type(ick_pset_key)
      assert_equal 'zset',      redis.type(ick_cset_key)
      assert_raises(Redis::CommandError) do
        ick.ickstats(@ick_key)
      end
      assert_raises(Redis::CommandError) do
        ick.ickadd(@ick_key)
      end
      assert_raises(Redis::CommandError) do
        ick.ickreserve(@ick_key,0)
      end
      assert_raises(Redis::CommandError) do
        ick.ickcommit(@ick_key)
      end
      assert_raises(Redis::CommandError) do
        ick.ickdel(@ick_key)
      end
      #
      # saving a non-sorted-set to ick_pset_key messes things up
      #
      redis.del(ick_ver_key,ick_pset_key,ick_cset_key)
      assert_equal [2,0],       ick.ickadd(@ick_key,1,'x',2,'y')
      assert_equal [['x',1.0]], ick.ickreserve(@ick_key,1)
      assert_equal 'string',    redis.type(ick_ver_key)
      assert_equal 'zset',      redis.type(ick_pset_key)
      assert_equal 'zset',      redis.type(ick_cset_key)
      redis.del(ick_pset_key)
      redis.set(ick_pset_key,'warbles')
      assert_equal 'string',    redis.type(ick_ver_key)
      assert_equal 'string',    redis.type(ick_pset_key)
      assert_equal 'zset',      redis.type(ick_cset_key)
      assert_raises(Redis::CommandError) do
        ick.ickstats(@ick_key)
      end
      assert_raises(Redis::CommandError) do
        ick.ickadd(@ick_key)
      end
      assert_raises(Redis::CommandError) do
        ick.ickreserve(@ick_key,0)
      end
      assert_raises(Redis::CommandError) do
        ick.ickcommit(@ick_key)
      end
      assert_raises(Redis::CommandError) do
        ick.ickdel(@ick_key)
      end
      #
      # saving a non-sorted-set to ick_cset_key messes things up
      #
      redis.del(ick_ver_key,ick_pset_key,ick_cset_key)
      assert_equal [2,0],       ick.ickadd(@ick_key,1,'x',2,'y')
      assert_equal [['x',1.0]], ick.ickreserve(@ick_key,1)
      assert_equal 'string',    redis.type(ick_ver_key)
      assert_equal 'zset',      redis.type(ick_pset_key)
      assert_equal 'zset',      redis.type(ick_cset_key)
      redis.del(ick_cset_key)
      redis.set(ick_cset_key,'warbles')
      assert_equal 'string',    redis.type(ick_ver_key)
      assert_equal 'zset',      redis.type(ick_pset_key)
      assert_equal 'string',    redis.type(ick_cset_key)
      assert_raises(Redis::CommandError) do
        ick.ickstats(@ick_key)
      end
      assert_raises(Redis::CommandError) do
        ick.ickadd(@ick_key)
      end
      assert_raises(Redis::CommandError) do
        ick.ickreserve(@ick_key,0)
      end
      assert_raises(Redis::CommandError) do
        ick.ickcommit(@ick_key)
      end
      assert_raises(Redis::CommandError) do
        ick.ickdel(@ick_key)
      end
    end

    def test_very_large_argument_lists_to_redis_lua_unpack
      #
      # Unfortunately, when we first ramped Ick in prod we ran into a
      # failure when ickcommit() was called with very large batch sizes:
      # Redis returns to us the Lua error "too many results to unpack"
      # from Lua.
      #
      # This matches the pathology (but not the path-to-reproduce) of:
      #
      #   https://github.com/antirez/redis/issues/678
      #
      # It seems the Lua in Redis is built with a parameter
      # DEFAULT_STACK_SIZE of 1024 which affects how much the unpack()
      # method can handle.
      #
      # Way to overuse recursion, Lua team!
      #
      # In this test, we repro that basic error and probe its limits.
      #
      return if !ick || !redis
      #
      # There is no apparent limit passing args to and from Redis-Lua.
      #
      # In contrast, there is a very clear breaking stress for unpack().
      #
      lua_echo   = 'return ARGV'   # proves problem not arg or result passing
      lua_unpack = 'unpack(ARGV)'  # demonstrates problem in Redis/LUA unpack()
      [
        7998,
        7999,                      # unpack() happy with array size up to 7999
      ].each do |num|
        args = num.times.to_a
        redis.eval(lua_echo,[@ick_key],args)
        redis.eval(lua_unpack,[@ick_key],args)
      end
      [
        8000,                      # unpack() unhappy with array size 8000+
        8001,
      ].each do |num|
        args = num.times.to_a
        redis.eval(lua_echo,[@ick_key],args)
        assert_raises(Redis::CommandError,"num #{num}") do
          redis.eval(lua_unpack,[@ick_key],args)
        end
      end
    end

    def test_very_large_argument_lists_to_ick
      #
      # From the preceeding test, we are armed with the knowledge that
      # Redis-Lua breaks not in general argument processing, but in
      # calls to unpack().
      #
      # Here, we evaluate whether the three variadic Ick commands with
      # can handle extremely large calls i.e. whether they avoid the
      # unpack() bug.
      #
      return if !ick || !redis
      happy_sizes          = [10,20,30]   # make sure this test is sane
      unhappy_sizes        = [8000, 8010] # 8k unhappy per previous test
      (happy_sizes + unhappy_sizes).each do |size|
        members            = Array.new(size) { |i| sprintf('key-%08d',1.0 * i) }
        score_member_pairs = members.each_with_index.map { |x,i| [i,x] }
        ick.ickadd(@ick_key,*score_member_pairs.flatten)
        reserved           = ick.ickreserve(@ick_key,size)
        assert_equal score_member_pairs, reserved.map { |x,i| [i,x] }
        num_committed      = ick.ickcommit(@ick_key,*members)
        assert_equal size,               num_committed
        rereserved         = ick.ickreserve(@ick_key,size)
        assert_equal [],                 rereserved
        num_recommitted    = ick.ickcommit(@ick_key,*members)
        assert_equal 0,                  num_recommitted
      end
      #
      # ickadd() and ickreserve() do not fail because they never called
      # unpack().
      #
      # ickcommit() does not fail, because its Lua has been updated to
      # work around the unpack() woe.
      #
    end

    # This test suite skips many test if a redis-server is not
    # available.  When this happens, we consider testing incomplete.
    #
    # This test directly checks the availability of a redis-server at
    # ENV['REDIS_URL'].
    #
    def test_redis_is_available
      refute_nil   ENV['REDIS_URL'],   'need REDIS_URL for complete test'
      refute_nil   redis,              'need a redis for complete test'
      assert_equal 'PONG', redis.ping, 'no redis-server at REDIS_URL'
    end

    # For a variety of ick_keys, both simple and malicious, we test
    # our claim that the master key, producer set key, and consumer
    # set key will all hash to the same slot in both Redis Cluster and
    # in stock RedisLabs Enterprise Cluster.
    #
    [
      #
      # LUA_ICK_PREFIX has complex expansions for ick_pset_key and
      # ick_cset_key which are meant to guarantee that these keys will
      # hash to the same slot as ick_key in both Redis Cluster and
      # RedisLabs Enterprise Cluster.
      #
      # Unfortunately, that logic is only sound if ick_key and
      # namespace are both free of RC or RLEC prescriptive hash
      # markers.
      #
      [ 'x',     nil,     true ],
      [ 'x',     '',      true ],
      [ 'x',     'x',     true ],
      [ 'x',     'foo',   true ],
      [ 'foo',   nil,     true ],
      [ 'foo',   '',      true ],
      [ 'foo',   'x',     true ],
      [ 'foo',   'foo',   true ],
      #
      # If the ick_keys include prescriptive tags, the scheme breaks
      # down.
      #
      [ '{}abc', nil,     false ],
      [ '{}abc', '',      false ],
      [ '{}abc', 'x',     false ],
      [ '{a}bc', nil,     false ],
      [ '{a}bc', '',      false ],
      [ '{a}bc', 'x',     false ],
      [ 'a{b}c', nil,     false ],
      [ 'a{b}c', '',      false ],
      [ 'a{b}c', 'x',     false ],
      [ 'ab{c}', nil,     false ],
      [ 'ab{c}', '',      false ],
      [ 'ab{c}', 'x',     false ],
      [ 'abc{}', nil,     false ],
      [ 'abc{}', '',      false ],
      [ 'abc{}', 'x',     false ],
      #
      # If the namespace includes prescriptive tags, the scheme breaks
      # down.
      #
      [ 'x',     '{}',    false ],
      [ 'x',     'f{o}o', false ],
      [ 'foo',   '{}',    false ],
      [ 'foo',   'f{o}o', false ],
      #
      # If both ick_keys and namespace include prescriptive tags, the
      # scheme breaks down.
      #
      [ '{}abc', '{}',    false ],
      [ '{}abc', 'f{o}o', false ],
      [ '{a}bc', '{}',    false ],
      [ '{a}bc', 'f{o}o', false ],
      [ 'a{b}c', '{}',    false ],
      [ 'a{b}c', 'f{o}o', false ],
      [ 'ab{c}', '{}',    false ],
      [ 'ab{c}', 'f{o}o', false ],
      [ 'abc{}', '{}',    false ],
      [ 'abc{}', 'f{o}o', false ],
    ].each do |ick_key,namespace,expect|
      define_method("test_p_hash_#{ick_key}_#{namespace.inspect}_#{expect}") do
        return if !ick || !redis
        #
        # Note we pre-map the key with logic borrowed from
        # Redis::Namespace.add_namespace.
        #
        # That logic is private in redis-namespace 1.5.2 and 1.5.3 so
        # we route around the censorship.
        #
        # Note that we used the ick_key_namespaced when we call Ick
        # operations but we do not pass the :namespace option to
        # Redis::KeyHash.all_in_one_slot! because the namespace will
        # already have been incorporated into the actual keys by
        # ICKSTATS.
        #
        redis_namespaced   = Redis::Namespace.new(namespace, redis: redis)
        ick_key_namespaced = redis_namespaced.send(:add_namespace,ick_key)
        ick.ickdel(ick_key_namespaced)       # clean up any old cruft
        ick.ickadd(ick_key_namespaced,0,'')  # make sure the Ick exists
        stats = ick.ickstats(ick_key_namespaced)
        assert_equal ick_key_namespaced, stats['key']      # agreement
        refute_equal ick_key_namespaced, stats['pset_key'] # diversity
        refute_equal ick_key_namespaced, stats['cset_key'] # diversity
        refute_equal stats['pset_key'],  stats['cset_key'] # diversity
        if expect
          Redis::KeyHash.all_in_one_slot!(
            stats['key'],
            stats['pset_key'],
            stats['cset_key'],
          )
        else
          got = Redis::KeyHash.all_in_one_slot?(
            stats['key'],
            stats['pset_key'],
            stats['cset_key'],
          )
          assert_equal false, got
        end
      end
    end
  end
end
