require 'redis/ick/version'
require 'redis/script_manager'

class Redis

  # Accessor for Ick data structures in Redis.
  #
  class Ick

    # TODO: rdoc

    # Creates an Ick accessor.
    #
    # @param redis Redis
    #
    # @param statsd a stats proxy.  May be nil, else expected to respond
    # to :increment and :timing.
    #
    def initialize(redis, statsd: nil)
      if !redis.is_a?(Redis)
        raise ArgumentError, "not a Redis: #{redis}"
      end
      if statsd
        if !statsd.respond_to?(:increment)
          raise ArgumentError, 'no statsd.increment'
        end
        if !statsd.respond_to?(:timing)
          raise ArgumentError, 'no statsd.timeing'
        end
        if !statsd.respond_to?(:time)
          raise ArgumentError, 'no statsd.time'
        end
      end
      @redis  = redis
      @statsd = statsd
    end

    attr_accessor :redis
    attr_accessor :statsd

    # Reports a single count on the requested metric to statsd (if any).
    #
    # @param metric String
    #
    def _statsd_increment(metric)
      statsd.increment(metric) if statsd
    end

    # Reports the specified timing on the requested metric to statsd (if
    # any).
    #
    # @param metric String
    #
    def _statsd_timing(metric,time)
      statsd.timing(metric,time) if statsd
    end

    # Executes the block (if any) and reports its timing in milliseconds
    # on the requested metric to statsd (if any).
    #
    # @param metric String
    #
    # @return the value of the block, or nil if none
    #
    def _statsd_time(metric)
      if statsd
        statsd.time(metric) do
          block_given? ? yield : nil
        end
      else
        block_given? ? yield : nil
      end
    end

    # Removes all data associated with the Ick in Redis at key.
    #
    # Similar to DEL key, http://redis.io/commands/del, but may
    # delete multiple keys which together implement the Ick data
    # structure.
    #
    # @param ick_key String the base key for the Ick
    #
    # @return an integer, the number of Redis keys deleted, which will
    # be >= 1 if an Ick existed at key.
    #
    def ickdel(ick_key)
      if !ick_key.is_a?(String)
        raise ArgumentError, "bogus non-String ick_key #{ick_key}"
      end
      _statsd_increment('profile.ick.ickdel.calls')
      _statsd_time('profile.ick.ickdel.time') do
        _eval(LUA_ICKDEL,ick_key)
      end
    end

    # Removes all data associated with the Ick in Redis at key.
    #
    # Similar to UNLINK key, http://redis.io/commands/unlink, but may
    # unlink multiple keys which together implement the Ick data
    # structure.
    #
    # @param ick_key String the base key for the Ick
    #
    # @return an integer, the number of Redis keys unlinked, which will
    # be >= 1 if an Ick existed at key.
    #
    def ickunlink(ick_key)
      if !ick_key.is_a?(String)
        raise ArgumentError, "bogus non-String ick_key #{ick_key}"
      end
      _statsd_increment('profile.ick.ickunlink.calls')
      _statsd_time('profile.ick.ickunlink.time') do
        _eval(LUA_ICKUNLINK,ick_key)
      end
    end

    # Fetches stats.
    #
    # @param ick_key String the base key for the Ick
    #
    # @return If called outside a Redis pipeline, a Hash with stats
    # about the Ick at ick_key, if any, else nil.  If called within a
    # pipeline, returns a redis::Future whose value is a Hash or nil as
    # before.
    #
    def ickstats(ick_key)
      if !ick_key.is_a?(String)
        raise ArgumentError, "bogus non-String ick_key #{ick_key}"
      end
      _statsd_increment('profile.ick.ickstats.calls')
      raw_results = nil
      _statsd_time('profile.ick.time.ickstats') do
        raw_results = _eval(LUA_ICKSTATS,ick_key)
      end
      _postprocess(
        raw_results,
        lambda do |results|
          return nil if !results
          #
          # LUA_ICKSTATS returned bulk data response [k,v,k,v,...]
          #
          stats = Hash[*results]
          #
          # From http://redis.io/commands/eval, the "Lua to Redis conversion
          # table" states that:
          #
          #   Lua number -> Redis integer reply (the number is converted
          #   into an integer)
          #
          #   ...If you want to return a float from Lua you should return
          #   it as a string.
          #
          # LUA_ICKSTATS works around this by converting certain stats to
          # strings.  We reverse that conversion here.
          #
          stats.keys.select{|k|/_min$/ =~ k || /_max$/ =~ k}.each do |k|
            next if !stats[k]
            stats[k] = (/^\d+$/ =~ stats[k]) ? stats[k].to_i : stats[k].to_f
          end
          stats
        end
      )
    end

    # Adds all the specified members with the specified scores to the
    # Ick stored at key.
    #
    # Entries are stored in order by score.  Lower-scored entries will
    # pop out in reserve before higher-scored entries.  Re-adding an
    # entry which already exists in the producer set with a new score
    # results in the entry having the lowest of the old and new scores.
    #
    # Similar to http://redis.io/commands/zadd with a modified NX
    # option, operating on the producer set.
    #
    # Usage:
    #
    #   ick.ickadd(ick_key,score,member[,score,member]*)
    #
    # Suggested usage is for scores to be a Unix timestamp indicating
    # when something became dirty.
    #
    # @param ick_key String the base key for the Ick
    #
    # @param score_member_pairs Array of Arrays of [score,message]
    #
    # @return a pair, the number of new values followed by the numer of
    # changed scores.
    #
    def ickadd(ick_key,*score_member_pairs)
      if !ick_key.is_a?(String)
        raise ArgumentError, "bogus non-String ick_key #{ick_key}"
      end
      if score_member_pairs.size.odd?
        raise ArgumentError, "bogus odd-numbered #{score_member_pairs}"
      end
      score_member_pairs.each_slice(2) do |slice|
        score, member = slice
        if ! score.is_a? Numeric
          raise ArgumentError, "bogus non-Numeric score #{score}"
        end
        if ! member.is_a? String
          raise ArgumentError, "bogus non-String member #{member}"
        end
      end
      _statsd_increment('profile.ick.ickadd.calls')
      _statsd_timing('profile.ick.ickadd.pairs',score_member_pairs.size / 2)
      _statsd_time('profile.ick.time.ickadd') do
        _eval(LUA_ICKADD,ick_key,*score_member_pairs)
      end
    end

    # Tops up the consumer set up to max_size by shifting the
    # lowest-scored elements from the producer set into the consumer set
    # until the consumer set cardinality reaches max_size or the
    # producer set is exhausted.
    #
    # The reserved elements are meant to represent consumer
    # work-in-progress.  If they are not committed, they will be
    # returned again in future calls to ickreserve.
    #
    # Note that the Lua for ick is irritating like so:
    #
    #   - you add in the pattern      [ score_number,  member_string, ... ]
    #   - you retrieve in the pattern [ member_string, score_string, ... ]
    #
    # Native ZADD and ZRANGE WITHSCORES exhibit this same irritating
    # inconsistency: Ick is annoyance-compatible with Redis sorted sets.
    #
    # However, by analogy with the redis gem's Redis.current.zrange(),
    # this Ruby wrapper method pairs up the results for you, and
    # converts the string scores to floats.
    #
    #   - you get from this method    [[ member_string, score_number] , ... ]
    #
    # @param ick_key String the base key for the Ick
    #
    # @param max_size max number of messages to reserve
    #
    # @param backwash if true, in the reserve function cset members
    # with high scores are swapped out for pset members with lower
    # scores.  Otherwise cset members remain in the cset until
    # committed regardless of how low scores in the pset might be.
    #
    # @return a list of up to max_size pairs, similar to
    # Redis.current.zrange() withscores: [ member_string, score_number ]
    # representing the lowest-scored elements from the producer set.
    #
    def ickreserve(ick_key,max_size=0,backwash: false)
      if !ick_key.is_a?(String)
        raise ArgumentError, "bogus non-String ick_key #{ick_key}"
      end
      if !max_size.is_a?(Integer)
        raise ArgumentError, "bogus non-Integer max_size #{max_size}"
      end
      if max_size < 0
        raise ArgumentError, "bogus negative #{max_size}"
      end
      _statsd_increment('profile.ick.ickreserve.calls')
      _statsd_timing('profile.ick.ickreserve.max_size',max_size)
      raw_results   = nil
      _statsd_time('profile.ick.time.ickreserve') do
        raw_results = _eval(
          LUA_ICKEXCHANGE,
          ick_key,
          max_size,
          backwash ? 'backwash' : false,
        )
      end
      _postprocess(raw_results,Skip0ThenFloatifyPairs)
    end

    # Removes the indicated members from the producer set, if present.
    #
    # Similar to ZREM ick_key [member]*, per
    # http://redis.io/commands/zrem, operating on the consumer set only.
    #
    # Usage:
    #
    #   ick.ickcommit(ick_key,memberA,memberB,...)
    #
    # Committed elements are meant to represent consumer work-completed.
    #
    # @param ick_key String the base key for the Ick
    #
    # @param members members to be committed out pf the pset
    #
    # @return an integer, the number of members removed from the
    # producer set, not including non existing members.
    #
    def ickcommit(ick_key,*members)
      if !ick_key.is_a?(String)
        raise ArgumentError, "bogus non-String ick_key #{ick_key}"
      end
      _statsd_increment('profile.ick.ickcommit.calls')
      _statsd_timing('profile.ick.ickcommit.members',members.size)
      raw_results = nil
      _statsd_time('profile.ick.time.ickcommit') do
        raw_results = _eval(
          LUA_ICKEXCHANGE,
          ick_key,
          0,
          false,              # backwash not relevant in ickcommit
          *members
        )
      end
      # 
      # raw_results are num_committed followed by 0 message-and-score
      # pairs.
      #
      # We just capture the num_committed.
      #
      _postprocess(raw_results,lambda { |results| results[0] })
    end

    # ickexchange combines several functions in one Redis round-trip.
    #
    # 1. As ickcommit, removes consumed members from the consumer set.
    #
    # 2. As ickreserve, tops up the consumer set from the producer and
    #    returns the requested new consumer members, if any.
    #
    # @param ick_key String the base key for the Ick
    #
    # @param reserve_size Integer max number of messages to reserve.
    #
    # @param commit_members Array members to be committed.
    #
    # @param backwash if true, in the reserve function cset members
    # with high scores are swapped out for pset members with lower
    # scores.  Otherwise cset members remain in the cset until
    # committed regardless of how low scores in the pset might be.
    #
    # @return a list of up to reserve_size pairs, similar to
    # Redis.current.zrange() withscores: [ message, score ]
    # representing the lowest-scored elements from the producer set
    # after the commit and reserve operations.
    #
    def ickexchange(ick_key,reserve_size,*commit_members,backwash: false)
      if !ick_key.is_a?(String)
        raise ArgumentError, "bogus non-String ick_key #{ick_key}"
      end
      if !reserve_size.is_a?(Integer)
        raise ArgumentError, "bogus non-Integer reserve_size #{reserve_size}"
      end
      if reserve_size < 0
        raise ArgumentError, "bogus negative reserve_size #{reserve_size}"
      end
      _statsd_increment('profile.ick.ickexchange.calls')
      _statsd_timing('profile.ick.ickexchange.reserve_size',reserve_size)
      _statsd_timing(
        'profile.ick.ickexchange.commit_members',
        commit_members.size
      )
      raw_results = nil
      _statsd_time('profile.ick.time.ickexchange') do
        raw_results = _eval(
          LUA_ICKEXCHANGE,
          ick_key,
          reserve_size,
          backwash ? 'backwash' : false,
          commit_members
        )
      end
      _postprocess(raw_results,Skip0ThenFloatifyPairs)
    end

    # Postprocessing done on the LUA_ICKEXCHANGE results for both
    # ickreserve and ickexchange.
    #
    # results are num_committed followed by N message-and-score
    # pairs.
    #
    # We do results[1..-1] to skip the first element, num_committed.
    #
    # On the rest, we floatify the scores to convert from Redis
    # number-as-string limitation to Ruby Floats.
    #
    # This is similar to to Redis::FloatifyPairs:
    #
    # https://github.com/redis/redis-rb/blob/master/lib/redis.rb#L2887-L2896
    #
    Skip0ThenFloatifyPairs = lambda do |results|
      results[1..-1].each_slice(2).map do |m_and_s|
        [ m_and_s[0], ::Redis::Ick._floatify(m_and_s[1]) ]
      end
    end

    # Calls back to block with the results.
    # 
    # If raw_results is a Redis::Future, callback will be deferred
    # until the future is expanded.
    #
    # Otherwise, callback will happen immediately.
    #
    def _postprocess(raw_results,callback)
      if raw_results.is_a?(Redis::Future)
        #
        # Redis::Future have a built-in mechanism for calling a
        # transformation on the raw results.
        #
        # Here, we monkey-patch not the Redis::Future class, but just
        # this one raw_results object.  We give ourselves a door to
        # set the post-processing transformation.
        #
        # The transformation will be called only once when the real
        # results are materialized.
        #
        class << raw_results
          def transformation=(transformation)
            raise "transformation collision" if @transformation
            @transformation = transformation
          end
        end
        raw_results.transformation = callback
        raw_results
      else
        #
        # If not Redis::Future, we invoke the callback immediately.
        #
        callback.call(raw_results)
      end
    end

    # A deferred computation which allows us to perform post-processing
    # on results which come back from redis pipelines.
    #
    # The idea is to regain some measure of composability by allowing
    # utility methods to respond polymorphically depending on whether
    # they are called in a pipeline.
    #
    # TODO: Where this utility lives in the code is not very well
    # thought-out.  This is more broadly applicable than just for
    # Icks.  This probably belongs in its own file, or in RedisUtil,
    # or as a monkey-patch into the redis gem.  This is intended for
    # use with Redis::Futures, but has zero Redis-specific code.  This
    # is more broadly applicable, maybe, than Redis. This is in class
    # Ick for the time being only because Ick.ickstats() is where I
    # first needed this and it isn't otherwise obvious where to put
    # this.
    #
    class FutureContinuation
      #
      # The first (and only the first) time :value is called on this
      # FutureContinuation, conversion will be called.
      #
      def initialize(continuation)
        @continuation = continuation
        @result       = nil
      end
      #
      # Force the computation.  :value is chosen as the name of this
      # method to be duck-typing compatible with Redis::Future.
      #
      def value
        if @continuation
          @result       = @continuation.call
          @continuation = nil
        end
        @result
      end
    end

    # Converts a string str into a Float, and recognizes 'inf', '-inf',
    # etc.
    #
    # So we can be certain of compatibility, this was stolen with tweaks
    # from:
    #
    #   https://github.com/redis/redis-rb/blob/master/lib/redis.rb#L2876-L2885
    #
    def self._floatify(str)
      raise ArgumentError, "not String: #{str}" if !str.is_a?(String)
      if (inf = str.match(/^(-)?inf/i))
        (inf[1] ? -1.0 : 1.0) / 0.0
      else
        Float(str)
      end
    end

    # Runs the specified lua in the redis against the specifified Ick.
    #
    def _eval(lua,ick_key,*args)
      if !lua.is_a?(String)
        raise ArgumentError, "bogus non-String lua #{lua}"
      end
      if !ick_key.is_a?(String)
        raise ArgumentError, "bogus non-String ick_key #{ick_key}"
      end
      ick_pset_key = "#{ick_key}/ick/{#{ick_key}}/pset"
      ick_cset_key = "#{ick_key}/ick/{#{ick_key}}/cset"
      Redis::ScriptManager.eval_gently(
        redis,
        lua,
        [ick_key,ick_pset_key,ick_cset_key],
        args
      )
    end

    #######################################################################
    #
    # The Ick Data Model in Redis
    #
    # - At ick_key, we keep a simple manifest string.  Currently, only
    #   'ick.v1' is expected or supported.  This is deprecated in
    #   0.1.3: supported and tolerated but not required.  In 0.2.0
    #   these will be deleted, and in 0.2.1 they will be cut from the
    #   code entirely.
    #
    # - At "#{ick_key}/ick/{#{ick_key}}/pset" we keep a sorted set.
    #   Ick, the "producer set", into which new messages are pushed by
    #   ickadd.
    #
    # - At "#{ick_key}/ick/{#{ick_key}}/cset" we keep another sorted
    #   set, the "consumer set", where messages are held between
    #   ickreserve and ickcommit.
    #
    # These name patterns were chosen carefully, so that under the Redis
    # Cluster Specification, http://redis.io/topics/cluster-spec, all
    # three keys will always be hashed to the same HASH_SLOT.
    #
    # Note that if ick_key contain a user-specified prescriptive hashing
    # subsequence like "{foo}", then that {}-sequence appears at the
    # front of the key, hence will serve as the prescriptive hash key
    # for all the derived keys which use ick_keys a prefix.
    #
    # If ick_key does not contain a {}-sequence, then the portion of all
    # derived keys ".../{#{ick_key}}/..." provides one which hashes to
    # the same slot as ick_key.
    #
    # Thus, we know all keys will be present on these same shard at
    # run-time.
    #
    # The ickadd op adds entries to the pset, but only if they do not
    # already exist.
    #
    # The ickreserve op moves entries from the pset into the cset.
    #
    # The ickcommit op removes entries from the pset.
    #
    # WARNING: If ick_key itself contains an {}-expr, this hashslot
    # matching algorithm will break in RedisLabs Enterprise Cluster due
    # to the newly-discovered Yikes Curly Brace Surprise.  See
    # https://github.com/ProsperWorks/ALI/pull/1132 for details.
    #
    #######################################################################


    #######################################################################
    # LUA_ICK_PREFIX
    #######################################################################
    #
    # A snippet of Lua code which is common to all the Ick scripts.
    #
    # For convenience and to avoid repeating code, we set up
    # some computed key names.
    #
    # For safety, we check that ick_pset and ick_cset either are
    # sorted sets or do not exist, and that ick_ver either is 'ick.v1'
    # or does not exist.
    #
    # All scripts in the LUA_ICK series expect only one KEYS, the root
    # key of the Ick data structure.  We expect a version flag as a
    # string at this key.  Keys for other data are computed from KEYS[1]
    # in such a way as to guarantee they all hash to the same slot.
    #
    LUA_ICK_PREFIX = %{
      local ick_key        = KEYS[1]
      local ick_ver        = redis.call('GET',ick_key)
      local ick_pset_key   = KEYS[2]
      local ick_cset_key   = KEYS[3]
      local ick_ver_type   = redis.call('TYPE',ick_key).ok
      local ick_pset_type  = redis.call('TYPE',ick_pset_key).ok
      local ick_cset_type  = redis.call('TYPE',ick_cset_key).ok
      if (false ~= ick_ver and 'ick.v1' ~= ick_ver) then
        return redis.error_reply('unrecognized ick version ' .. ick_ver)
      end
      if ('none' ~= ick_ver_type and 'string' ~= ick_ver_type) then
        return redis.error_reply('ick defense: expected string at ' ..
                                 ick_key .. ', found ' .. ick_ver_type)
      end
      if ('none' ~= ick_pset_type and 'zset' ~= ick_pset_type) then
        return redis.error_reply('ick defense: expected zset at ' ..
                                 ick_pset_key .. ', found ' .. ick_pset_type)
      end
      if ('none' ~= ick_cset_type and 'zset' ~= ick_cset_type) then
        return redis.error_reply('ick defense: expected zset at ' ..
                                 ick_cset_key .. ', found ' .. ick_cset_type)
      end
    }.freeze

    #######################################################################
    # LUA_ICKDEL and LUA_ICKUNLINK
    #######################################################################
    #
    # Removes all keys associated with the Ick at KEYS[1].
    #
    # @param uses no ARGV
    #
    # @return the number of Redis keys deleted/unlinked, which will be
    # 0 if and only if no Ick existed at KEYS[1]
    #
    LUA_ICKDEL    = (LUA_ICK_PREFIX + %{
      return redis.call('DEL',ick_key,ick_pset_key,ick_cset_key)
    }).freeze
    LUA_ICKUNLINK = (LUA_ICK_PREFIX + %{
      return redis.call('UNLINK',ick_key,ick_pset_key,ick_cset_key)
    }).freeze

    #######################################################################
    # LUA_ICKSTATS
    #######################################################################
    #
    # @param uses no ARGV
    #
    # @return a bulk data response with statistics about the Ick at
    # KEYS[1], or nil if none.
    #
    # Note: At http://redis.io/commands/eval, the "Lua to Redis
    # conversion table" stats:
    #
    #   Lua number -> Redis integer reply (the number is converted
    #   into an integer)
    #
    #   ...If you want to return a float from Lua you should return
    #   it as a string.
    #
    # We follow this recommendation in our Lua below where we convert
    # our numeric responses to strings with "tostring(tonumber(n))".
    #
    LUA_ICKSTATS = (LUA_ICK_PREFIX + %{
      if (false == ick_ver) then
        return nil
      end
      local ick_pset_size = redis.call('ZCARD',ick_pset_key)
      local ick_cset_size = redis.call('ZCARD',ick_cset_key)
      local ick_stats     = {
        'key',        ick_key,
        'pset_key',   ick_pset_key,
        'cset_key',   ick_cset_key,
        'ver',        ick_ver,
        'cset_size',  ick_cset_size,
        'pset_size',  ick_pset_size,
        'total_size', ick_cset_size + ick_pset_size,
      }
      local pset_min = nil
      local pset_max = nil
      if ick_pset_size > 0 then
        pset_min = redis.call('ZRANGE',ick_pset_key, 0, 0,'WITHSCORES')[2]
        table.insert(ick_stats, 'pset_min')
        table.insert(ick_stats, tostring(tonumber(pset_min)))
        pset_max = redis.call('ZRANGE',ick_pset_key,-1,-1,'WITHSCORES')[2]
        table.insert(ick_stats, 'pset_max')
        table.insert(ick_stats, tostring(tonumber(pset_max)))
      end
      local cset_min = nil
      local cset_max = nil
      if ick_cset_size > 0 then
        cset_min = redis.call('ZRANGE',ick_cset_key, 0, 0,'WITHSCORES')[2]
        table.insert(ick_stats, 'cset_min')
        table.insert(ick_stats, tostring(tonumber(cset_min)))
        cset_max = redis.call('ZRANGE',ick_cset_key,-1,-1,'WITHSCORES')[2]
        table.insert(ick_stats, 'cset_max')
        table.insert(ick_stats, tostring(tonumber(cset_max)))
      end
      local total_min = nil
      if pset_min and cset_min then
        total_min = math.min(cset_min,pset_min)
      elseif pset_min then
        total_min = pset_min
      elseif cset_min then
        total_min = cset_min
      end
      if total_min then
        table.insert(ick_stats, 'total_min')
        table.insert(ick_stats, tostring(tonumber(total_min)))
      end
      local total_max = nil
      if pset_max and cset_max then
        total_max = math.max(cset_max,pset_max)
      elseif pset_max then
        total_max = pset_max
      elseif cset_max then
        total_max = cset_max
      end
      if total_max then
        table.insert(ick_stats, 'total_max')
        table.insert(ick_stats, tostring(tonumber(total_max)))
      end
      return ick_stats
    }).freeze

    #######################################################################
    # LUA_ICKADD
    #######################################################################
    #
    # Adds members to the cset as per ZADD.  Where a member is
    # re-written, we always take the lowest score.
    #
    # Thus, scores are only allowed to move downward.  changes to score.
    #
    # Creates the Ick if necessary.
    #
    # @param ARGV a sequence of score,member pairs as per Redis ZADD.
    #
    # @return a pair of numbers [num_new, num_changed]
    #
    LUA_ICKADD = (LUA_ICK_PREFIX + %{
      local num_args    = table.getn(ARGV)
      if 1 == (num_args % 2) then
        return redis.error_reply("odd number of arguments for 'ickadd' command")
      end
      local num_new     = 0
      local num_changed = 0
      for i = 1,num_args,2 do
        local score     = tonumber(ARGV[i])
        local member    = ARGV[i+1]
        local old_score = redis.call('ZSCORE',ick_pset_key,member)
        if false == old_score then
          redis.call('ZADD',ick_pset_key,score,member)
          num_new       = num_new + 1
        elseif score < tonumber(old_score) then
          redis.call('ZADD',ick_pset_key,score,member)
          num_changed   = num_changed + 1
        end
      end
      redis.call('SETNX', ick_key, 'ick.v1')
      return { num_new, num_changed }
    }).freeze

    #######################################################################
    # LUA_ICKEXCHANGE: commit then reserve
    #######################################################################
    #
    # Commit Function
    #
    # Removes specified members in ARGV[2..N] from the pset, then tops
    # up the cset to up to size ARGV[1] by shifting the lowest-scored
    # members over from the pset.
    #
    # The cset might already be full, in which case we may shift fewer
    # than ARGV[1] elements.
    #
    # Reserve Function
    #
    # Tops up the cset to up to size ARGV[1] by shifting the
    # lowest-scored members over from the pset.
    #
    # The cset might already be full, in which case we may shift fewer
    # than ARGV[1] elements.
    #
    # The same score-folding happens as per ICKADD.  Thus where there
    # are duplicate messages, we may remove more members from the pset
    # than we add to the cset.
    #
    # @param ARGV[1] single number, batch_size, the desired size for
    # cset and to be returned
    #
    # @param ARGV[2] string, 'backwash' for backwash
    #
    # @param ARGV[3..N] messages to be removed from the cset before reserving
    #
    # @return a bulk response, the number of members removed from the
    # cset by the commit function followed by up to ARGV[1] pairs
    # [member,score,...] from the reserve funciton.
    #
    # Note: This Lua calls unpack(ARGV,i,j) in limited-size slices to
    # avoid a "too many results to unpack" failure at 8000 args.  Perf
    # testing on my Mac gives 0.14s to commit 50k messages with
    # unpack_limit=1 or with a naive one-at-a-time loop, 0.11s
    # with unpack_limit=7000, and 0.11s with unpack_limit=7950.
    #
    # Follow up note: for a given batch size, the commit part is only
    # half the cost of an add, 25% the cost of reserve without
    # backwash, and 12% the cost of reserve with backwash.  So effort
    # to optimize commit further, even as far as we have gone, is
    # small potatoes.
    #
    LUA_ICKEXCHANGE = (LUA_ICK_PREFIX + %{
      local reserve_size   = tonumber(ARGV[1])
      local backwash       = ARGV[2]
      local argc           = table.getn(ARGV)
      local num_committed  = 0
      local unpack_limit   = 7950
      for i = 3,argc,unpack_limit do
        local max          = math.min(i+unpack_limit,argc)
        local num_zrem     = redis.call('ZREM',ick_cset_key,unpack(ARGV,i,max))
        num_committed      = num_committed + num_zrem
      end
      if 'backwash' == backwash then
        local cset_all     = redis.call('ZRANGE',ick_cset_key,0,-1,'WITHSCORES')
        local cset_size    = table.getn(cset_all)
        for i = 1,cset_size,2 do
          local member     = cset_all[i]
          local score      = cset_all[i+1]
          local old_score  = redis.call('ZSCORE',ick_pset_key,member)
          if false == old_score then
            redis.call('ZADD',ick_pset_key,score,member)
          elseif tonumber(score) < tonumber(old_score) then
            redis.call('ZADD',ick_pset_key,score,member)
          end
        end
        redis.call('DEL',ick_cset_key)
      end
      while true do
        local cset_size    = redis.call('ZCARD',ick_cset_key)
        if cset_size and reserve_size <= cset_size then
          break
        end
        if false then
          local first_pset   =
            redis.call('ZRANGE',ick_pset_key,0,0,'WITHSCORES')
          if 0 == table.getn(first_pset) then
            break
          end
          local first_member = first_pset[1]
          local first_score  = tonumber(first_pset[2])
          redis.call('ZREM',ick_pset_key,first_member)
          local old_score    = redis.call('ZSCORE',ick_cset_key,first_member)
          if false == old_score or first_score < tonumber(old_score) then
            redis.call('ZADD',ick_cset_key,first_score,first_member)
          end
        else
          local num_to_get     = math.min(reserve_size-cset_size,unpack_limit/2)
          if num_to_get < 1 then
            return redis.error_reply("num_to_get internal error A")
          end
          local head_pset      =
            redis.call('ZRANGE',ick_pset_key,0,(num_to_get-1),'WITHSCORES')
          local head_pset_size = table.getn(head_pset)
          if 0 == head_pset_size then
            break
          end
          local to_zrem_pset   = {} -- just members
          local to_zadd_cset   = {} -- scores and members
          for i = 1,head_pset_size,2 do
            local member       = head_pset[i]
            local score_pset   = head_pset[i+1]
            local score_cset   = redis.call('ZSCORE',ick_cset_key,member)
            if false == score_cset or score_pset < tonumber(score_cset) then
              to_zadd_cset[#to_zadd_cset+1] = score_pset
              to_zadd_cset[#to_zadd_cset+1] = member
            end
            to_zrem_pset[#to_zrem_pset+1]   = member
          end
          redis.call('ZREM',ick_pset_key,unpack(to_zrem_pset))
          redis.call('ZADD',ick_cset_key,unpack(to_zadd_cset))
        end
      end
      redis.call('SETNX', ick_key, 'ick.v1')
      local result         = { num_committed }
      if reserve_size > 0 then
        local max          = reserve_size - 1
        local cset_batch   =
          redis.call('ZRANGE',ick_cset_key,0,max,'WITHSCORES')
        for _i,v in ipairs(cset_batch) do
          table.insert(result,v)
        end
      end
      return result
    }).freeze

  end
end
