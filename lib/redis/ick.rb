require 'redis/ick/version'

class Redis
  class Ick

    # TODO: test *everything* in pipelines
    # TODO: redis-script_manager for eval
    # TODO: rubocop
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
      if statsd && !statsd.respond_to?(:increment)
        raise ArgumentError, "no statsd.increment"
      end
      if statsd && !statsd.respond_to?(:timing)
        raise ArgumentError, "no statsd.timeing"
      end
      if statsd && !statsd.respond_to?(:time)
        raise ArgumentError, "no statsd.time"
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
          return block_given? ? yield : nil
        end
      else
        return block_given? ? yield : nil
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
        Ick._eval(redis,LUA_ICKDEL,ick_key)
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
      raw_ickstats_results = nil
      _statsd_time('profile.ick.time.ickstats') do
        raw_ickstats_results = Ick._eval(redis,LUA_ICKSTATS,ick_key)
      end
      if raw_ickstats_results.is_a?(Redis::Future)
        #
        # We extend the Redis::Future with a continuation so we can add
        # our own post-processing.
        #
        class << raw_ickstats_results
          alias_method :original_value, :value
          def value
            ::Redis::Ick._postprocess_ickstats_results(original_value)
          end
        end
        raw_ickstats_results
      else
        ::Redis::Ick._postprocess_ickstats_results(raw_ickstats_results)
      end
    end

    def self._postprocess_ickstats_results(raw_ickstats_results)
      return nil if !raw_ickstats_results
      #
      # LUA_ICKSTATS returned bulk data response [k,v,k,v,...]
      #
      stats = Hash[*raw_ickstats_results]
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
        Ick._eval(redis,LUA_ICKADD,ick_key,*score_member_pairs)
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
    # However, by analogy with redis-rb's Redis.current.zrange(), this
    # Ruby wrapper method pairs up the results for you, and converts the
    # string scores to floats.
    #
    #   - you get from this method    [[ member_string, score_number] , ... ]
    #
    # @param ick_key String the base key for the Ick
    #
    # @param max_size max number of messages to reserve
    #
    # @return a list of up to max_size pairs, similar to
    # Redis.current.zrange() withscores: [ member_string, score_number ]
    # representing the lowest-scored elements from the producer set.
    #
    def ickreserve(ick_key,max_size=0)
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
      results = nil
      _statsd_time('profile.ick.time.ickreserve') do
        results =
          Ick._eval(
            redis,
            LUA_ICKRESERVE,
            ick_key,
            max_size
          ).each_slice(2).map { |p|
          [ p[0], Ick._floatify(p[1]) ]
        }
      end
      _statsd_timing('profile.ick.ickreserve.num_results',results.size)
      results
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
      _statsd_time('profile.ick.time.ickcommit') do
        Ick._eval(redis,LUA_ICKCOMMIT,ick_key,*members)
      end
    end

    # Converts a string str into a Float, and recognizes 'inf', '-inf',
    # etc.
    #
    # So we can be certain of compatibility, this was stolen with tweaks
    # from https://github.com/redis/redis-rb/blob/master/lib/redis.rb.
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
    def self._eval(redis,lua,ick_key,*args)
      if !lua.is_a?(String)
        raise ArgumentError, "bogus non-String lua #{lua}"
      end
      if !ick_key.is_a?(String)
        raise ArgumentError, "bogus non-String ick_key #{ick_key}"
      end
      redis.eval(lua,[ick_key],args)
    end

    #######################################################################
    #
    # The Ick Data Model in Redis
    #
    # - At ick_key, we keep a simple manifest string.  Currently, only
    #   'ick.v1' is expected or supported.  This is for future-proofing
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
    # For safety, we check that the ick_ver, ick_pset, and ick_cset
    # either do not exist or exit with the correct types and values to
    # be identifiable as an Ick.
    #
    # All scripts in the LUA_ICK series expect only one KEYS, the root
    # key of the Ick data structure.  We expect a version flag as a
    # string at this key.  Keys for other data are computed from KEYS[1]
    # in such a way as to guarantee they all hash to the same slot.
    #
    LUA_ICK_PREFIX = %{
      local ick_key        = KEYS[1]
      local ick_ver        = redis.call('GET',ick_key)
      local ick_pset_key   = ick_key .. '/ick/{' .. ick_key .. '}/pset'
      local ick_cset_key   = ick_key .. '/ick/{' .. ick_key .. '}/cset'
      local ick_ver_type   = redis.call('TYPE',ick_key).ok
      local ick_pset_type  = redis.call('TYPE',ick_pset_key).ok
      local ick_cset_type  = redis.call('TYPE',ick_cset_key).ok
      if (false ~= ick_ver and 'ick.v1' ~= ick_ver) then
        return redis.error_reply('unrecognized ick version ' .. ick_ver)
      end
      if ('none' ~= ick_ver_type and 'string' ~= ick_ver_type) then
        return redis.error_reply('ick defense: expected string at ' ..
                                 ick_ver_key .. ', found ' .. ick_ver_type)
      end
      if ('none' ~= ick_pset_type and 'zset' ~= ick_pset_type) then
        return redis.error_reply('ick defense: expected string at ' ..
                                 ick_pset_key .. ', found ' .. ick_pset_type)
      end
      if ('none' ~= ick_cset_type and 'zset' ~= ick_cset_type) then
        return redis.error_reply('ick defense: expected string at ' ..
                                 ick_cset_key .. ', found ' .. ick_cset_type)
      end
      if ('none' == ick_ver_type) then
        if ('none' ~= ick_pset_type) then
          return redis.error_reply('ick defense: no ver at ' .. ick_ver_key ..
                                   ', but found pset at ' .. ick_pset_key)
        end
        if ('none' ~= ick_cset_type) then
          return redis.error_reply('ick defense: no ver at ' .. ick_ver_key ..
                                   ', but found cset at ' .. ick_cset_key)
        end
      end
    }.freeze

    #######################################################################
    # LUA_ICKDEL
    #######################################################################
    #
    # Removes all keys associated with the Ick at KEYS[1].
    #
    # @param uses no ARGV
    #
    # @return the number of Redis keys deleted, which will be 0 if and
    # only if no Ick existed at KEYS[1]
    #
    LUA_ICKDEL = (LUA_ICK_PREFIX + %{
      return redis.call('DEL',ick_key,ick_pset_key,ick_cset_key)
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
    # LUA_ICKRESERVE
    #######################################################################
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
    # @param ARGV a single number, batch_size, the desired
    # size for cset and to be returned
    #
    # @return a bulk response, up to ARGV[1] pairs [member,score,...]
    #
    LUA_ICKRESERVE = (LUA_ICK_PREFIX + %{
      local target_cset_size = tonumber(ARGV[1])
      while true do
        local ick_cset_size  = redis.call('ZCARD',ick_cset_key)
        if ick_cset_size and target_cset_size <= ick_cset_size then
          break
        end
        local first_in_pset  = redis.call('ZRANGE',ick_pset_key,0,0,'WITHSCORES')
        if 0 == table.getn(first_in_pset) then
          break
        end
        local first_member   = first_in_pset[1]
        local first_score    = tonumber(first_in_pset[2])
        redis.call('ZREM',ick_pset_key,first_member)
        local old_score      = redis.call('ZSCORE',ick_cset_key,first_member)
        if false == old_score or first_score < tonumber(old_score) then
          redis.call('ZADD',ick_cset_key,first_score,first_member)
        end
      end
      redis.call('SETNX', ick_key, 'ick.v1')
      if target_cset_size <= 0 then
        return {}
      else
        local max            = target_cset_size - 1
        return redis.call('ZRANGE',ick_cset_key,0,max,'WITHSCORES')
      end
    }).freeze

    #######################################################################
    # LUA_ICKCOMMIT
    #######################################################################
    #
    # Removes specified members from the pset.
    #
    # @param ARGV a list of members to be removed from the cset
    #
    # @return the number of members removed
    #
    # Note: This this Lua unpacks ARGV with the iterator ipairs()
    # instead of unpack() to avoid a "too many results to unpack"
    # failure at 8000 args.  However, the loop over many redis.call is
    # regrettably heavy-weight.  From a performance standpoint it
    # would be preferable to call ZREM in larger batches.
    #
    LUA_ICKCOMMIT = (LUA_ICK_PREFIX + %{
      redis.call('SETNX', ick_key, 'ick.v1')
      if 0 == table.getn(ARGV) then
        return 0
      end
      local num_removed = 0
      for i,v in ipairs(ARGV) do
        num_removed = num_removed + redis.call('ZREM',ick_cset_key,v)
      end
      return num_removed
    }).freeze

  end
end
