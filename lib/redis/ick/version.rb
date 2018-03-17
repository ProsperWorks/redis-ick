class Redis
  class Ick
    #
    # Version plan/history:
    #
    # 0.0.1 - Still in Prosperworks/ALI/vendor/gems/redis-ick.
    #
    # 0.0.2 - Broke out into Prosperworks/redis-ick, make public.
    #
    # 0.0.3 - Got .travis.yml working with a live redis-server.
    #
    #         Runtime dependency on redis-script_manager for
    #         Ick._eval.
    #
    #         Initial Rubocop integration.
    #
    #         Misc cleanup.
    #
    # 0.0.4 - Imported text from original design doc to README.md, polish.
    #
    #         Rubocop polish and defiance.
    #
    #         Development dependency on redis-key_hash to test
    #         prescriptive hash claims.  Identified limits of
    #         prescriptive hash robustness.
    #
    # 0.0.5 - Rework ickstats so it no longer angers Twemproxy, per
    #         https://github.com/ProsperWorks/redis-ick/issues/3,
    #         by producing a nested Array response.
    #
    # 0.1.0 - LUA_ICKCOMMIT and LUA_ICKCOMMIT combined to LUA_ICKEXCHANGE.
    #         Expanded .travis.yml to cover more rvm versions.
    #
    VERSION = '0.1.0'.freeze
  end
end
