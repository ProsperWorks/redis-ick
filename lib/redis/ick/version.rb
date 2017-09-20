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
    # 0.1.0 - (future) Big README.md and Rdoc update, solicit feedback
    #         from select external beta users.
    #
    # 0.2.0 - (future) Incorporate feedback, announce.
    #
    VERSION = '0.0.5'.freeze
  end
end
