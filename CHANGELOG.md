# 0.1.4 (2019-11-XX) TODO TBD
- Introduced ICKUNLINK.
  - As ICKDEL but uses Redis UNLINK for O(1) time.
- Deprecate the presence of the ick_key sentinel in Redis.
  - Continue to create it for backward-compatibility.
  - Cease to require it for future-compatibility.
  - Anticipate ceasing to create it in 0.2.0.
  - Anticipate ignoring it entirely in 0.2.1.
  - This way, an emptied Ick leaves no footprint in Redis.

## 0.1.3 (2019-06-07)
- Support for redis >= 4.0.0 added.
- Breaking changes at redis v4.0.0 addressed.
- Support for ruby < 2.2.2 dropped.
- Support for ruby 2.6 added.
- Update ruby microversions tested in .travis.yml.
- Gemfile, Appraisals, and .travis.yml set to test with redis
  gems from 3.0 through 4.1.

## 0.1.2 (2019-02-27)
- Expanded .travis.yml to cover more ruby versions.
- Change Lua scripts so all Redis keys are passed as explicit args.
  - The pset and cset keys are now computed from the main key in Ruby.
  - This supports certain forms of Redis cluster syncing.

## 0.1.1 (2018-06-18)
- Fix bug in `backwash`: cset scores not converted to number
  before comparison with pset scores.
- redis-ick.gemspec reworked only to include runtime dependencies.
- Gemfile, Appraisals, and .travis.yml set to test with redis
  gem 3.0 to 3.3.

## 0.1.0 (2018-03-20)
- Added ickexchange which combines ickcommit+ickreserve.
- Introduced backwash to ickreserve and ickexchange.
- Expanded .travis.yml to cover more rvm versions.
- Shrink Rubocop coverage to exclude `Style/*`.
- Moves version history out into CHANGELOG.md.

## 0.0.5 (2017-09-20)
- Rework ickstats so it no longer angers Twemproxy, per https://github.com/ProsperWorks/redis-ick/issues/3, by producing a nested Array response.

## 0.0.4 (2017-09-12)
- Imported text from original design doc to README.md, polish.
- Rubocop polish and defiance.
- Development dependency on [redis-key_hash](https://github.com/ProsperWorks/redis-key_hash) to test prescriptive hash claims.
- Identified limits of prescriptive hash robustness.

## 0.0.3 (2017-08-29)
- Got .travis.yml working with a live redis-server.
- Runtime dependency on redis-script_manager for Ick._eval.
- Initial Rubocop integration.
- Misc cleanup.

## 0.0.2 (2017-08-29)
- Broke out into Prosperworks/redis-ick, make public.

## 0.0.1 (prehistory)
- Still in Prosperworks/ALI/vendor/gems/redis-ick.

