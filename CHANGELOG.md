## 0.1.1 (2018-06-18)

- Fix bug in `backwash`: cset scores not converted to number
  before comparison with pset scores.
- redis-ick.gemspec reworked only to include runtime dependencies.
- Gemfile, Appraisals, and .travis.yml set to test with redis-rb
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

