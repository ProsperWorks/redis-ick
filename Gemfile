# Development dependencies are captured in Gemfile and in
# gemfiles/*.gemfile, and managed with the gem 'approaisal', per the
# pattern:
#
#   https://github.com/jollygoodcode/jollygoodcode.github.io/issues/21

source 'https://rubygems.org'

git_source(:github) {|repo_name| "https://github.com/#{repo_name}" }

gemspec

group :development do
  gem 'appraisal',            '~> 2.2.0'
  gem 'bundler'
  gem 'rake',                 '~> 12.3.1'
end

group :test do
  gem 'minitest',             '~> 5.11.3'
  gem 'redis-key_hash',       '~> 0.0.4'
  gem 'redis-namespace',      '~> 1.5'
  gem 'rubocop',              '~> 0.54.0'
end
