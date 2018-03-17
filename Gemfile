source 'https://rubygems.org'

git_source(:github) {|repo_name| "https://github.com/#{repo_name}" }

# Specify your gem's dependencies in redis-ick.gemspec
gemspec

group :development do
  gem 'appraisal'
  gem 'bundler',              '~> 1.14'
  gem 'rake',                 '~> 10.0'
end

group :test do
  gem 'minitest',             '~> 5.0'
  gem 'redis-key_hash',       '~> 0.0.4'
  gem 'redis-namespace',      '~> 1.5'
  gem 'rubocop',              '~> 0.50.0'
end
