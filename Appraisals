appraise 'redis-3.0' do
  gem 'redis', '~> 3.0'
end
appraise 'redis-3.1' do
  gem 'redis', '~> 3.1'
end
appraise 'redis-3.2' do
  gem 'redis', '~> 3.2'
end
appraise 'redis-3.3' do
  gem 'redis', '~> 3.3'
end
#
# redis (>= 4.0) depends on ruby (>= 2.2.2).
#
# However, at present ALI is still on ruby (= 2.1.6), and I want
# .travis.yml for this project to cover ALI's version of Ruby and
# also the modern Rubies.
#
# Therefore, until ALI upgrades to at least ruby (>= 2.2.2), we
# cannot test redis (>= 4.0) here.
#
#appraise 'redis-4.0' do
#  gem 'redis', '~> 4.0'
#end
