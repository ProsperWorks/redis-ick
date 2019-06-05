appraise 'redis-3.0' do
  gem 'redis', '>= 3.0', '< 3.1'
end
appraise 'redis-3.1' do
  gem 'redis', '>= 3.1', '< 3.2'
end
appraise 'redis-3.2' do
  gem 'redis', '>= 3.2', '< 3.3'
end
appraise 'redis-3.3' do
  gem 'redis', '>= 3.3', '< 3.4'
end
#
# redis >= 4.0 depends on ruby >= 2.2.2
#
appraise 'redis-4.0' do
  gem 'redis', '>= 4.0', '< 4.1'
end
#
# redis 4.1.1 still supports ruby 2.2, but redis 4.1.2 drops it
#
# I am not ready to pull the plug on ruby 2.2.2, so redis 4.1.x is
# as high as we will go for now.
#
appraise 'redis-4.1' do
  gem 'redis', '>= 4.1', '< 4.1.2'
end
