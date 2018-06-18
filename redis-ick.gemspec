lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'redis/ick/version'

Gem::Specification.new do |spec|

  spec.name          = 'redis-ick'
  spec.version       = Redis::Ick::VERSION
  spec.platform      = Gem::Platform::RUBY

  spec.authors       = ['jhwillett']
  spec.email         = ['jhw@prosperworks.com']

  spec.summary       = 'Redis queues with two-phase commit and write-folding.'
  spec.homepage      = 'https://github.com/ProsperWorks/redis-ick'

  spec.files         = `git ls-files -z`.split("\x0").reject do |f|
    f.match(%r{^(test|spec|features)/})
  end
  spec.bindir        = 'exe'
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ['lib']

  spec.add_development_dependency 'bundler',              '~> 1.16.1'
  spec.add_development_dependency 'minitest',             '~> 5.11.3'
  spec.add_development_dependency 'rake',                 '~> 12.3.1'
  spec.add_development_dependency 'redis-key_hash',       '~> 0.0.4'
  spec.add_development_dependency 'redis-namespace',      '~> 1.5'
  spec.add_development_dependency 'rubocop',              '~> 0.54.0'

  spec.add_runtime_dependency     'redis',                '=> 3.2'
  spec.add_runtime_dependency     'redis-script_manager', '=> 0.0.2'
end
