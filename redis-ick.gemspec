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

  # redis-script_manager 0.0.6 added support for redis >= 4.0.0 which
  # we also intend to support here in redis-ick.
  #
  spec.required_ruby_version = ['>= 2.2.2', '< 2.7.0']       # tested to 2.6.3
  spec.add_runtime_dependency 'redis', '>= 3.0.0', '< 5.0.0' # tested to 4.1.1
  spec.add_runtime_dependency 'redis-script_manager', '~> 0.0.6'

  # Development dependencies are captured in Gemfile, per the pattern:
  #
  #   https://github.com/jollygoodcode/jollygoodcode.github.io/issues/21
  #
end
