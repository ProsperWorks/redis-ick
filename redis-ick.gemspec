# coding: utf-8
lib = File.expand_path("../lib", __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require "redis/ick/version"

Gem::Specification.new do |spec|
  spec.name          = "redis-ick"
  spec.version       = Redis::Ick::VERSION
  spec.authors       = ["jhw-at-prosperworks-com"]
  spec.email         = ["jhw@prosperworks.com"]

  spec.summary       = 'Redis queues with two-phase commit and write-folding.'
  spec.homepage      = "TODO: Put your gem's website or public repo URL here."

  # Prevent pushing this gem to RubyGems.org. To allow pushes either
  # set the 'allowed_push_host' to allow pushing to a single host or
  # delete this section to allow pushing to any host.
  #
  if spec.respond_to?(:metadata)
    spec.metadata["allowed_push_host"] = "TODO: Set to 'http://mygemserver.com'"
  else
    raise "RubyGems 2.0 or newer is required to protect against " \
      "public gem pushes."
  end

  spec.files         = `git ls-files -z`.split("\x0").reject do |f|
    f.match(%r{^(test|spec|features)/})
  end
  spec.bindir        = "exe"
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  spec.add_development_dependency 'bundler',  '~> 1.14'
  spec.add_development_dependency 'rake',     '~> 10.0'
  spec.add_development_dependency 'minitest', '~> 5.0'
  spec.add_development_dependency 'redis',    '~> 3.2'

  # This gem is pure Ruby, no weird dependencies.
  spec.platform = Gem::Platform::RUBY
end
