
lib = File.expand_path("../lib", __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require "rdb/version"

Gem::Specification.new do |spec|
  spec.name          = "rdb"
  spec.version       = Rdb::VERSION
  spec.authors       = ["rustik"]
  spec.email         = ["vasil.brazil@gmail.com"]

  spec.summary       = %q{Rustik db}
  spec.description   = %q{Ruby implementation of database with btree and ORM mapper}
  spec.license       = "MIT"

  spec.files         = `git ls-files -z`.split("\x0").reject do |f|
    f.match(%r{^(test|spec|features)/})
  end
  spec.bindir        = "exe"
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler", "~> 1.16"
  spec.add_development_dependency "rake", "~> 10.0"
  required_ruby_version = '~> 2.5'
end
