# Rustik db XD

Rustik DB is a Ruby implementation of simple files database, with Btree indexes.
Btree is described in Introduction to Algorithms by Cormen, Leiserson, Rivest and Stein, Chapter 18.
It's bundled with the `ORM mapper` which behaves like a Rails `find_by` and `save` methods on your classes.
Please note, this gem is not purposed for production use, the only reason I've written it is to demonstrate how to write your own database, and use it with your own data mapper, like `ActiveRecord`.


###Features

- Create Btree of arbitrary degree (4 by default)
- Insert and search by object attributes, like Rails framework does

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'rdb'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install rdb

## Usage

```

require_relative 'rdb'
require_relative 'rdb/orm'

class Plane
  include Rdb::Orm
  rdb_mount 'planes', {id: Integer, code: String, weight: Integer}, {id: Rdb::Indexes::Btree}, '/home/rustam/rdata'
end

names = %w(rose velvet pink blue charley delta bravo)

300000.times do |x|
  code = "%s%d%s" % [names.sample, rand(1000), names.sample]
  Plane.create(code: code, weight: rand(1000))
end

Plane.find_by(id: 1123)

x = Plane.new
x.code = "superjet3000"
x.weight = 2572
x.save

Plane.find_by(id: x.id)

```

## To Do

The features in development are:

- Hash index
- Multiple indexes

## License

The gem is available as open source under the terms of the [MIT License](https://opensource.org/licenses/MIT).
