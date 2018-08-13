# frozen_string_literal: true

require 'spec_helper'
require 'redis'
require 'set'

def test_error_rate(bf, elems)
  visited = Set.new
  error = 0
  elems.times do |_i|
    a = rand(elems)
    error += 1 if bf.include?(a) != visited.include?(a)
    visited << a
    bf.insert a
  end
  error.to_f / elems
end

def factory(options, driver)
  options[:driver] = driver
  Redis::Bloomfilter.new options
end

describe Redis::Bloomfilter do

  let(:redis_url) { "redis://localhost:6379" }

  it 'should return the right version' do
    expect(Redis::Bloomfilter.version).to eq "redis-bloomfilter version #{Redis::Bloomfilter::VERSION}"
  end

  it 'should check for the initialize options' do
    expect { Redis::Bloomfilter.new }.to raise_error(ArgumentError)
    expect { Redis::Bloomfilter.new size: 123 }.to raise_error(ArgumentError)
    expect { Redis::Bloomfilter.new error_rate: 0.01 }.to raise_error(ArgumentError)
    expect { Redis::Bloomfilter.new size: 123, error_rate: 0.01, driver: 'bibu' }.to raise_error(NameError)
  end

  it 'should choose the right driver based on the Redis version' do
    redis_mock = flexmock('redis')
    redis_mock.should_receive(:info).and_return({ 'redis_version' => '2.6.0' })
    redis_mock_2_5 = flexmock('redis_2_5')
    redis_mock_2_5.should_receive(:info).and_return({ 'redis_version' => '2.5.0' })

    bf = factory({ size: 1000, error_rate: 0.01, key_name: 'ossom', redis: redis_mock }, nil)
    expect(bf.driver).to be_kind_of(Redis::BloomfilterDriver::Lua)

    bf = factory({ size: 1000, error_rate: 0.01, key_name: 'ossom', redis: redis_mock_2_5 }, nil)
    expect(bf.driver).to be_kind_of(Redis::BloomfilterDriver::Ruby)
  end

  it 'should create a Redis::Bloomfilter object' do
    bf = factory({ size: 1000, error_rate: 0.01, key_name: 'ossom' }, 'ruby')
    expect(bf).to be
    expect(bf.options[:size]).to eq 1000
    expect(bf.options[:bits]).to eq 9585
    expect(bf.options[:hashes]).to eq 6
    expect(bf.options[:key_name]).to eq 'ossom'
    bf.clear
  end

  it 'should load the lua script when not exists' do
    redis = Redis.new(url: redis_url)
    redis.script(:flush)
    redis_mock = flexmock(redis)
    redis_mock.should_receive(:evalsha).with(String, Hash).once.and_raise(Redis::CommandError.new("NOSCRIPT No matching script. Please use EVAL.")).pass_thru
    redis_mock.should_receive(:script).with(:load, String).once.pass_thru
    redis_mock.should_receive(:evalsha).once.pass_thru
    bf = factory({ size: 1000, error_rate: 0.01, key_name: 'osson', redis: redis_mock }, nil)
    bf.insert "blah"
    redis_mock.should_receive(:evalsha).once.pass_thru
    bf.insert "blue"
    redis_mock.should_receive(:evalsha).with(String, Hash).once.and_raise(Redis::CommandError.new("NOSCRIPT No matching script. Please use EVAL.")).pass_thru
    redis_mock.should_receive(:script).with(:load, String).once.pass_thru
    redis_mock.should_receive(:evalsha).once.pass_thru
    expect(bf.include?("blah")).to be true
  end

  it 'should not load the lua script when already loaded' do
    redis = Redis.new(url: redis_url)
    redis.script(:flush)
    redis.script(:load, Redis::BloomfilterDriver::Lua.get_script(:add))
    redis_mock = flexmock(redis)
    redis_mock.should_receive(:evalsha).once.pass_thru
    bf = factory({ size: 1000, error_rate: 0.01, key_name: 'osson', redis: redis_mock }, nil)
    bf.insert "blah"
  end

  %w[ruby lua ruby-test].each do |driver|
    it 'should work' do
      bf = factory({ size: 1000, error_rate: 0.01, key_name: '__test_bf' }, driver)
      bf.clear
      expect(bf.include?('asdlol')).to be false
      bf.insert 'asdlol'
      expect(bf.include?('asdlol')).to be true
      bf.clear
      expect(bf.include?('asdlol')).to be false
    end

    it 'should honor the error rate' do
      bf = factory({ size: 100, error_rate: 0.02, key_name: '__test_bf' }, driver)
      bf.clear
      e = test_error_rate bf, 180
      expect(e.round(2)).to be <= bf.options[:error_rate].round(2)
      bf.clear
    end

    it 'should add an element to the filter' do
      bf = factory({ size: 100, error_rate: 0.01, key_name: '__test_bf' }, driver)
      bf.insert 'asdlolol'
      expect(bf.include?('asdlolol')).to be true
    end

    it "should add ttl to the filter when requested (#{driver})" do
      bf = factory({ size: 100, error_rate: 0.01, key_name: "__test_bf_#{driver}" }, driver)
      bf.insert('asdlolol', 120)
      expect(Redis.current.ttl("__test_bf_#{driver}#{driver == 'lua' ? ':1' : ''}")).to be > 0
    end

  end

  it 'should be a scalable bloom filter' do
    bf = factory({ size: 100, error_rate: 0.02, key_name: '__test_bf' }, 'lua')
    bf.clear
    e = test_error_rate(bf, 150)
    expect(e).to be <= bf.options[:error_rate]
    bf.clear
  end
end
