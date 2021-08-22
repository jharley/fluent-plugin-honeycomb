require_relative './helper'
require 'fluent/time'
require 'fluent/test/driver/filter'

require 'fluent/test'
require 'fluent/test/helpers'

include Fluent::Test::Helpers

class HoneycombSpanEventFilter < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
    require 'fluent/plugin/filter_honeycomb_spanevent'
    @old_tz = ENV["TZ"]
    ENV["TZ"] = "UTC"
    @default_newline = if Fluent.windows?
                         "\r\n"
                       else
                         "\n"
                       end
  end

  def teardown
    super
    Timecop.return
    ENV["TZ"] = @old_tz
  end

  def create_driver(conf = '')
    Fluent::Test::Driver::Filter.new(Fluent::Plugin::HoneycombSpanEventFilter).configure(conf)
  end

  def filter(d, time, record)
    d.run {
      d.feed("filter.test", time, record)
    }
    d.filtered
  end

  def test_non_span_log_empty
    # log messages missing span_id, trace_id, and @timestamp fields are dropped
    d = create_driver
    filtered = filter(d, event_time, {'test' => 'empty'})
    assert_equal([], filtered)
  end

  def test_span_log
    timestamp = '2021-08-21T14:56:26.914Z'

    input = {
      '@timestamp' => timestamp,
      '@version' => '1',
      'level' => 'DEBUG',
      'message' => 'A structured log',
      'trace_id' => 'ce94fa5b1960d6812c6f7688fee8ad45',
      'span_id' => 'ffadccc6155863e8',
      'parent_span_id' => '3c4e7bcabb72623f',
      'extra' => 'stuff'
    }
    expected = [event_time(timestamp), {
      'meta.annotation_type' => 'span_event',
      'name' => 'fluentd log',
      'trace.parent_id' => 'ffadccc6155863e8',
      'trace.trace_id' => 'ce94fa5b1960d6812c6f7688fee8ad45',
      'level' => 'DEBUG',
      'message' => 'A structured log',
      'extra' => 'stuff'
    }]

    d = create_driver
    filtered = filter(d, event_time, input)
    assert_equal(filtered, [expected])
  end
end
