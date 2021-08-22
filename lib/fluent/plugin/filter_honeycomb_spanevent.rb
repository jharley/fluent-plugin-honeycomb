require 'fluent/filter'
require 'fluent/plugin'
require 'fluent/time'

module Fluent
  module Plugin
    class HoneycombSpanEventFilter < Filter
      # Converts a structured log with trace and span IDs present into a Honeycomb Span Event
      #
      # See: https://docs.honeycomb.io/getting-data-in/tracing/send-trace-data/#span-annotations
      Plugin.register_filter('honeycomb_spanevent', self)

      def initialize
        @has_filter_with_time = true
        super
      end

      def configure(conf)
        super
      end

      def filter_with_time(tag, time, record)
        if record.has_key?("span_id") && record.has_key?("trace_id") && record.has_key?('@timestamp')
            record["meta.annotation_type"] = "span_event"
            record["name"] = "fluentd log"

            # map span_id to trace.parent_id as we annotate our parent
            record["trace.parent_id"] = record.delete("span_id")
            # map trace_id to trace.trace_id
            record["trace.trace_id"] = record.delete("trace_id")
            # map logstash @timestamp to time
            timestamp = Fluent::EventTime.parse(record.delete('@timestamp'))

            if record.has_key?('@version')
              # remove logstash version field
              record.delete('@version')
            end
            if record.has_key?("parent_span_id")
              # remove parent_span_id if present to avoid confusion
              record.delete("parent_span_id")
            end

            return timestamp, record
        end

        # drop the event if it doesn't have the expected fields: not a span event
        return nil
      end
    end
  end
end
