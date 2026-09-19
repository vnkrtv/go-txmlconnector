package bridge

import "github.com/prometheus/client_golang/prometheus"

type Metrics struct {
	commands        *prometheus.CounterVec
	duration        *prometheus.HistogramVec
	events          prometheus.Counter
	delivered       prometheus.Counter
	callbackErrors  prometheus.Counter
	faults          *prometheus.CounterVec
	ready           prometheus.Gauge
	stream          prometheus.Gauge
	brokerConnected prometheus.Gauge
}

func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		commands:        prometheus.NewCounterVec(prometheus.CounterOpts{Name: "txml_commands_total", Help: "Commands by XML command id and outcome."}, []string{"command", "outcome"}),
		duration:        prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "txml_command_duration_seconds", Help: "RPC command latency including queue wait.", Buckets: prometheus.DefBuckets}, []string{"command"}),
		events:          prometheus.NewCounter(prometheus.CounterOpts{Name: "txml_events_received_total", Help: "Native callback messages received."}),
		delivered:       prometheus.NewCounter(prometheus.CounterOpts{Name: "txml_events_sent_total", Help: "Messages handed to gRPC (not application acknowledgements)."}),
		callbackErrors:  prometheus.NewCounter(prometheus.CounterOpts{Name: "txml_callback_errors_total", Help: "Asynchronous error XML events."}),
		faults:          prometheus.NewCounterVec(prometheus.CounterOpts{Name: "txml_session_faults_total", Help: "First fatal session fault by bounded reason."}, []string{"reason"}),
		ready:           prometheus.NewGauge(prometheus.GaugeOpts{Name: "txml_ready", Help: "Bridge initialized and accepting commands; not broker connectivity."}),
		stream:          prometheus.NewGauge(prometheus.GaugeOpts{Name: "txml_stream_active", Help: "Whether the single callback consumer is attached."}),
		brokerConnected: prometheus.NewGauge(prometheus.GaugeOpts{Name: "txml_broker_connected", Help: "Last delivered server_status: 1 connected, 0 disconnected/error, -1 unknown."}),
	}
	m.brokerConnected.Set(-1)
	reg.MustRegister(m.commands, m.duration, m.events, m.delivered, m.callbackErrors, m.faults, m.ready, m.stream, m.brokerConnected)
	return m
}
