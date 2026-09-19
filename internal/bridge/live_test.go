package bridge

import (
	"context"
	"encoding/xml"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	pb "go-txmlconnector/proto"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// Opt-in check for a freshly started, dedicated bridge. This test claims the
// session and verifies handover after disconnecting. Never target a live
// trading session. It sends no connect or trading commands.
func TestLiveDLLBridge(t *testing.T) {
	target, health := os.Getenv("TXML_SMOKE_TARGET"), os.Getenv("TXML_SMOKE_HTTP")
	if target == "" || health == "" {
		t.Skip("set TXML_SMOKE_TARGET and TXML_SMOKE_HTTP for a dedicated test process")
	}
	httpClient := &http.Client{Timeout: time.Second}
	readyStatus := func() int {
		resp, err := httpClient.Get(health + "/readyz")
		if err != nil {
			return 0
		}
		defer resp.Body.Close()
		return resp.StatusCode
	}
	require.Eventually(t, func() bool { return readyStatus() == http.StatusOK }, 60*time.Second, 100*time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	client := pb.NewConnectServiceClient(conn)
	stream, err := client.FetchResponseData(ctx, &pb.DataRequest{})
	require.NoError(t, err)
	_, err = stream.Header()
	require.NoError(t, err)
	ack, err := client.SendCommand(ctx, &pb.SendCommandRequest{Message: `<command id="get_connector_version"/>`})
	require.NoError(t, err)
	require.Contains(t, ack.Message, `success="true"`)
	event, err := stream.Recv()
	require.NoError(t, err)
	require.Contains(t, event.Message, "connector_version")
	rejectionResponse, err := client.SendCommand(ctx, &pb.SendCommandRequest{Message: `<command id="__bridge_smoke_invalid_command__"/>`})
	if err == nil {
		// Some DLL revisions encode command rejection in result/message.
		var rejection struct {
			Success bool   `xml:"success,attr"`
			Message string `xml:"message"`
		}
		require.NoError(t, xml.Unmarshal([]byte(rejectionResponse.Message), &rejection))
		require.False(t, rejection.Success)
		require.NotEmpty(t, rejection.Message)
	} else {
		require.Equal(t, codes.Internal, status.Code(err))
		require.Equal(t, "CONNECTOR_ERROR", errorReason(err))
		require.NotContains(t, err.Error(), "nil response")
	}
	other, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer other.Close()
	_, err = pb.NewConnectServiceClient(other).SendCommand(ctx, &pb.SendCommandRequest{Message: `<command id="get_connector_version"/>`})
	require.Equal(t, "CLIENT_ALREADY_ATTACHED", errorReason(err))
	require.Equal(t, http.StatusOK, readyStatus())
	metrics, err := httpClient.Get(health + "/metrics")
	require.NoError(t, err)
	defer metrics.Body.Close()
	data, err := io.ReadAll(metrics.Body)
	require.NoError(t, err)
	require.Contains(t, string(data), `txml_commands_total{command="get_connector_version",outcome="accepted"} 1`)
	require.Contains(t, string(data), "txml_client_attached 1")
	require.NoError(t, conn.Close())
	next := pb.NewConnectServiceClient(other)
	require.Eventually(t, func() bool {
		_, callErr := next.SendCommand(ctx, &pb.SendCommandRequest{Message: `<command id="get_connector_version"/>`})
		return callErr == nil
	}, 5*time.Second, 50*time.Millisecond)
	nextStream, err := next.FetchResponseData(ctx, &pb.DataRequest{})
	require.NoError(t, err)
	event, err = nextStream.Recv()
	require.NoError(t, err)
	require.Contains(t, event.Message, "connector_version")
	require.Equal(t, http.StatusOK, readyStatus())
}
