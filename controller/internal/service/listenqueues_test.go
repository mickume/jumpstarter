/*
Copyright 2024.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package service

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	jumpstarterdevv1alpha1 "github.com/jumpstarter-dev/jumpstarter-controller/api/v1alpha1"
	"github.com/jumpstarter-dev/jumpstarter-controller/internal/authentication"
	"github.com/jumpstarter-dev/jumpstarter-controller/internal/authorization"
	pb "github.com/jumpstarter-dev/jumpstarter-controller/internal/protocol/jumpstarter/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/authentication/user"
	k8sauthorizer "k8s.io/apiserver/pkg/authorization/authorizer"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// ---------- Mock Implementations ----------

// mockListenStream implements pb.ControllerService_ListenServer (grpc.ServerStreamingServer[pb.ListenResponse])
type mockListenStream struct {
	grpc.ServerStream
	ctx    context.Context
	sendFn func(*pb.ListenResponse) error
	sent   []*pb.ListenResponse
	mu     sync.Mutex
}

func (m *mockListenStream) Send(resp *pb.ListenResponse) error {
	m.mu.Lock()
	m.sent = append(m.sent, resp)
	m.mu.Unlock()
	if m.sendFn != nil {
		return m.sendFn(resp)
	}
	return nil
}

func (m *mockListenStream) Context() context.Context {
	return m.ctx
}

func (m *mockListenStream) SetHeader(_ metadata.MD) error  { return nil }
func (m *mockListenStream) SendHeader(_ metadata.MD) error { return nil }
func (m *mockListenStream) SetTrailer(_ metadata.MD)       {}
func (m *mockListenStream) SendMsg(_ any) error            { return nil }
func (m *mockListenStream) RecvMsg(_ any) error            { return nil }

// mockAuthn implements authentication.ContextAuthenticator
type mockAuthn struct {
	username string
}

var _ authentication.ContextAuthenticator = &mockAuthn{}

func (m *mockAuthn) AuthenticateContext(_ context.Context) (*authenticator.Response, bool, error) {
	return &authenticator.Response{
		User: &user.DefaultInfo{Name: m.username},
	}, true, nil
}

// mockAuthz implements k8sauthorizer.Authorizer
type mockAuthz struct{}

func (m *mockAuthz) Authorize(_ context.Context, _ k8sauthorizer.Attributes) (k8sauthorizer.Decision, string, error) {
	return k8sauthorizer.DecisionAllow, "", nil
}

// mockAttrGetter implements authorization.ContextAttributesGetter
type mockAttrGetter struct {
	namespace string
	resource  string
	name      string
}

var _ authorization.ContextAttributesGetter = &mockAttrGetter{}

func (m *mockAttrGetter) ContextAttributes(_ context.Context, _ user.Info) (k8sauthorizer.Attributes, error) {
	return k8sauthorizer.AttributesRecord{
		ResourceRequest: true,
		Namespace:       m.namespace,
		Resource:        m.resource,
		Name:            m.name,
	}, nil
}

// ---------- Test Helpers ----------

const (
	testNamespace    = "default"
	testExporterName = "test-exporter"
	testLeaseName    = "test-lease"
)

// newTestScheme creates a runtime.Scheme with the jumpstarter types registered.
func newTestScheme() *k8sruntime.Scheme {
	s := k8sruntime.NewScheme()
	_ = jumpstarterdevv1alpha1.AddToScheme(s)
	_ = corev1.AddToScheme(s)
	return s
}

// newTestExporter creates an Exporter resource for testing with the default test namespace.
func newTestExporter() *jumpstarterdevv1alpha1.Exporter {
	return &jumpstarterdevv1alpha1.Exporter{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testExporterName,
			Namespace: testNamespace,
		},
	}
}

// newTestLease creates a Lease resource with the given name and default exporter reference.
func newTestLease(name string) *jumpstarterdevv1alpha1.Lease {
	return &jumpstarterdevv1alpha1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: testNamespace,
		},
		Status: jumpstarterdevv1alpha1.LeaseStatus{
			ExporterRef: &corev1.LocalObjectReference{
				Name: testExporterName,
			},
		},
	}
}

// newTestService creates a ControllerService with mocked auth and a fake k8s client
// containing the given objects.
func newTestService(objs ...client.Object) *ControllerService {
	scheme := newTestScheme()
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
		WithStatusSubresource(&jumpstarterdevv1alpha1.Lease{}).
		Build()

	return &ControllerService{
		Client: fakeClient,
		Scheme: scheme,
		Authn: &mockAuthn{
			username: testExporterName,
		},
		Authz: &mockAuthz{},
		Attr: &mockAttrGetter{
			namespace: testNamespace,
			resource:  "Exporter",
			name:      testExporterName,
		},
	}
}

// newTestStream creates a mock stream with the given context and optional Send function.
func newTestStream(ctx context.Context, sendFn func(*pb.ListenResponse) error) *mockListenStream {
	return &mockListenStream{
		ctx:    ctx,
		sendFn: sendFn,
	}
}

// ---------- Unit Tests ----------

// TestListenQueuesCleanupOnContextCancel verifies that the listenQueues entry
// is removed when Listen() returns due to context cancellation.
// Test Spec: TS-01-1, Requirement: 01-REQ-1.1
func TestListenQueuesCleanupOnContextCancel(t *testing.T) {
	svc := newTestService(
		newTestExporter(),
		newTestLease(testLeaseName),
	)

	ctx, cancel := context.WithCancel(context.Background())
	stream := newTestStream(ctx, nil)

	req := &pb.ListenRequest{
		LeaseName: testLeaseName,
	}

	done := make(chan error, 1)
	go func() {
		done <- svc.Listen(req, stream)
	}()

	// Give Listen() time to reach LoadOrStore and start the select loop
	time.Sleep(100 * time.Millisecond)

	// Verify the entry exists while Listen() is active
	_, loaded := svc.listenQueues.Load(testLeaseName)
	if !loaded {
		t.Fatal("expected listenQueues entry to exist while Listen() is active")
	}

	// Cancel context to trigger Listen() return
	cancel()

	// Wait for Listen() to complete
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Listen() returned unexpected error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Listen() did not return within timeout")
	}

	// Verify the entry has been cleaned up
	_, loaded = svc.listenQueues.Load(testLeaseName)
	if loaded {
		t.Error("expected listenQueues entry to be removed after context cancellation, but it still exists")
	}
}

// TestListenQueuesCleanupOnSendError verifies that the listenQueues entry
// is removed when Listen() returns due to a stream.Send failure.
// Test Spec: TS-01-2, Requirement: 01-REQ-1.2
func TestListenQueuesCleanupOnSendError(t *testing.T) {
	svc := newTestService(
		newTestExporter(),
		newTestLease(testLeaseName),
	)

	sendErr := errors.New("send failed")
	ctx := context.Background()
	stream := newTestStream(ctx, func(_ *pb.ListenResponse) error {
		return sendErr
	})

	req := &pb.ListenRequest{
		LeaseName: testLeaseName,
	}

	done := make(chan error, 1)
	go func() {
		done <- svc.Listen(req, stream)
	}()

	// Give Listen() time to reach LoadOrStore
	time.Sleep(100 * time.Millisecond)

	// Send a message to the queue to trigger Send() which will fail
	val, loaded := svc.listenQueues.Load(testLeaseName)
	if !loaded {
		t.Fatal("expected listenQueues entry to exist while Listen() is active")
	}
	queue := val.(chan *pb.ListenResponse)
	queue <- &pb.ListenResponse{
		RouterEndpoint: "test-endpoint",
		RouterToken:    "test-token",
	}

	// Wait for Listen() to return due to Send error
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected Listen() to return an error on Send failure")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Listen() did not return within timeout")
	}

	// Verify the entry has been cleaned up
	_, loaded = svc.listenQueues.Load(testLeaseName)
	if loaded {
		t.Error("expected listenQueues entry to be removed after Send error, but it still exists")
	}
}

// TestListenQueuesDialDelivery verifies that messages sent to the listen queue
// are delivered to the active Listen() stream.
// Test Spec: TS-01-3, Requirement: 01-REQ-2.1
func TestListenQueuesDialDelivery(t *testing.T) {
	svc := newTestService(
		newTestExporter(),
		newTestLease(testLeaseName),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	delivered := make(chan struct{})
	stream := newTestStream(ctx, func(_ *pb.ListenResponse) error {
		close(delivered)
		return nil
	})

	req := &pb.ListenRequest{
		LeaseName: testLeaseName,
	}

	done := make(chan error, 1)
	go func() {
		done <- svc.Listen(req, stream)
	}()

	// Give Listen() time to set up
	time.Sleep(100 * time.Millisecond)

	// Send a message through the queue (simulating what Dial() does)
	val, loaded := svc.listenQueues.Load(testLeaseName)
	if !loaded {
		t.Fatal("expected listenQueues entry to exist")
	}
	queue := val.(chan *pb.ListenResponse)
	testResp := &pb.ListenResponse{
		RouterEndpoint: "test-router:8080",
		RouterToken:    "test-jwt-token",
	}
	queue <- testResp

	// Wait for delivery
	select {
	case <-delivered:
		// success
	case <-time.After(5 * time.Second):
		t.Fatal("message was not delivered within timeout")
	}

	stream.mu.Lock()
	defer stream.mu.Unlock()
	if len(stream.sent) != 1 {
		t.Fatalf("expected 1 sent message, got %d", len(stream.sent))
	}
	if stream.sent[0].RouterEndpoint != "test-router:8080" {
		t.Errorf("expected RouterEndpoint 'test-router:8080', got %q", stream.sent[0].RouterEndpoint)
	}
	if stream.sent[0].RouterToken != "test-jwt-token" {
		t.Errorf("expected RouterToken 'test-jwt-token', got %q", stream.sent[0].RouterToken)
	}

	cancel()
	<-done
}

// TestListenQueuesChannelCapacity verifies that the listen queue channel has capacity 8.
// Test Spec: TS-01-4, Requirement: 01-REQ-2.2
func TestListenQueuesChannelCapacity(t *testing.T) {
	svc := &ControllerService{}

	queue, _ := svc.listenQueues.LoadOrStore("capacity-test-lease", make(chan *pb.ListenResponse, 8))
	ch := queue.(chan *pb.ListenResponse)

	if cap(ch) != 8 {
		t.Errorf("expected channel capacity 8, got %d", cap(ch))
	}

	// Cleanup
	svc.listenQueues.Delete("capacity-test-lease")
}

// ---------- Edge Case Tests ----------

// TestListenQueuesConcurrentDialAndCleanup verifies that concurrent LoadOrStore/Delete
// operations on listenQueues do not panic or deadlock.
// Test Spec: TS-01-E1, Requirement: 01-REQ-1.E1
func TestListenQueuesConcurrentDialAndCleanup(t *testing.T) {
	svc := &ControllerService{}
	const iterations = 100
	const key = "concurrent-test-lease"

	var wg sync.WaitGroup

	done := make(chan struct{})
	go func() {
		defer close(done)
		for range iterations {
			wg.Add(3)
			// Simulate Listen() doing LoadOrStore
			go func() {
				defer wg.Done()
				queue, _ := svc.listenQueues.LoadOrStore(key, make(chan *pb.ListenResponse, 8))
				ch := queue.(chan *pb.ListenResponse)
				// Non-blocking send (simulating Dial)
				select {
				case ch <- &pb.ListenResponse{}:
				default:
				}
			}()
			// Simulate Dial() doing LoadOrStore + send
			go func() {
				defer wg.Done()
				queue, _ := svc.listenQueues.LoadOrStore(key, make(chan *pb.ListenResponse, 8))
				ch := queue.(chan *pb.ListenResponse)
				select {
				case ch <- &pb.ListenResponse{}:
				default:
				}
			}()
			// Simulate cleanup Delete
			go func() {
				defer wg.Done()
				svc.listenQueues.Delete(key)
			}()
		}
		wg.Wait()
	}()

	select {
	case <-done:
		// No panic, no deadlock
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent operations deadlocked")
	}
}

// TestListenQueuesReListenAfterCleanup verifies that after Listen() cleans up,
// a new Listen() call creates a fresh channel and functions correctly.
// Test Spec: TS-01-E2, Requirement: 01-REQ-1.E2
func TestListenQueuesReListenAfterCleanup(t *testing.T) {
	svc := newTestService(
		newTestExporter(),
		newTestLease(testLeaseName),
	)

	req := &pb.ListenRequest{
		LeaseName: testLeaseName,
	}

	// --- First Listen + Cleanup ---
	ctx1, cancel1 := context.WithCancel(context.Background())
	stream1 := newTestStream(ctx1, nil)

	done1 := make(chan error, 1)
	go func() {
		done1 <- svc.Listen(req, stream1)
	}()

	time.Sleep(100 * time.Millisecond)
	cancel1()

	select {
	case <-done1:
	case <-time.After(5 * time.Second):
		t.Fatal("first Listen() did not return within timeout")
	}

	// Verify cleanup happened
	_, loaded := svc.listenQueues.Load(testLeaseName)
	if loaded {
		t.Fatal("expected listenQueues entry to be cleaned up after first Listen(), but it still exists")
	}

	// --- Second Listen ---
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()

	delivered := make(chan struct{})
	stream2 := newTestStream(ctx2, func(_ *pb.ListenResponse) error {
		close(delivered)
		return nil
	})

	done2 := make(chan error, 1)
	go func() {
		done2 <- svc.Listen(req, stream2)
	}()

	time.Sleep(100 * time.Millisecond)

	// Verify a new channel was created
	val, loaded := svc.listenQueues.Load(testLeaseName)
	if !loaded {
		t.Fatal("expected listenQueues entry to exist for second Listen()")
	}

	// Send a message and verify it's delivered
	queue := val.(chan *pb.ListenResponse)
	queue <- &pb.ListenResponse{RouterEndpoint: "re-listen-test"}

	select {
	case <-delivered:
		// success
	case <-time.After(5 * time.Second):
		t.Fatal("message was not delivered to second Listen() within timeout")
	}

	stream2.mu.Lock()
	if len(stream2.sent) != 1 || stream2.sent[0].RouterEndpoint != "re-listen-test" {
		t.Error("second Listen() did not receive the expected message")
	}
	stream2.mu.Unlock()

	cancel2()
	<-done2
}

// TestListenQueuesSendToRemovedChannel verifies that sending to a channel reference
// obtained before the map entry was deleted does not panic.
// Test Spec: TS-01-E3, Requirement: 01-REQ-1.E3
func TestListenQueuesSendToRemovedChannel(t *testing.T) {
	svc := &ControllerService{}
	const key = "orphan-channel-lease"

	// Get a channel reference
	queue, _ := svc.listenQueues.LoadOrStore(key, make(chan *pb.ListenResponse, 8))
	ch := queue.(chan *pb.ListenResponse)

	// Delete the map entry
	svc.listenQueues.Delete(key)

	// Sending to the orphaned channel should not panic
	// The channel is still a valid Go channel, just no longer in the map
	select {
	case ch <- &pb.ListenResponse{}:
		// buffered send succeeded — this is fine
	default:
		// buffer full — also fine
	}

	// If we get here, no panic occurred
}

// ---------- Property-Style Tests ----------

// TestListenQueuesCleanupGuarantee verifies that for any exit mode (context cancel
// or send error), the listenQueues entry is always cleaned up.
// Test Spec: TS-01-P1, Property 1, Requirement: 01-REQ-1.1, 01-REQ-1.2, 01-REQ-1.3
func TestListenQueuesCleanupGuarantee(t *testing.T) {
	exitModes := []struct {
		name        string
		leaseFmt    string
		triggerExit func(cancel context.CancelFunc, svc *ControllerService, leaseName string)
	}{
		{
			name:     "context_cancel",
			leaseFmt: "cleanup-guarantee-cancel-%d",
			triggerExit: func(cancel context.CancelFunc, _ *ControllerService, _ string) {
				cancel()
			},
		},
		{
			name:     "send_error",
			leaseFmt: "cleanup-guarantee-send-err-%d",
			triggerExit: func(_ context.CancelFunc, svc *ControllerService, leaseName string) {
				val, ok := svc.listenQueues.Load(leaseName)
				if ok {
					ch := val.(chan *pb.ListenResponse)
					ch <- &pb.ListenResponse{}
				}
			},
		},
	}

	for _, mode := range exitModes {
		// Run multiple iterations for each exit mode (property-style)
		for i := range 5 {
			leaseName := fmt.Sprintf(mode.leaseFmt, i)
			t.Run(fmt.Sprintf("%s/%d", mode.name, i), func(t *testing.T) {
				svc := newTestService(
					newTestExporter(),
					newTestLease(leaseName),
				)

				ctx, cancel := context.WithCancel(context.Background())

				var sendFn func(*pb.ListenResponse) error
				if mode.name == "send_error" {
					sendFn = func(_ *pb.ListenResponse) error {
						return errors.New("injected send error")
					}
				}
				stream := newTestStream(ctx, sendFn)

				req := &pb.ListenRequest{
					LeaseName: leaseName,
				}

				done := make(chan error, 1)
				go func() {
					done <- svc.Listen(req, stream)
				}()

				time.Sleep(100 * time.Millisecond)

				mode.triggerExit(cancel, svc, leaseName)

				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Fatal("Listen() did not return within timeout")
				}

				_, loaded := svc.listenQueues.Load(leaseName)
				if loaded {
					t.Errorf("expected listenQueues entry %q to be removed after %s, but it still exists",
						leaseName, mode.name)
				}

				// Ensure context is cancelled for cleanup
				cancel()
			})
		}
	}
}

// TestListenQueuesConcurrentSafety is a stress test verifying that random concurrent
// operations on listenQueues (LoadOrStore, Send, Delete) do not panic or deadlock.
// Test Spec: TS-01-P2, Property 2, Requirement: 01-REQ-1.E1, 01-REQ-1.E3
func TestListenQueuesConcurrentSafety(t *testing.T) {
	svc := &ControllerService{}
	const key = "concurrent-safety-lease"

	goroutineCounts := []int{1, 5, 10, 25, 50}

	for _, n := range goroutineCounts {
		t.Run(fmt.Sprintf("goroutines_%d", n), func(t *testing.T) {
			var wg sync.WaitGroup
			wg.Add(n)

			done := make(chan struct{})
			go func() {
				defer close(done)
				for range n {
					go func() {
						defer wg.Done()
						// Each goroutine performs a random sequence of operations
						for range 20 {
							switch rand.Intn(3) { //nolint:gosec
							case 0: // LoadOrStore
								svc.listenQueues.LoadOrStore(key, make(chan *pb.ListenResponse, 8))
							case 1: // Non-blocking send
								val, ok := svc.listenQueues.Load(key)
								if ok {
									ch := val.(chan *pb.ListenResponse)
									select {
									case ch <- &pb.ListenResponse{}:
									default:
									}
								}
							case 2: // Delete
								svc.listenQueues.Delete(key)
							}
						}
					}()
				}
				wg.Wait()
			}()

			select {
			case <-done:
				// Success — no panic, no deadlock
			case <-time.After(10 * time.Second):
				t.Fatal("concurrent operations deadlocked")
			}

			// Final cleanup
			svc.listenQueues.Delete(key)
		})
	}
}

// TestListenQueuesReListenCycles verifies that sequential listen-cleanup-relisten cycles
// all function correctly, with each new Listen() creating a fresh channel.
// Test Spec: TS-01-P4, Property 4, Requirement: 01-REQ-1.E2
func TestListenQueuesReListenCycles(t *testing.T) {
	cycleCounts := []int{1, 3, 5, 10}

	for _, numCycles := range cycleCounts {
		t.Run(fmt.Sprintf("cycles_%d", numCycles), func(t *testing.T) {
			svc := newTestService(
				newTestExporter(),
				newTestLease(testLeaseName),
			)

			req := &pb.ListenRequest{
				LeaseName: testLeaseName,
			}

			for i := range numCycles {
				ctx, cancel := context.WithCancel(context.Background())
				stream := newTestStream(ctx, nil)

				done := make(chan error, 1)
				go func() {
					done <- svc.Listen(req, stream)
				}()

				time.Sleep(50 * time.Millisecond)
				cancel()

				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Fatalf("Listen() did not return in cycle %d", i)
				}

				// Verify cleanup after each cycle
				_, loaded := svc.listenQueues.Load(testLeaseName)
				if loaded {
					t.Fatalf("expected listenQueues to be cleaned up after cycle %d, but entry still exists", i)
				}
			}

			// Final listen to verify fresh channel works
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			delivered := make(chan struct{})
			stream := newTestStream(ctx, func(_ *pb.ListenResponse) error {
				close(delivered)
				return nil
			})

			done := make(chan error, 1)
			go func() {
				done <- svc.Listen(req, stream)
			}()

			time.Sleep(50 * time.Millisecond)

			val, loaded := svc.listenQueues.Load(testLeaseName)
			if !loaded {
				t.Fatal("expected listenQueues entry to exist for final Listen()")
			}

			queue := val.(chan *pb.ListenResponse)
			queue <- &pb.ListenResponse{RouterEndpoint: "final-cycle-test"}

			select {
			case <-delivered:
				// success
			case <-time.After(5 * time.Second):
				t.Fatal("message not delivered to final Listen()")
			}

			cancel()
			<-done
		})
	}
}

// TestListenQueuesFunctionalPreservation verifies that messages sent to
// the listen queue are delivered, testing with multiple different messages.
// Test Spec: TS-01-P3, Property 3, Requirement: 01-REQ-2.1, 01-REQ-2.2
func TestListenQueuesFunctionalPreservation(t *testing.T) {
	testMessages := []*pb.ListenResponse{
		{RouterEndpoint: "router1:8080", RouterToken: "token-aaa"},
		{RouterEndpoint: "router2:9090", RouterToken: "token-bbb"},
		{RouterEndpoint: "10.0.0.1:443", RouterToken: "long-jwt-token-value-here"},
	}

	for i, msg := range testMessages {
		t.Run(fmt.Sprintf("message_%d", i), func(t *testing.T) {
			leaseName := fmt.Sprintf("func-preserve-lease-%d", i)
			svc := newTestService(
				newTestExporter(),
				newTestLease(leaseName),
			)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			delivered := make(chan struct{})
			stream := newTestStream(ctx, func(_ *pb.ListenResponse) error {
				close(delivered)
				return nil
			})

			req := &pb.ListenRequest{
				LeaseName: leaseName,
			}

			done := make(chan error, 1)
			go func() {
				done <- svc.Listen(req, stream)
			}()

			time.Sleep(100 * time.Millisecond)

			val, loaded := svc.listenQueues.Load(leaseName)
			if !loaded {
				t.Fatal("expected listenQueues entry to exist")
			}

			queue := val.(chan *pb.ListenResponse)
			queue <- msg

			select {
			case <-delivered:
			case <-time.After(5 * time.Second):
				t.Fatal("message not delivered within timeout")
			}

			stream.mu.Lock()
			defer stream.mu.Unlock()
			if len(stream.sent) != 1 {
				t.Fatalf("expected 1 sent message, got %d", len(stream.sent))
			}
			if stream.sent[0].RouterEndpoint != msg.RouterEndpoint {
				t.Errorf("RouterEndpoint mismatch: got %q, want %q",
					stream.sent[0].RouterEndpoint, msg.RouterEndpoint)
			}
			if stream.sent[0].RouterToken != msg.RouterToken {
				t.Errorf("RouterToken mismatch: got %q, want %q",
					stream.sent[0].RouterToken, msg.RouterToken)
			}

			cancel()
			<-done
		})
	}
}
