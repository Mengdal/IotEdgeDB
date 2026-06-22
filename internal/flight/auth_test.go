package flight

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestExtractBearer_ValidToken(t *testing.T) {
	md := metadata.Pairs("authorization", "Bearer my-secret-token")
	ctx := metadata.NewIncomingContext(context.Background(), md)

	token, err := extractBearer(ctx)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if token != "my-secret-token" {
		t.Fatalf("expected 'my-secret-token', got %q", token)
	}
}

func TestExtractBearer_MissingMetadata(t *testing.T) {
	ctx := context.Background()

	_, err := extractBearer(ctx)
	if err == nil {
		t.Fatal("expected error for missing metadata")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got %T", err)
	}
	if st.Code() != codes.Unauthenticated {
		t.Fatalf("expected Unauthenticated, got %v", st.Code())
	}
}

func TestExtractBearer_MissingAuthorizationHeader(t *testing.T) {
	md := metadata.Pairs("some-other-key", "value")
	ctx := metadata.NewIncomingContext(context.Background(), md)

	_, err := extractBearer(ctx)
	st, _ := status.FromError(err)
	if st.Code() != codes.Unauthenticated {
		t.Fatalf("expected Unauthenticated, got %v", st.Code())
	}
}

func TestExtractBearer_WrongScheme(t *testing.T) {
	md := metadata.Pairs("authorization", "Basic YWxhZGRpbjpvcGVuc2VzYW1l")
	ctx := metadata.NewIncomingContext(context.Background(), md)

	_, err := extractBearer(ctx)
	st, _ := status.FromError(err)
	if st.Code() != codes.Unauthenticated {
		t.Fatalf("expected Unauthenticated, got %v", st.Code())
	}
	if st.Message() == "" {
		t.Fatal("expected message about Bearer scheme")
	}
}

func TestExtractBearer_EmptyBearer(t *testing.T) {
	md := metadata.Pairs("authorization", "Bearer ")
	ctx := metadata.NewIncomingContext(context.Background(), md)

	token, err := extractBearer(ctx)
	if err != nil {
		t.Fatalf("expected no error for empty Bearer token, got: %v", err)
	}
	if token != "" {
		t.Fatalf("expected empty token, got %q", token)
	}
}

func TestExtractBearer_MultipleHeaders_UsesFirst(t *testing.T) {
	md := metadata.MD{
		"authorization": []string{"Bearer first-token", "Bearer second-token"},
	}
	ctx := metadata.NewIncomingContext(context.Background(), md)

	token, err := extractBearer(ctx)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if token != "first-token" {
		t.Fatalf("expected 'first-token', got %q", token)
	}
}

func TestExtractBearer_EmptyAuthorizationValue(t *testing.T) {
	md := metadata.MD{
		"authorization": []string{""},
	}
	ctx := metadata.NewIncomingContext(context.Background(), md)

	_, err := extractBearer(ctx)
	st, _ := status.FromError(err)
	if st.Code() != codes.Unauthenticated {
		t.Fatalf("expected Unauthenticated for empty auth value, got %v", st.Code())
	}
}
