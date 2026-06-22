package flight

import (
	"context"

	"iedb/internal/auth"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// extractBearer extracts a Bearer token from incoming gRPC metadata.
func extractBearer(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", status.Error(codes.Unauthenticated, "missing metadata")
	}

	vals := md.Get("authorization")
	if len(vals) == 0 {
		return "", status.Error(codes.Unauthenticated, "missing authorization header")
	}

	token := vals[0]
	const prefix = "Bearer "
	if len(token) < len(prefix) || token[:len(prefix)] != prefix {
		return "", status.Error(codes.Unauthenticated, "authorization header must use Bearer scheme")
	}

	return token[len(prefix):], nil
}

// verifyToken extracts and validates a Bearer token, returning TokenInfo.
func (s *Server) verifyToken(ctx context.Context) (*auth.TokenInfo, error) {
	token, err := extractBearer(ctx)
	if err != nil {
		return nil, err
	}

	info := s.authMgr.VerifyToken(token)
	if info == nil {
		return nil, status.Error(codes.Unauthenticated, "invalid or expired token")
	}

	return info, nil
}

// checkPermission verifies a token has the required permission.
// Falls back to token-level permission check in OSS mode (no RBAC).
func (s *Server) checkPermission(info *auth.TokenInfo, database, measurement, permission string) error {
	// First, check token-level permissions (OSS mode)
	if s.authMgr.HasPermission(info, permission) || s.authMgr.HasPermission(info, "admin") {
		return nil
	}

	// If RBAC is configured, also check RBAC permissions
	if s.rbacMgr != nil {
		result := s.rbacMgr.CheckPermission(&auth.PermissionCheckRequest{
			TokenInfo:   info,
			Database:    database,
			Measurement: measurement,
			Permission:  permission,
		})
		if result.Allowed {
			return nil
		}
	}

	return status.Error(codes.PermissionDenied, "insufficient permissions")
}
