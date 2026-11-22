package aistudio_test

import (
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/Mag1cFall/AIStudio2API/internal/aistudio"
)

func TestModelBusyErrorRequiresModelOverloadEvidence(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "high demand",
			err: &aistudio.RPCError{StatusCode: 503, Code: 14,
				Message: "This model is currently experiencing high demand. Spikes in demand are usually temporary. Please try again later."},
			want: true,
		},
		{
			name: "model overloaded wrapped",
			err: fmt.Errorf("generate: %w", &aistudio.RPCError{StatusCode: 503, Code: 14,
				Message: "The model is overloaded. Please try again later."}),
			want: true,
		},
		{
			name: "currently overloaded case insensitive",
			err: &aistudio.RPCError{StatusCode: 503, Code: 14,
				Message: "The MODEL is currently OVERLOADED, please retry."},
			want: true,
		},
		{
			name: "protocol code required",
			err: &aistudio.RPCError{StatusCode: 503, Code: 13,
				Message: "The model is overloaded."},
		},
		{
			name: "HTTP status required",
			err: &aistudio.RPCError{StatusCode: http.StatusInternalServerError, Code: 14,
				Message: "The model is overloaded."},
		},
		{
			name: "no HTTP status",
			err:  &aistudio.RPCError{Code: 14, Message: "The model is overloaded."},
		},
		{
			name: "generic unavailable",
			err:  &aistudio.RPCError{StatusCode: 503, Code: 14, Message: "Service unavailable. Please try again later."},
		},
		{
			name: "network failure",
			err:  &aistudio.RPCError{StatusCode: 503, Code: 14, Message: "upstream connection reset"},
		},
		{
			name: "proxy overload is not model overload",
			err:  &aistudio.RPCError{StatusCode: 503, Code: 14, Message: "Proxy server overloaded while forwarding a model request."},
		},
		{
			name: "quota is not busy",
			err:  &aistudio.RPCError{StatusCode: 503, Code: 14, Message: "Resource exhausted: daily model quota exceeded."},
		},
		{
			name: "plain error is not structured evidence",
			err:  errors.New("HTTP 503 code 14: The model is overloaded."),
		},
		{name: "nil"},
		{name: "typed nil", err: (*aistudio.RPCError)(nil)},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := aistudio.ModelBusyError(test.err); got != test.want {
				t.Errorf("ModelBusyError(%v)=%v, want %v", test.err, got, test.want)
			}
		})
	}
}
