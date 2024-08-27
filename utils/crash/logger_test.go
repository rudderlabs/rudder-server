package crash

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/logger/mock_logger"
)

func TestNotify(t *testing.T) {
	t.Run("capture panic", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		logger := mock_logger.NewMockLogger(ctrl)
		logger.EXPECT().Child("panic").Return(logger)

		ph := UsingLogger(logger, PanicWrapperOpts{})

		logger.EXPECT().Fataln("Panic detected. Application will crash.", gomock.Any()).Times(1)
		require.Panics(t, func() {
			defer ph.Notify("team")()
			panic("test")
		})

		t.Log("log only once")
		require.Panics(t, func() {
			defer ph.Notify("team")()
			panic("test")
		})
	})

	t.Run("no panics", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		logger := mock_logger.NewMockLogger(ctrl)
		logger.EXPECT().Child("panic").Return(logger)

		ph := UsingLogger(logger, PanicWrapperOpts{})

		ph.Notify("team")()
	})
}

func TestHandler(t *testing.T) {
	t.Run("capture panic", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		logger := mock_logger.NewMockLogger(ctrl)
		logger.EXPECT().Child("panic").Return(logger)

		ph := UsingLogger(logger, PanicWrapperOpts{})

		logger.EXPECT().Fataln("Panic detected. Application will crash.", gomock.Any()).Times(1)
		require.Panics(t, func() {
			ph.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				panic("test")
			})).ServeHTTP(nil, nil)
		})
	})

	t.Run("no panics", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		logger := mock_logger.NewMockLogger(ctrl)
		logger.EXPECT().Child("panic").Return(logger)

		ph := UsingLogger(logger, PanicWrapperOpts{})

		ph.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})).
			ServeHTTP(nil, nil)
	})
}
