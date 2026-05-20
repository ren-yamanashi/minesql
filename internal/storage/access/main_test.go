package access

import (
	"os"
	"testing"
	"time"

	"github.com/ren-yamanashi/minesql/internal/storage/config"
)

func TestMain(m *testing.M) {
	config.LockWaitTimeout = 100 * time.Millisecond
	os.Exit(m.Run())
}
