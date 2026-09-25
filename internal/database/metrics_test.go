package database

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/gateixeira/live-actions/pkg/logger"
	"github.com/stretchr/testify/require"
)

// newTestDBWrapper spins up a real (migrated) SQLite-backed DBWrapper in a
// temp dir, since GetMetricsHistory's downsampling relies on SQLite-specific
// SQL (strftime bucketing) that a mock can't exercise.
func newTestDBWrapper(t *testing.T) *DBWrapper {
	t.Helper()
	logger.InitLogger("error")
	dir := t.TempDir()
	w, r, err := InitDB(filepath.Join(dir, "t.db"))
	require.NoError(t, err)
	t.Cleanup(func() {
		w.Close()
		r.Close()
	})
	return &DBWrapper{writeDB: w, readDB: r}
}

// insertSnapshotAt inserts a metrics_snapshots row with an explicit
// timestamp, bypassing InsertMetricsSnapshot (which always stamps "now") so
// tests can control exactly how far back a row falls.
func insertSnapshotAt(t *testing.T, db *DBWrapper, ts time.Time, running, queued int) {
	t.Helper()
	_, err := db.writeDB.ExecContext(context.Background(),
		"INSERT INTO metrics_snapshots (timestamp, running_jobs, queued_jobs) VALUES (?, ?, ?)",
		ts.UTC().Format("2006-01-02 15:04:05"), running, queued,
	)
	require.NoError(t, err)
}

func TestGetMetricsHistory_Empty(t *testing.T) {
	db := newTestDBWrapper(t)

	snapshots, err := db.GetMetricsHistory(context.Background(), time.Hour)
	require.NoError(t, err)
	require.Empty(t, snapshots)
}

func TestGetMetricsHistory_ExcludesSnapshotsOutsideWindow(t *testing.T) {
	db := newTestDBWrapper(t)
	now := time.Now().UTC()

	insertSnapshotAt(t, db, now.Add(-2*time.Hour), 5, 5) // outside a 1h window
	insertSnapshotAt(t, db, now.Add(-1*time.Minute), 3, 1)

	snapshots, err := db.GetMetricsHistory(context.Background(), time.Hour)
	require.NoError(t, err)
	require.Len(t, snapshots, 1)
	require.Equal(t, 3, snapshots[0].Running)
	require.Equal(t, 1, snapshots[0].Queued)
}

func TestGetMetricsHistory_DownsamplesLargeHistory(t *testing.T) {
	db := newTestDBWrapper(t)
	now := time.Now().UTC()

	// One snapshot every 2s (the real collection cadence) across a full day
	// is 43,200 rows. Insert enough of them, spread across the whole day, to
	// confirm the response stays bounded near targetHistoryPoints rather
	// than growing with the raw row count.
	const rawCount = 6000
	step := 24 * time.Hour / rawCount
	for i := 0; i < rawCount; i++ {
		insertSnapshotAt(t, db, now.Add(-24*time.Hour+time.Duration(i)*step), i%10, i%4)
	}

	snapshots, err := db.GetMetricsHistory(context.Background(), 24*time.Hour)
	require.NoError(t, err)
	require.NotEmpty(t, snapshots)
	require.LessOrEqualf(t, len(snapshots), targetHistoryPoints+1,
		"expected downsampling to cap points near %d, got %d from %d raw rows",
		targetHistoryPoints, len(snapshots), rawCount)

	// Bucketed timestamps must be strictly increasing (no duplicate/out of
	// order buckets) so the chart's x-axis stays sane.
	for i := 1; i < len(snapshots); i++ {
		require.Greater(t, snapshots[i].Timestamp, snapshots[i-1].Timestamp)
	}
}

func TestGetMetricsHistory_AveragesWithinABucket(t *testing.T) {
	db := newTestDBWrapper(t)
	now := time.Now().UTC()

	// Use a 1h window: targetHistoryPoints=288 gives a 12s bucket width, so
	// four snapshots 2s apart all land in the same bucket - as long as the
	// base is aligned to a bucket boundary (the query buckets by absolute
	// epoch time, so an unaligned base could straddle two buckets depending
	// on when the test happens to run).
	const bucketWidth = int64(time.Hour/time.Second) / targetHistoryPoints // 12s
	baseUnix := now.Add(-30 * time.Minute).Unix()
	baseUnix -= baseUnix % bucketWidth
	base := time.Unix(baseUnix, 0).UTC()
	insertSnapshotAt(t, db, base, 2, 10)
	insertSnapshotAt(t, db, base.Add(2*time.Second), 4, 20)
	insertSnapshotAt(t, db, base.Add(4*time.Second), 6, 30)
	insertSnapshotAt(t, db, base.Add(6*time.Second), 8, 40)

	snapshots, err := db.GetMetricsHistory(context.Background(), time.Hour)
	require.NoError(t, err)
	require.Len(t, snapshots, 1)
	require.Equal(t, 5, snapshots[0].Running) // avg(2,4,6,8) = 5
	require.Equal(t, 25, snapshots[0].Queued) // avg(10,20,30,40) = 25
}

func TestGetMetricsHistory_SeparatesDistantBuckets(t *testing.T) {
	db := newTestDBWrapper(t)
	now := time.Now().UTC()

	// Two snapshots far enough apart (relative to the 1h window's ~12s
	// buckets) that they must land in different buckets and stay distinct
	// rather than being averaged together.
	insertSnapshotAt(t, db, now.Add(-45*time.Minute), 10, 0)
	insertSnapshotAt(t, db, now.Add(-5*time.Minute), 50, 0)

	snapshots, err := db.GetMetricsHistory(context.Background(), time.Hour)
	require.NoError(t, err)
	require.Len(t, snapshots, 2)
	require.Equal(t, 10, snapshots[0].Running)
	require.Equal(t, 50, snapshots[1].Running)
	require.Less(t, snapshots[0].Timestamp, snapshots[1].Timestamp)
}
