// Package store 提供任务与运行记录的持久化。
//
// 目前实现基于纯 Go 的 SQLite（modernc.org/sqlite，无需 CGO）。
// 所有时间字段以 Unix 毫秒（UTC）存储，读写时自动转换。
package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	_ "modernc.org/sqlite" // 注册 sqlite 驱动
)

// ErrNotFound 表示记录不存在。
var ErrNotFound = errors.New("记录不存在")

// ErrConflict 表示违反唯一约束（如任务名重复）。
var ErrConflict = errors.New("记录已存在")

// Store 是持久化接口。
type Store interface {
	Migrate(ctx context.Context) error
	Close() error

	CreateTask(ctx context.Context, t *Task) error
	UpdateTask(ctx context.Context, t *Task) error
	GetTask(ctx context.Context, id int64) (*Task, error)
	GetTaskByName(ctx context.Context, name string) (*Task, error)
	ListTasks(ctx context.Context) ([]*Task, error)
	DeleteTask(ctx context.Context, id int64) error
	UpdateTaskRuntime(ctx context.Context, id int64, rt TaskRuntime) error
	// ClearTaskNextRun 清空任务的 next_run_at（任务被禁用或改为手动时使用）。
	ClearTaskNextRun(ctx context.Context, id int64) error

	CreateRun(ctx context.Context, r *Run) error
	UpdateRun(ctx context.Context, r *Run) error
	GetRun(ctx context.Context, id int64) (*Run, error)
	ListRuns(ctx context.Context, f RunFilter) ([]*Run, int, error)
	Counts(ctx context.Context) (Counts, error)

	// InterruptStaleRuns 将重启后残留的非终态记录标记为失败。
	InterruptStaleRuns(ctx context.Context) (int64, error)
	// PruneRuns 按任务保留最近 perTaskLimit 条运行记录，perTaskLimit<=0 时跳过。
	PruneRuns(ctx context.Context, perTaskLimit int) (int64, error)
	// DeleteRun 删除单条运行记录；记录不存在时返回 ErrNotFound。
	DeleteRun(ctx context.Context, id int64) error
	// DeleteAllRuns 清空全部运行记录，返回删除条数。
	DeleteAllRuns(ctx context.Context) (int64, error)

	// PruneExpiredRuns 删除已结束且开始时间早于 cutoff 的运行记录。
	PruneExpiredRuns(ctx context.Context, cutoff time.Time) (int64, error)
	// CountExpiredRuns 统计 PruneExpiredRuns 会删除多少条（用于清理前的提示）。
	CountExpiredRuns(ctx context.Context, cutoff time.Time) (int64, error)
	// RunStorage 返回运行记录占用情况（条数、最早记录、数据库文件大小）。
	RunStorage(ctx context.Context) (RunStorage, error)
	// Vacuum 回收空闲页（删除大量记录后数据库文件不会自动变小）。
	Vacuum(ctx context.Context) error

	// GetSetting 读取运行期设置；ok 为 false 表示未设置过。
	GetSetting(ctx context.Context, key string) (string, bool, error)
	// SetSetting 写入（upsert）运行期设置。
	SetSetting(ctx context.Context, key, value string) error
}

// TaskRuntime 是任务的运行态字段，由调度器/执行器回写。
type TaskRuntime struct {
	LastRunAt  *time.Time
	NextRunAt  *time.Time
	LastRunID  *int64
	LastStatus string
}

// RunStorage 描述运行记录的占用情况，供设置页展示。
type RunStorage struct {
	// Runs 是运行记录总条数。
	Runs int64 `json:"runs"`
	// OldestRunAt 是最早一条记录的开始时间，没有记录时为 nil。
	OldestRunAt *time.Time `json:"oldest_run_at,omitempty"`
	// DBSizeBytes 是数据库文件（不含 -wal）当前大小。
	DBSizeBytes int64 `json:"db_size_bytes"`
}

// SQLiteStore 是基于 SQLite 的实现。
type SQLiteStore struct {
	db     *sql.DB
	path   string
	logger *slog.Logger
}

var _ Store = (*SQLiteStore)(nil)

// Open 打开（必要时创建）SQLite 数据库。
func Open(path string, logger *slog.Logger) (*SQLiteStore, error) {
	if logger == nil {
		logger = slog.Default()
	}
	dsn := buildDSN(path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("打开数据库 %s: %w", path, err)
	}
	// SQLite 单写者模型：限制为单连接可彻底避免 SQLITE_BUSY。
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)

	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("连接数据库 %s: %w", path, err)
	}
	return &SQLiteStore{db: db, path: path, logger: logger}, nil
}

func buildDSN(path string) string {
	if path == ":memory:" || strings.Contains(path, "mode=memory") {
		if strings.Contains(path, "?") {
			return path
		}
		return path + "?_pragma=busy_timeout(10000)"
	}
	pragmas := []string{
		"_pragma=busy_timeout(10000)",
		"_pragma=journal_mode(WAL)",
		"_pragma=synchronous(NORMAL)",
		"_pragma=foreign_keys(1)",
	}
	sep := "?"
	if strings.Contains(path, "?") {
		sep = "&"
	}
	return path + sep + strings.Join(pragmas, "&")
}

// DB 暴露底层连接，供只读诊断使用。
func (s *SQLiteStore) DB() *sql.DB { return s.db }

// Close 关闭数据库。
func (s *SQLiteStore) Close() error {
	if s.db == nil {
		return nil
	}
	return s.db.Close()
}

const schema = `
CREATE TABLE IF NOT EXISTS tasks (
	id              INTEGER PRIMARY KEY AUTOINCREMENT,
	name            TEXT    NOT NULL UNIQUE,
	description     TEXT    NOT NULL DEFAULT '',
	kind            TEXT    NOT NULL,
	source          TEXT    NOT NULL,
	dest            TEXT    NOT NULL DEFAULT '',
	extra_flags     TEXT    NOT NULL DEFAULT '{}',
	cron_expr       TEXT    NOT NULL DEFAULT '',
	timeout_seconds INTEGER NOT NULL DEFAULT 0,
	enabled         INTEGER NOT NULL DEFAULT 1,
	dedupe_before   INTEGER NOT NULL DEFAULT 0,
	created_at      INTEGER NOT NULL,
	updated_at      INTEGER NOT NULL,
	last_run_at     INTEGER,
	next_run_at     INTEGER,
	last_run_id     INTEGER,
	last_status     TEXT    NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS runs (
	id                 INTEGER PRIMARY KEY AUTOINCREMENT,
	task_id            INTEGER NOT NULL,
	task_name          TEXT    NOT NULL DEFAULT '',
	kind               TEXT    NOT NULL DEFAULT '',
	trigger_src        TEXT    NOT NULL DEFAULT 'manual',
	job_id             INTEGER NOT NULL DEFAULT 0,
	status             TEXT    NOT NULL,
	started_at         INTEGER NOT NULL,
	finished_at        INTEGER,
	duration_ms        INTEGER NOT NULL DEFAULT 0,
	bytes              INTEGER NOT NULL DEFAULT 0,
	total_bytes        INTEGER NOT NULL DEFAULT 0,
	files              INTEGER NOT NULL DEFAULT 0,
	total_files        INTEGER NOT NULL DEFAULT 0,
	checks             INTEGER NOT NULL DEFAULT 0,
	transfers          INTEGER NOT NULL DEFAULT 0,
	errors             INTEGER NOT NULL DEFAULT 0,
	renames            INTEGER NOT NULL DEFAULT 0,
	deletes            INTEGER NOT NULL DEFAULT 0,
	speed              REAL    NOT NULL DEFAULT 0,
	eta_seconds        INTEGER NOT NULL DEFAULT 0,
	percent            REAL    NOT NULL DEFAULT 0,
	server_side_copies INTEGER NOT NULL DEFAULT 0,
	server_side_moves  INTEGER NOT NULL DEFAULT 0,
	fatal_error        INTEGER NOT NULL DEFAULT 0,
	error              TEXT    NOT NULL DEFAULT '',
	log_tail           TEXT    NOT NULL DEFAULT '',
	message            TEXT    NOT NULL DEFAULT '',
	step_index         INTEGER NOT NULL DEFAULT 0,
	step_total         INTEGER NOT NULL DEFAULT 0,
	step_results       TEXT    NOT NULL DEFAULT ''
);

-- 任务的步骤定义：一个任务按顺序执行它的全部步骤。
-- 老任务的单条 src/dst 由 Migrate 回填成 position=0 的一条记录，
-- 之后执行路径只有"多步骤"一种，不再保留单步/多步两套代码。
CREATE TABLE IF NOT EXISTS task_steps (
	id              INTEGER PRIMARY KEY AUTOINCREMENT,
	task_id         INTEGER NOT NULL,
	position        INTEGER NOT NULL,
	name            TEXT    NOT NULL DEFAULT '',
	kind            TEXT    NOT NULL,
	source          TEXT    NOT NULL,
	dest            TEXT    NOT NULL DEFAULT '',
	extra_flags     TEXT    NOT NULL DEFAULT '{}',
	timeout_seconds INTEGER NOT NULL DEFAULT 0,
	dedupe_before   INTEGER NOT NULL DEFAULT 0,
	delay_after     INTEGER NOT NULL DEFAULT 0,
	on_error        TEXT    NOT NULL DEFAULT 'continue'
);

CREATE TABLE IF NOT EXISTS settings (
	key        TEXT PRIMARY KEY,
	value      TEXT    NOT NULL,
	updated_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_runs_task_started ON runs (task_id, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_runs_status       ON runs (status);
CREATE INDEX IF NOT EXISTS idx_runs_started      ON runs (started_at DESC);
CREATE INDEX IF NOT EXISTS idx_tasks_enabled     ON tasks (enabled);
CREATE INDEX IF NOT EXISTS idx_steps_task        ON task_steps (task_id, position);
`

// Migrate 建表（幂等）。
func (s *SQLiteStore) Migrate(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, schema); err != nil {
		return fmt.Errorf("初始化数据库结构: %w", err)
	}
	// CREATE TABLE IF NOT EXISTS 对已存在的表不会补列，新增字段必须在这里显式补齐。
	if err := s.ensureColumn(ctx, "tasks", "dedupe_before", "INTEGER NOT NULL DEFAULT 0"); err != nil {
		return err
	}
	// 运行记录的步骤信息是后加的列，老库同样要补齐。
	for _, col := range []struct{ name, decl string }{
		{"step_index", "INTEGER NOT NULL DEFAULT 0"},
		{"step_total", "INTEGER NOT NULL DEFAULT 0"},
		{"step_results", "TEXT NOT NULL DEFAULT ''"},
	} {
		if err := s.ensureColumn(ctx, "runs", col.name, col.decl); err != nil {
			return err
		}
	}
	if err := s.backfillTaskSteps(ctx); err != nil {
		return err
	}
	return nil
}

// backfillTaskSteps 把老任务的单条 src/dst 补成 position=0 的一条步骤。
//
// 少了这一步，升级后所有已存在任务的 Steps 都是空的，Validate 会直接拒绝——
// 用户看到的是"昨天还好好的任务今天跑不了"。
// 这里刻意不复制 timeout_seconds：任务级超时是各步骤的默认超时，写进步骤会让
// 之后调整任务级超时对这些老任务失效。
func (s *SQLiteStore) backfillTaskSteps(ctx context.Context) error {
	res, err := s.db.ExecContext(ctx, `
		INSERT INTO task_steps (task_id, position, name, kind, source, dest, extra_flags,
			timeout_seconds, dedupe_before, delay_after, on_error)
		SELECT id, 0, '', kind, source, dest, extra_flags, 0, dedupe_before, 0, 'continue'
		FROM tasks
		WHERE NOT EXISTS (SELECT 1 FROM task_steps WHERE task_steps.task_id = tasks.id)`)
	if err != nil {
		return fmt.Errorf("回填任务步骤: %w", err)
	}
	if n, err := res.RowsAffected(); err == nil && n > 0 && s.logger != nil {
		s.logger.Info("已把旧任务补齐为单步骤", "tasks", n)
	}
	return nil
}

// ensureColumn 在指定表缺少某列时补一列。
//
// SQLite 既没有 "ADD COLUMN IF NOT EXISTS"，也不会让已有表追平新 schema，
// 所以只能用 PRAGMA table_info 先判断再 ALTER：少了这一步，老库在读写时会
// 因为 SELECT/INSERT 里出现不存在的列而直接报 "no such column"。
// table/column/decl 均为本包内常量，不接受外部输入，不存在注入面。
func (s *SQLiteStore) ensureColumn(ctx context.Context, table, column, decl string) error {
	rows, err := s.db.QueryContext(ctx, `PRAGMA table_info(`+table+`)`)
	if err != nil {
		return fmt.Errorf("读取 %s 表结构: %w", table, err)
	}
	defer rows.Close()

	for rows.Next() {
		var cid, notnull, pk int64
		var name, ctype string
		var dflt sql.NullString
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			return fmt.Errorf("扫描 tasks 表结构: %w", err)
		}
		if name == column {
			return nil // 已存在，无需 ALTER
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("遍历 tasks 表结构: %w", err)
	}

	if _, err := s.db.ExecContext(ctx, `ALTER TABLE `+table+` ADD COLUMN `+column+` `+decl); err != nil {
		return fmt.Errorf("补齐 %s 列 %s: %w", table, column, err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// tasks
// ---------------------------------------------------------------------------

const taskColumns = `id, name, description, kind, source, dest, extra_flags, cron_expr,
	timeout_seconds, enabled, dedupe_before, created_at, updated_at, last_run_at, next_run_at,
	last_run_id, last_status`

// CreateTask 插入任务并回填 ID。
//
// ID 用「最小空闲编号」而不是纯自增：任务删除后编号长期空着，列表会出现
// 1、3、4 这样的断号，用户容易误以为少了一个任务。补洞之后编号保持 1..N 连续。
func (s *SQLiteStore) CreateTask(ctx context.Context, t *Task) error {
	if err := t.Validate(); err != nil {
		return err
	}
	now := time.Now().UTC()
	if t.CreatedAt.IsZero() {
		t.CreatedAt = now
	}
	t.UpdatedAt = now

	flags, err := marshalFlags(t.ExtraFlags)
	if err != nil {
		return err
	}

	// 并发创建时两个请求可能算出同一个空闲编号，撞唯一约束后重算一次就能拿到新编号。
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("开启事务: %w", err)
	}
	defer tx.Rollback()

	for attempt := 0; ; attempt++ {
		id, err := nextFreeTaskID(ctx, tx)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `
			INSERT INTO tasks (id, name, description, kind, source, dest, extra_flags, cron_expr,
				timeout_seconds, enabled, dedupe_before, created_at, updated_at, last_status)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '')`,
			id, t.Name, t.Description, string(t.Kind), t.Source, t.Dest, flags, t.CronExpr,
			t.TimeoutSeconds, boolToInt(t.Enabled), boolToInt(t.DedupeBefore),
			millis(t.CreatedAt), millis(t.UpdatedAt),
		)
		if err == nil {
			// 步骤与任务必须在同一事务里：只写一半会留下一个没有步骤的任务，
			// 它过得了校验却跑不起来。
			if err := replaceSteps(ctx, tx, id, t.Steps); err != nil {
				return err
			}
			if err := tx.Commit(); err != nil {
				return fmt.Errorf("提交任务: %w", err)
			}
			t.ID = id
			return nil
		}
		if attempt == 0 && strings.Contains(err.Error(), "UNIQUE constraint failed: tasks.id") {
			continue
		}
		return translateErr(err, t.Name)
	}
}

// queryRower 让 ID 分配既能跑在连接上、也能跑在事务里。
type queryRower interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// nextFreeTaskID 返回当前最小的空闲任务编号（从 1 起、保持连续）。
//
// 单条 SQL 覆盖三种情况：表为空/没有 1 号 → 返回 1；中间有空洞 → 返回最小空洞；
// 编号连续 → 返回 max+1。
//
// 必须在与后续 INSERT 相同的事务里执行：否则两个并发创建会读到同一个空闲号，
// 其中一个只能靠唯一约束冲突后重试。
func nextFreeTaskID(ctx context.Context, q queryRower) (int64, error) {
	var id int64
	err := q.QueryRowContext(ctx, `
		SELECT COALESCE(MIN(gap), 1) FROM (
			SELECT id + 1 AS gap FROM tasks WHERE id + 1 NOT IN (SELECT id FROM tasks)
			UNION
			SELECT 1 AS gap WHERE NOT EXISTS (SELECT 1 FROM tasks WHERE id = 1)
		)`).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("分配任务 ID: %w", err)
	}
	return id, nil
}

// UpdateTask 全量更新任务定义（不影响运行态字段）。
func (s *SQLiteStore) UpdateTask(ctx context.Context, t *Task) error {
	if err := t.Validate(); err != nil {
		return err
	}
	flags, err := marshalFlags(t.ExtraFlags)
	if err != nil {
		return err
	}
	t.UpdatedAt = time.Now().UTC()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("开启事务: %w", err)
	}
	defer tx.Rollback()

	res, err := tx.ExecContext(ctx, `
		UPDATE tasks SET name = ?, description = ?, kind = ?, source = ?, dest = ?,
			extra_flags = ?, cron_expr = ?, timeout_seconds = ?, enabled = ?,
			dedupe_before = ?, updated_at = ?
		WHERE id = ?`,
		t.Name, t.Description, string(t.Kind), t.Source, t.Dest, flags, t.CronExpr,
		t.TimeoutSeconds, boolToInt(t.Enabled), boolToInt(t.DedupeBefore),
		millis(t.UpdatedAt), t.ID,
	)
	if err != nil {
		return translateErr(err, t.Name)
	}
	if err := ensureAffected(res); err != nil {
		return err
	}
	if err := replaceSteps(ctx, tx, t.ID, t.Steps); err != nil {
		return err
	}
	return tx.Commit()
}

// ---------------------------------------------------------------------------
// task_steps
// ---------------------------------------------------------------------------

const stepColumns = `id, position, name, kind, source, dest, extra_flags,
	timeout_seconds, dedupe_before, delay_after, on_error`

// replaceSteps 用给定的步骤整体覆盖任务的步骤（在调用方的事务内执行）。
//
// 步骤数量是个位数级别，整体删掉重插比逐条 diff 简单且不会出现残留；
// 顺序以切片下标为准，position 在这里重新编号。
func replaceSteps(ctx context.Context, tx *sql.Tx, taskID int64, steps []*TaskStep) error {
	if _, err := tx.ExecContext(ctx, `DELETE FROM task_steps WHERE task_id = ?`, taskID); err != nil {
		return fmt.Errorf("清理任务步骤: %w", err)
	}
	for i, st := range steps {
		if st == nil {
			return fmt.Errorf("任务 %d 的步骤 %d 为空", taskID, i+1)
		}
		flags, err := marshalFlags(st.ExtraFlags)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO task_steps (task_id, position, name, kind, source, dest, extra_flags,
				timeout_seconds, dedupe_before, delay_after, on_error)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			taskID, i, strings.TrimSpace(st.Name), string(st.Kind), st.Source, st.Dest, flags,
			st.TimeoutSeconds, boolToInt(st.DedupeBefore), st.DelayAfter, string(st.OnError),
		); err != nil {
			return fmt.Errorf("写入任务步骤: %w", err)
		}
	}
	return nil
}

// loadSteps 读取单个任务的步骤（按 position 升序）。
func (s *SQLiteStore) loadSteps(ctx context.Context, taskID int64) ([]*TaskStep, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT task_id, `+stepColumns+` FROM task_steps WHERE task_id = ? ORDER BY position ASC`, taskID)
	if err != nil {
		return nil, fmt.Errorf("查询任务步骤: %w", err)
	}
	defer rows.Close()

	var out []*TaskStep
	for rows.Next() {
		_, st, err := scanStepWithTask(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, st)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("遍历任务步骤: %w", err)
	}
	return out, nil
}

// loadAllSteps 一次性读取全部步骤并按任务分组，避免列表页 N+1 查询。
func (s *SQLiteStore) loadAllSteps(ctx context.Context) (map[int64][]*TaskStep, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT task_id, `+stepColumns+` FROM task_steps ORDER BY task_id ASC, position ASC`)
	if err != nil {
		return nil, fmt.Errorf("查询全部任务步骤: %w", err)
	}
	defer rows.Close()

	grouped := make(map[int64][]*TaskStep)
	for rows.Next() {
		taskID, st, err := scanStepWithTask(rows)
		if err != nil {
			return nil, err
		}
		grouped[taskID] = append(grouped[taskID], st)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("遍历任务步骤: %w", err)
	}
	return grouped, nil
}

// scanStepWithTask 扫描一行步骤，一并返回它所属的 task_id。
//
// task_id 不进 TaskStep（步骤自己不需要它），但分组查询必须读到。
func scanStepWithTask(sc scanner) (int64, *TaskStep, error) {
	var (
		taskID int64
		st     TaskStep
		flags  string
		dedupe int
		onErr  string
	)
	err := sc.Scan(&taskID, &st.ID, &st.Position, &st.Name, &st.Kind, &st.Source, &st.Dest,
		&flags, &st.TimeoutSeconds, &dedupe, &st.DelayAfter, &onErr)
	if err != nil {
		return 0, nil, fmt.Errorf("扫描任务步骤: %w", err)
	}
	st.DedupeBefore = dedupe != 0
	st.OnError = StepOnError(onErr).normalize()
	if flags != "" && flags != "{}" {
		if err := json.Unmarshal([]byte(flags), &st.ExtraFlags); err != nil {
			return 0, nil, fmt.Errorf("解析步骤 %d 的 extra_flags: %w", st.ID, err)
		}
	}
	return taskID, &st, nil
}

// GetTask 按 ID 查询任务。
func (s *SQLiteStore) GetTask(ctx context.Context, id int64) (*Task, error) {
	row := s.db.QueryRowContext(ctx, `SELECT `+taskColumns+` FROM tasks WHERE id = ?`, id)
	t, err := scanTask(row)
	if err != nil {
		return nil, err
	}
	steps, err := s.loadSteps(ctx, id)
	if err != nil {
		return nil, err
	}
	t.Steps = steps
	return t, nil
}

// GetTaskByName 按名称查询任务。
func (s *SQLiteStore) GetTaskByName(ctx context.Context, name string) (*Task, error) {
	row := s.db.QueryRowContext(ctx, `SELECT `+taskColumns+` FROM tasks WHERE name = ?`, name)
	t, err := scanTask(row)
	if err != nil {
		return nil, err
	}
	steps, err := s.loadSteps(ctx, t.ID)
	if err != nil {
		return nil, err
	}
	t.Steps = steps
	return t, nil
}

// ListTasks 返回全部任务（按 ID 升序）。
func (s *SQLiteStore) ListTasks(ctx context.Context) ([]*Task, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+taskColumns+` FROM tasks ORDER BY id ASC`)
	if err != nil {
		return nil, fmt.Errorf("查询任务列表: %w", err)
	}
	defer rows.Close()

	var out []*Task
	for rows.Next() {
		t, err := scanTask(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("遍历任务列表: %w", err)
	}

	// 步骤单独一次查询再按任务分组：任务量是几十级，N+1 查询会明显拖慢列表页。
	grouped, err := s.loadAllSteps(ctx)
	if err != nil {
		return nil, err
	}
	for _, t := range out {
		t.Steps = grouped[t.ID]
	}
	return out, nil
}

// DeleteTask 删除任务及其历史运行记录与步骤。
//
// 运行记录必须一并删掉：任务编号现在会被复用，残留的记录会挂到将来同编号的
// 新任务名下，运行详情里会混进两个任务的执行历史。
// 三张表必须在一个事务里删干净：任务编号会被复用，残留的运行记录或步骤会让
// 新任务凭空多出历史，甚至带着上一个主人的步骤直接开始跑。
func (s *SQLiteStore) DeleteTask(ctx context.Context, id int64) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("开启事务: %w", err)
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `DELETE FROM runs WHERE task_id = ?`, id); err != nil {
		return fmt.Errorf("删除任务历史运行记录: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM task_steps WHERE task_id = ?`, id); err != nil {
		return fmt.Errorf("删除任务步骤: %w", err)
	}
	res, err := tx.ExecContext(ctx, `DELETE FROM tasks WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("删除任务: %w", err)
	}
	if err := ensureAffected(res); err != nil {
		return err
	}
	return tx.Commit()
}

// UpdateTaskRuntime 只更新运行态字段。
func (s *SQLiteStore) UpdateTaskRuntime(ctx context.Context, id int64, rt TaskRuntime) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE tasks SET
			last_run_at = COALESCE(?, last_run_at),
			next_run_at = COALESCE(?, next_run_at),
			last_run_id = COALESCE(?, last_run_id),
			last_status = CASE WHEN ? = '' THEN last_status ELSE ? END
		WHERE id = ?`,
		nullableMillis(rt.LastRunAt), nullableMillis(rt.NextRunAt), nullableInt64(rt.LastRunID),
		rt.LastStatus, rt.LastStatus, id,
	)
	if err != nil {
		return fmt.Errorf("更新任务运行态: %w", err)
	}
	return nil
}

// ClearTaskNextRun 清空下次运行时间（任务被禁用或改为手动时使用）。
func (s *SQLiteStore) ClearTaskNextRun(ctx context.Context, id int64) error {
	_, err := s.db.ExecContext(ctx, `UPDATE tasks SET next_run_at = NULL WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("清空 next_run_at: %w", err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// runs
// ---------------------------------------------------------------------------

const runColumns = `id, task_id, task_name, kind, trigger_src, job_id, status, started_at,
	finished_at, duration_ms, bytes, total_bytes, files, total_files, checks, transfers, errors,
	renames, deletes, speed, eta_seconds, percent, server_side_copies, server_side_moves,
	fatal_error, error, log_tail, message, step_index, step_total, step_results`

// CreateRun 插入运行记录并回填 ID。
func (s *SQLiteStore) CreateRun(ctx context.Context, r *Run) error {
	if r.StartedAt.IsZero() {
		r.StartedAt = time.Now().UTC()
	}
	if r.Status == "" {
		r.Status = StatusPending
	}
	tail, err := marshalTail(r.LogTail)
	if err != nil {
		return err
	}
	results, err := marshalStepResults(r.StepResults)
	if err != nil {
		return err
	}
	res, err := s.db.ExecContext(ctx, `
		INSERT INTO runs (task_id, task_name, kind, trigger_src, job_id, status, started_at,
			finished_at, duration_ms, bytes, total_bytes, files, total_files, checks, transfers,
			errors, renames, deletes, speed, eta_seconds, percent, server_side_copies,
			server_side_moves, fatal_error, error, log_tail, message,
			step_index, step_total, step_results)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
			?, ?, ?)`,
		r.TaskID, r.TaskName, string(r.Kind), string(r.Trigger), r.JobID, string(r.Status),
		millis(r.StartedAt), nullableMillis(r.FinishedAt), r.DurationMS,
		r.Bytes, r.TotalBytes, r.Files, r.TotalFiles, r.Checks, r.Transfers, r.Errors,
		r.Renames, r.Deletes, r.Speed, r.ETASeconds, r.Percent,
		r.ServerSideCopies, r.ServerSideMoves, boolToInt(r.FatalError), r.Error, tail, r.Message,
		r.StepIndex, r.StepTotal, results,
	)
	if err != nil {
		return fmt.Errorf("写入运行记录: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return fmt.Errorf("读取运行记录 ID: %w", err)
	}
	r.ID = id
	return nil
}

// UpdateRun 更新运行记录。
func (s *SQLiteStore) UpdateRun(ctx context.Context, r *Run) error {
	tail, err := marshalTail(r.LogTail)
	if err != nil {
		return err
	}
	results, err := marshalStepResults(r.StepResults)
	if err != nil {
		return err
	}
	res, err := s.db.ExecContext(ctx, `
		UPDATE runs SET job_id = ?, status = ?, finished_at = ?, duration_ms = ?,
			bytes = ?, total_bytes = ?, files = ?, total_files = ?, checks = ?, transfers = ?,
			errors = ?, renames = ?, deletes = ?, speed = ?, eta_seconds = ?, percent = ?,
			server_side_copies = ?, server_side_moves = ?, fatal_error = ?, error = ?,
			log_tail = ?, message = ?, step_index = ?, step_total = ?, step_results = ?
		WHERE id = ?`,
		r.JobID, string(r.Status), nullableMillis(r.FinishedAt), r.DurationMS,
		r.Bytes, r.TotalBytes, r.Files, r.TotalFiles, r.Checks, r.Transfers,
		r.Errors, r.Renames, r.Deletes, r.Speed, r.ETASeconds, r.Percent,
		r.ServerSideCopies, r.ServerSideMoves, boolToInt(r.FatalError), r.Error, tail, r.Message,
		r.StepIndex, r.StepTotal, results,
		r.ID,
	)
	if err != nil {
		return fmt.Errorf("更新运行记录: %w", err)
	}
	return ensureAffected(res)
}

// GetRun 按 ID 查询运行记录。
func (s *SQLiteStore) GetRun(ctx context.Context, id int64) (*Run, error) {
	row := s.db.QueryRowContext(ctx, `SELECT `+runColumns+` FROM runs WHERE id = ?`, id)
	return scanRun(row)
}

// ListRuns 按条件分页查询，返回记录与总数。
func (s *SQLiteStore) ListRuns(ctx context.Context, f RunFilter) ([]*Run, int, error) {
	if f.Limit <= 0 {
		f.Limit = 50
	}
	if f.Limit > 500 {
		f.Limit = 500
	}
	if f.Offset < 0 {
		f.Offset = 0
	}

	var where []string
	var args []any
	if f.TaskID > 0 {
		where = append(where, "task_id = ?")
		args = append(args, f.TaskID)
	}
	if len(f.Status) > 0 {
		ph := make([]string, 0, len(f.Status))
		for _, st := range f.Status {
			ph = append(ph, "?")
			args = append(args, string(st))
		}
		where = append(where, "status IN ("+strings.Join(ph, ",")+")")
	} else if f.Active {
		where = append(where, "status IN ('pending','running')")
	}
	clause := ""
	if len(where) > 0 {
		clause = " WHERE " + strings.Join(where, " AND ")
	}

	var total int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM runs`+clause, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("统计运行记录: %w", err)
	}

	q := `SELECT ` + runColumns + ` FROM runs` + clause + ` ORDER BY started_at DESC, id DESC LIMIT ? OFFSET ?`
	args = append(args, f.Limit, f.Offset)
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("查询运行记录: %w", err)
	}
	defer rows.Close()

	var out []*Run
	for rows.Next() {
		r, err := scanRun(rows)
		if err != nil {
			return nil, 0, err
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("遍历运行记录: %w", err)
	}
	return out, total, nil
}

// Counts 汇总概览数据。
func (s *SQLiteStore) Counts(ctx context.Context) (Counts, error) {
	var c Counts
	q := `SELECT
		(SELECT COUNT(*) FROM tasks),
		(SELECT COUNT(*) FROM tasks WHERE enabled = 1),
		(SELECT COUNT(*) FROM tasks WHERE enabled = 1 AND cron_expr <> ''),
		(SELECT COUNT(*) FROM runs WHERE status = 'running'),
		(SELECT COUNT(*) FROM runs WHERE status = 'pending'),
		(SELECT COUNT(*) FROM runs WHERE status = 'success' AND started_at >= ?),
		(SELECT COUNT(*) FROM runs WHERE status = 'failed'  AND started_at >= ?),
		(SELECT MAX(started_at) FROM runs)`
	since := millis(time.Now().UTC().Add(-24 * time.Hour))
	var last sql.NullInt64
	if err := s.db.QueryRowContext(ctx, q, since, since).Scan(
		&c.Tasks, &c.EnabledTasks, &c.ScheduledTasks, &c.Running, &c.Pending,
		&c.Success24h, &c.Failed24h, &last,
	); err != nil {
		return Counts{}, fmt.Errorf("统计概览: %w", err)
	}
	c.LastRunAt = timeFromNull(last)
	return c, nil
}

// InterruptStaleRuns 处理程序重启后残留的非终态记录。
func (s *SQLiteStore) InterruptStaleRuns(ctx context.Context) (int64, error) {
	now := time.Now().UTC()
	res, err := s.db.ExecContext(ctx, `
		UPDATE runs SET status = 'failed', finished_at = ?, duration_ms = ? - started_at,
			fatal_error = 1, error = CASE WHEN error = '' THEN '程序重启导致任务中断' ELSE error END,
			message = '程序重启导致任务中断'
		WHERE status IN ('pending', 'running')`,
		millis(now), millis(now),
	)
	if err != nil {
		return 0, fmt.Errorf("清理残留运行记录: %w", err)
	}
	n, _ := res.RowsAffected()
	return n, nil
}

// DeleteRun 删除单条运行记录。
//
// 注意这只删库里的记录，不会碰正在执行的 rclone job——调用方必须先确认
// 该运行不在进行中（否则会出现"记录没了但任务还在跑"的观测黑洞）。
func (s *SQLiteStore) DeleteRun(ctx context.Context, id int64) error {
	res, err := s.db.ExecContext(ctx, `DELETE FROM runs WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("删除运行记录: %w", err)
	}
	return ensureAffected(res)
}

// DeleteAllRuns 清空全部运行记录。
//
// 运行态（任务的 last_run_id/last_status）故意不动：它们描述的是任务本身的
// 最近一次结果，而不是某条历史记录；清掉会让任务列表出现"从没跑过"的假象。
func (s *SQLiteStore) DeleteAllRuns(ctx context.Context) (int64, error) {
	res, err := s.db.ExecContext(ctx, `DELETE FROM runs`)
	if err != nil {
		return 0, fmt.Errorf("清空运行记录: %w", err)
	}
	n, _ := res.RowsAffected()
	if n > 0 {
		// runs 用 AUTOINCREMENT，计数器存在 sqlite_sequence 里；删光后不清零，
		// 下一条记录会接着历史最大值编号，而不是从头开始。
		if _, err := s.db.ExecContext(ctx, `DELETE FROM sqlite_sequence WHERE name = 'runs'`); err != nil {
			return n, fmt.Errorf("重置运行记录编号: %w", err)
		}
	}
	return n, nil
}

// PruneRuns 为每个任务保留最近 perTaskLimit 条记录。
func (s *SQLiteStore) PruneRuns(ctx context.Context, perTaskLimit int) (int64, error) {
	if perTaskLimit <= 0 {
		return 0, nil
	}
	res, err := s.db.ExecContext(ctx, `
		DELETE FROM runs WHERE id IN (
			SELECT id FROM (
				SELECT id, ROW_NUMBER() OVER (PARTITION BY task_id ORDER BY started_at DESC, id DESC) AS rn
				FROM runs
			) WHERE rn > ?
		)`, perTaskLimit)
	if err != nil {
		return 0, fmt.Errorf("清理历史运行记录: %w", err)
	}
	n, _ := res.RowsAffected()
	if n > 0 {
		if err := s.resetRunSequenceIfEmpty(ctx); err != nil {
			return n, err
		}
	}
	return n, nil
}

// PruneExpiredRuns 删除「已结束且早于 cutoff」的运行记录。
//
// 只删终态（success/failed/canceled）是硬约束：pending/running 是正在执行
// 任务的唯一观测窗口，把它删掉的后果比磁盘占用严重得多。
// 一个任务跑上几天很常见，所以用 started_at 而不是 finished_at 做 cutoff：
// 否则耗时超过保留期的任务一结束就会被清掉，等于白跑一次还查不到。
func (s *SQLiteStore) PruneExpiredRuns(ctx context.Context, cutoff time.Time) (int64, error) {
	res, err := s.db.ExecContext(ctx, `
		DELETE FROM runs
		WHERE status IN ('success', 'failed', 'canceled') AND started_at < ?`, millis(cutoff))
	if err != nil {
		return 0, fmt.Errorf("清理过期运行记录: %w", err)
	}
	n, _ := res.RowsAffected()
	if n > 0 {
		if err := s.resetRunSequenceIfEmpty(ctx); err != nil {
			return n, err
		}
	}
	return n, nil
}

// CountExpiredRuns 统计 PruneExpiredRuns 会删掉多少条，用于清理前的提示。
func (s *SQLiteStore) CountExpiredRuns(ctx context.Context, cutoff time.Time) (int64, error) {
	var n int64
	if err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM runs
		WHERE status IN ('success', 'failed', 'canceled') AND started_at < ?`, millis(cutoff),
	).Scan(&n); err != nil {
		return 0, fmt.Errorf("统计过期运行记录: %w", err)
	}
	return n, nil
}

// RunStorage 返回运行记录占用情况。
func (s *SQLiteStore) RunStorage(ctx context.Context) (RunStorage, error) {
	var out RunStorage
	var oldest sql.NullInt64
	err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*), MIN(started_at) FROM runs`).Scan(&out.Runs, &oldest)
	if err != nil {
		return RunStorage{}, fmt.Errorf("统计运行记录占用: %w", err)
	}
	out.OldestRunAt = timeFromNull(oldest)
	out.DBSizeBytes = dbFileSize(s.path)
	return out, nil
}

// Vacuum 重建数据库文件以回收空闲页。
//
// SQLite 删除行只是把页标记为空闲，文件大小不变；用户点"清理日志"后如果
// 看到磁盘没少会以为没生效，所以在手动清理后显式整理一次。自动清理不做
// VACUUM —— 它会重写整个库，不适合放进常驻的定时任务里。
func (s *SQLiteStore) Vacuum(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, `VACUUM`); err != nil {
		return fmt.Errorf("整理数据库失败: %w", err)
	}
	return nil
}

// resetRunSequenceIfEmpty 在 runs 被清空时重置自增计数器，使编号从头开始。
func (s *SQLiteStore) resetRunSequenceIfEmpty(ctx context.Context) error {
	var remaining int64
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM runs`).Scan(&remaining); err != nil {
		return fmt.Errorf("统计剩余运行记录: %w", err)
	}
	if remaining > 0 {
		return nil
	}
	if _, err := s.db.ExecContext(ctx, `DELETE FROM sqlite_sequence WHERE name = 'runs'`); err != nil {
		return fmt.Errorf("重置运行记录编号: %w", err)
	}
	return nil
}

// GetSetting 读取运行期设置。
func (s *SQLiteStore) GetSetting(ctx context.Context, key string) (string, bool, error) {
	var value string
	err := s.db.QueryRowContext(ctx, `SELECT value FROM settings WHERE key = ?`, key).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("读取设置 %s: %w", key, err)
	}
	return value, true, nil
}

// SetSetting 写入运行期设置（不存在则插入，存在则覆盖）。
func (s *SQLiteStore) SetSetting(ctx context.Context, key, value string) error {
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO settings (key, value, updated_at) VALUES (?, ?, ?)
		ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at`,
		key, value, millis(time.Now().UTC()),
	); err != nil {
		return fmt.Errorf("写入设置 %s: %w", key, err)
	}
	return nil
}

// dbFileSize 返回数据库文件大小；内存库或读取失败时返回 0（展示层可容错）。
func dbFileSize(path string) int64 {
	if path == "" || path == ":memory:" || strings.HasPrefix(path, "file:") {
		return 0
	}
	info, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return info.Size()
}

// ---------------------------------------------------------------------------
// 扫描与工具函数
// ---------------------------------------------------------------------------

type scanner interface {
	Scan(dest ...any) error
}

func scanTask(sc scanner) (*Task, error) {
	var (
		t         Task
		flags     string
		enabled   int
		dedupe    int
		created   int64
		updated   int64
		lastRunAt sql.NullInt64
		nextRunAt sql.NullInt64
		lastRunID sql.NullInt64
		lastStat  string
	)
	err := sc.Scan(&t.ID, &t.Name, &t.Description, &t.Kind, &t.Source, &t.Dest, &flags,
		&t.CronExpr, &t.TimeoutSeconds, &enabled, &dedupe, &created, &updated,
		&lastRunAt, &nextRunAt, &lastRunID, &lastStat)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("扫描任务记录: %w", err)
	}
	t.CronExpr = strings.TrimSpace(t.CronExpr)
	t.Enabled = enabled != 0
	t.DedupeBefore = dedupe != 0
	t.CreatedAt = timeFromMillis(created)
	t.UpdatedAt = timeFromMillis(updated)
	t.LastRunAt = timeFromNull(lastRunAt)
	t.NextRunAt = timeFromNull(nextRunAt)
	if lastRunID.Valid {
		v := lastRunID.Int64
		t.LastRunID = &v
	}
	t.LastStatus = lastStat
	if flags != "" && flags != "{}" {
		if err := json.Unmarshal([]byte(flags), &t.ExtraFlags); err != nil {
			return nil, fmt.Errorf("解析任务 %d 的 extra_flags: %w", t.ID, err)
		}
	}
	return &t, nil
}

func scanRun(sc scanner) (*Run, error) {
	var (
		r          Run
		startedAt  int64
		finishedAt sql.NullInt64
		fatal      int
		tail       string
		results    string
	)
	err := sc.Scan(&r.ID, &r.TaskID, &r.TaskName, &r.Kind, &r.Trigger, &r.JobID, &r.Status,
		&startedAt, &finishedAt, &r.DurationMS,
		&r.Bytes, &r.TotalBytes, &r.Files, &r.TotalFiles, &r.Checks, &r.Transfers, &r.Errors,
		&r.Renames, &r.Deletes, &r.Speed, &r.ETASeconds, &r.Percent,
		&r.ServerSideCopies, &r.ServerSideMoves, &fatal, &r.Error, &tail, &r.Message,
		&r.StepIndex, &r.StepTotal, &results)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("扫描运行记录: %w", err)
	}
	// started_at / finished_at 以 Unix 毫秒存储，这里统一转换为 UTC 时间。
	r.StartedAt = timeFromMillis(startedAt)
	r.FatalError = fatal != 0
	r.FinishedAt = timeFromNull(finishedAt)
	if tail != "" {
		var lines []string
		if err := json.Unmarshal([]byte(tail), &lines); err == nil {
			r.LogTail = lines
		}
	}
	if results != "" && results != "[]" {
		var sr []StepResult
		if err := json.Unmarshal([]byte(results), &sr); err == nil {
			r.StepResults = sr
		}
	}
	return &r, nil
}

func marshalFlags(flags map[string]any) (string, error) {
	if len(flags) == 0 {
		return "{}", nil
	}
	b, err := json.Marshal(flags)
	if err != nil {
		return "", fmt.Errorf("序列化 extra_flags: %w", err)
	}
	return string(b), nil
}

func marshalTail(lines []string) (string, error) {
	if len(lines) == 0 {
		return "", nil
	}
	b, err := json.Marshal(lines)
	if err != nil {
		return "", fmt.Errorf("序列化日志尾部: %w", err)
	}
	return string(b), nil
}

func marshalStepResults(results []StepResult) (string, error) {
	if len(results) == 0 {
		return "", nil
	}
	b, err := json.Marshal(results)
	if err != nil {
		return "", fmt.Errorf("序列化步骤结果: %w", err)
	}
	return string(b), nil
}

func translateErr(err error, name string) error {
	if err == nil {
		return nil
	}
	msg := err.Error()
	if strings.Contains(msg, "UNIQUE constraint failed") || strings.Contains(msg, "constraint failed") {
		return fmt.Errorf("%w: 任务名 %q 已存在", ErrConflict, name)
	}
	return fmt.Errorf("执行数据库操作: %w", err)
}

func ensureAffected(res sql.Result) error {
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("读取影响行数: %w", err)
	}
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func millis(t time.Time) int64 { return t.UTC().UnixMilli() }

func timeFromMillis(ms int64) time.Time { return time.UnixMilli(ms).UTC() }

func timeFromNull(v sql.NullInt64) *time.Time {
	if !v.Valid {
		return nil
	}
	t := time.UnixMilli(v.Int64).UTC()
	return &t
}

func nullableMillis(t *time.Time) any {
	if t == nil {
		return nil
	}
	return millis(*t)
}

func nullableInt64(v *int64) any {
	if v == nil {
		return nil
	}
	return *v
}
