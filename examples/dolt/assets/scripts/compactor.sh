#!/bin/sh
# compactor — flatten Dolt commit history with SQL preflight and verification.
#
# This is the daemon executor for mol-dog-compactor. The formula remains as
# observability documentation; this script owns the irreversible SQL operations.
set -eu

: "${GC_CITY_PATH:?GC_CITY_PATH must be set}"
: "${GC_DOLT_PORT:=}"
gc_dolt_port_input="$GC_DOLT_PORT"
gc_dolt_host_input="${GC_DOLT_HOST:-}"

PACK_DIR="${GC_PACK_DIR:-$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)}"
# shellcheck disable=SC1091
. "$PACK_DIR/assets/scripts/runtime.sh"

case "${GC_DOLT_MANAGED_LOCAL:-}" in
  0|false|FALSE|no|NO)
    printf 'compactor: managed local Dolt runtime is not applicable for this order — skip\n'
    exit 0
    ;;
esac

if [ "${GC_DOLT_MANAGED_LOCAL:-}" = "1" ]; then
  managed_port=$(managed_runtime_port "$DOLT_STATE_FILE" "$DOLT_DATA_DIR" || true)
  if [ -n "$managed_port" ]; then
    if [ -n "$gc_dolt_port_input" ] && [ "$gc_dolt_port_input" != "$managed_port" ]; then
      printf 'compactor: GC_DOLT_PORT=%s does not match managed runtime port=%s for data_dir=%s — skip\n' \
        "$gc_dolt_port_input" "$managed_port" "$DOLT_DATA_DIR"
      exit 0
    fi
    GC_DOLT_PORT="$managed_port"
  elif [ -z "$gc_dolt_port_input" ]; then
    printf 'compactor: managed local Dolt runtime is not active for data_dir=%s — skip\n' \
      "$DOLT_DATA_DIR"
    exit 0
  else
    GC_DOLT_PORT="$gc_dolt_port_input"
  fi
elif [ -n "$gc_dolt_port_input" ]; then
  case "$gc_dolt_host_input" in
    ''|127.0.0.1|localhost|0.0.0.0|::1|::|'[::1]'|'[::]')
      ;;
    *)
      printf 'compactor: GC_DOLT_HOST=%s is not a local managed Dolt host — skip\n' \
        "$gc_dolt_host_input"
      exit 0
      ;;
  esac
  managed_port=$(managed_runtime_port "$DOLT_STATE_FILE" "$DOLT_DATA_DIR" || true)
  if [ -z "$managed_port" ] || [ "$gc_dolt_port_input" != "$managed_port" ]; then
    printf 'compactor: GC_DOLT_PORT=%s does not match managed runtime port=%s for data_dir=%s — skip\n' \
      "$gc_dolt_port_input" "${managed_port:-<inactive>}" "$DOLT_DATA_DIR"
    exit 0
  fi
  GC_DOLT_PORT="$managed_port"
elif [ -z "$gc_dolt_port_input" ]; then
  managed_port=$(managed_runtime_port "$DOLT_STATE_FILE" "$DOLT_DATA_DIR" || true)
  if [ -z "$managed_port" ]; then
    printf 'compactor: managed local Dolt runtime is not active for data_dir=%s — skip\n' \
      "$DOLT_DATA_DIR"
    exit 0
  fi
  GC_DOLT_PORT="$managed_port"
fi

: "${GC_DOLT_PORT:?GC_DOLT_PORT must be set}"
: "${GC_DOLT_USER:=root}"

host="${GC_DOLT_HOST:-127.0.0.1}"
commit_threshold="${GC_DOLT_COMPACTOR_COMMIT_THRESHOLD:-500}"
configured_databases="${GC_DOLT_COMPACTOR_DATABASES:-}"
mode="${GC_DOLT_COMPACTOR_MODE:-flatten}"
sql_timeout="${GC_DOLT_COMPACTOR_SQL_TIMEOUT_SECS:-300}"
gc_timeout="${GC_DOLT_COMPACTOR_GC_TIMEOUT_SECS:-1800}"
dry_run="${GC_DOLT_COMPACTOR_DRY_RUN:-}"
evidence_targets="${GC_DOLT_COMPACTOR_EVIDENCE_BEADS:-}"

if [ -n "${GC_DOLT_COMPACTOR_EVIDENCE_BEAD:-}" ]; then
  evidence_targets="$evidence_targets $GC_DOLT_COMPACTOR_EVIDENCE_BEAD"
fi
if [ -n "${GC_ORDER_TRACKING_ID:-}" ]; then
  evidence_targets="$evidence_targets $GC_ORDER_TRACKING_ID"
fi

case "$commit_threshold" in
  ''|*[!0-9]*)
    printf 'compactor: invalid GC_DOLT_COMPACTOR_COMMIT_THRESHOLD=%s (must be a non-negative integer)\n' \
      "$commit_threshold" >&2
    exit 2
    ;;
esac

case "$sql_timeout" in
  ''|*[!0-9]*|0)
    printf 'compactor: invalid GC_DOLT_COMPACTOR_SQL_TIMEOUT_SECS=%s (must be a positive integer)\n' \
      "$sql_timeout" >&2
    exit 2
    ;;
esac

case "$gc_timeout" in
  ''|*[!0-9]*|0)
    printf 'compactor: invalid GC_DOLT_COMPACTOR_GC_TIMEOUT_SECS=%s (must be a positive integer)\n' \
      "$gc_timeout" >&2
    exit 2
    ;;
esac

sanitize_output() {
  printf '%s' "$1" | tr '\n' ' ' | cut -c1-500
}

utc_now() {
  date -u +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date
}

record_evidence() {
  status="$1"
  shift
  msg="$*"
  [ -n "$evidence_targets" ] || return 0
  ts=$(utc_now)
  note="dolt-compactor $ts [$status]: $msg"

  printf '%s\n' "$evidence_targets" | tr ',	 ' '\n\n\n' | while IFS= read -r target; do
    [ -n "$target" ] || continue
    bd update "$target" \
      --append-notes "$note" \
      --set-metadata "dolt_compactor.last_run=$ts" \
      --set-metadata "dolt_compactor.status=$status" \
      --quiet >/dev/null 2>&1 || true
  done
}

case "$mode" in
  flatten)
    ;;
  surgical)
    msg="mode=surgical is not supported by the daemon executor yet; no SQL changes were made"
    printf 'compactor: %s\n' "$msg" >&2
    record_evidence "skipped" "$msg"
    exit 1
    ;;
  *)
    printf 'compactor: invalid GC_DOLT_COMPACTOR_MODE=%s (want flatten or surgical)\n' "$mode" >&2
    exit 2
    ;;
esac

lock_host=$(printf '%s' "$host" | tr '[:upper:]' '[:lower:]' | sed 's/^\[\(.*\)\]$/\1/')
case "$lock_host" in
  ''|127.0.0.1|localhost|0.0.0.0|::1|::)
    lock_host="127.0.0.1"
    ;;
esac
lock_key=$(printf '%s-%s' "$lock_host" "$GC_DOLT_PORT" | tr -c 'A-Za-z0-9_.-' '-')
lock_root="/tmp/gc-dolt-compactor"
mkdir -p "$lock_root"
chmod 700 "$lock_root" 2>/dev/null || true
lock_path="$lock_root/${lock_key}.lock"
lock_dir="${lock_path}.d"
lock_mode=""
db_tmp=""

cleanup() {
  [ -n "$db_tmp" ] && rm -f "$db_tmp" 2>/dev/null || true
  if [ "$lock_mode" = "dir" ]; then
    rmdir "$lock_dir" 2>/dev/null || true
  fi
}
trap cleanup EXIT

if command -v flock >/dev/null 2>&1; then
  : > "$lock_path"
  chmod 600 "$lock_path" 2>/dev/null || true
  exec 9>"$lock_path"
  if ! flock -n 9; then
    printf 'compactor: another compactor already running for %s:%s — skipping\n' "$host" "$GC_DOLT_PORT"
    exit 0
  fi
  lock_mode="file"
else
  if ! mkdir "$lock_dir" 2>/dev/null; then
    printf 'compactor: another compactor already running for %s:%s — skipping\n' "$host" "$GC_DOLT_PORT"
    exit 0
  fi
  lock_mode="dir"
fi

valid_database_name() {
  db="$1"
  case "$db" in
    [A-Za-z0-9_]*)
      case "$db" in
        *[!A-Za-z0-9_-]*) return 1 ;;
        *) return 0 ;;
      esac
      ;;
    *) return 1 ;;
  esac
}

valid_table_identifier() {
  table="$1"
  case "$table" in
    [A-Za-z_]*)
      case "$table" in
        *[!A-Za-z0-9_]*) return 1 ;;
        *) return 0 ;;
      esac
      ;;
    *) return 1 ;;
  esac
}

is_system_database() {
  name=$(printf '%s' "$1" | tr '[:upper:]' '[:lower:]')
  case "$name" in
    information_schema|mysql|dolt_cluster|performance_schema|sys|__gc_probe) return 0 ;;
    *) return 1 ;;
  esac
}

emit_database_name() {
  db="$1"
  if ! valid_database_name "$db"; then
    printf 'compactor: db=%s invalid database name — skip\n' "$db" >&2
    return 0
  fi
  if is_system_database "$db"; then
    printf 'compactor: db=%s system database — skip\n' "$db" >&2
    return 0
  fi
  printf '%s\n' "$db"
}

discover_database_names() {
  if [ -n "$configured_databases" ]; then
    printf '%s\n' "$configured_databases" | tr ',' '\n' | while IFS= read -r db; do
      db=$(printf '%s' "$db" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
      [ -n "$db" ] && emit_database_name "$db"
    done
    return
  fi

  if [ -d "$DOLT_DATA_DIR" ]; then
    for d in "$DOLT_DATA_DIR"/*/; do
      [ -d "$d/.dolt" ] || continue
      db=${d%/}
      db=${db##*/}
      emit_database_name "$db"
    done
  fi
}

sql_csv() {
  db="$1"
  timeout="$2"
  query="$3"
  export DOLT_CLI_PASSWORD="${GC_DOLT_PASSWORD:-}"
  run_bounded "$timeout" dolt \
    --host "$host" \
    --port "$GC_DOLT_PORT" \
    --user "$GC_DOLT_USER" \
    --no-tls \
    --use-db "$db" \
    sql --result-format csv -q "$query"
}

first_numeric_line() {
  printf '%s\n' "$1" | grep -E '^[0-9]+$' | head -1
}

first_data_line() {
  printf '%s\n' "$1" | sed -n '2p' | tr -d '\r'
}

commit_count() {
  db="$1"
  out=$(sql_csv "$db" "$sql_timeout" "SELECT COUNT(*) FROM dolt_log;" 2>&1) || {
    printf '%s\n' "$out" >&2
    return 1
  }
  count=$(first_numeric_line "$out" || true)
  case "$count" in
    ''|*[!0-9]*) return 1 ;;
  esac
  printf '%s\n' "$count"
}

root_hash() {
  db="$1"
  out=$(sql_csv "$db" "$sql_timeout" "SELECT commit_hash FROM dolt_log ORDER BY commit_order ASC LIMIT 1;" 2>&1) || {
    printf '%s\n' "$out" >&2
    return 1
  }
  hash=$(first_data_line "$out")
  case "$hash" in
    ''|*[!A-Za-z0-9]*) return 1 ;;
  esac
  printf '%s\n' "$hash"
}

table_names() {
  db="$1"
  out=$(sql_csv "$db" "$sql_timeout" "SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE' AND table_name NOT LIKE 'dolt_%' ORDER BY table_name;" 2>&1) || {
    printf '%s\n' "$out" >&2
    return 1
  }
  printf '%s\n' "$out" | tail -n +2 | tr -d '\r'
}

table_counts() {
  db="$1"
  names=$(table_names "$db") || return 1
  printf '%s\n' "$names" | while IFS= read -r table; do
    [ -n "$table" ] || continue
    if ! valid_table_identifier "$table"; then
      printf 'invalid table identifier: %s\n' "$table" >&2
      exit 2
    fi
    out=$(sql_csv "$db" "$sql_timeout" "SELECT COUNT(*) FROM \`$table\`;" 2>&1) || {
      printf '%s\n' "$out" >&2
      exit 1
    }
    count=$(first_numeric_line "$out" || true)
    case "$count" in
      ''|*[!0-9]*)
        printf 'non-numeric count for table %s: %s\n' "$table" "$(sanitize_output "$out")" >&2
        exit 1
        ;;
    esac
    printf '%s=%s\n' "$table" "$count"
  done | sort
}

line_count() {
  if [ -z "$1" ]; then
    printf '0\n'
    return
  fi
  printf '%s\n' "$1" | grep -c .
}

row_total() {
  if [ -z "$1" ]; then
    printf '0\n'
    return
  fi
  printf '%s\n' "$1" | awk -F= '{sum += $2} END {printf "%d\n", sum}'
}

counts_summary() {
  if [ -z "$1" ]; then
    printf 'none'
    return
  fi
  printf '%s\n' "$1" | tr '\n' ',' | sed 's/,$//'
}

allowed_concurrent_churn() {
  pre="$1"
  post="$2"
  {
    printf '%s\n' "$pre" | sed 's/^/pre /'
    printf '%s\n' "$post" | sed 's/^/post /'
  } | awk '
    function churn_table(name) {
      return name ~ /^(child_counters|comments|dependencies|events|interactions|issue_counter|issues|labels|local_metadata|metadata|repo_mtimes|wisps|wisp_comments|wisp_dependencies|wisp_events|wisp_labels)$/
    }
    {
      split($2, item, "=")
      table = item[1]
      count = item[2] + 0
      seen[table] = 1
      if ($1 == "pre") {
        pre[table] = count
        have_pre[table] = 1
      } else {
        post[table] = count
        have_post[table] = 1
      }
    }
    END {
      bad = 0
      first = 1
      for (table in seen) {
        if (!(table in have_pre) || !(table in have_post) || pre[table] != post[table]) {
          if ((table in have_pre) && (table in have_post) && churn_table(table) && post[table] >= pre[table]) {
            if (!first) {
              printf ","
            }
            printf "%s:%d->%d", table, pre[table], post[table]
            first = 0
          } else {
            bad = 1
          }
        }
      }
      if (bad || first) {
        exit 1
      }
      printf "\n"
    }'
}

compact_database() {
  db="$1"
  before=$(commit_count "$db") || {
    msg="db=$db preflight commit count failed"
    record_evidence "failed" "$msg"
    printf 'compactor: %s\n' "$msg" >&2
    return 1
  }

  if [ "$before" -lt "$commit_threshold" ]; then
    msg="db=$db commits=$before below_threshold=$commit_threshold"
    printf 'compactor: %s — skip\n' "$msg"
    record_evidence "skipped" "$msg"
    return 0
  fi

  pre_counts=$(table_counts "$db") || {
    msg="db=$db preflight table counts failed"
    record_evidence "failed" "$msg"
    printf 'compactor: %s\n' "$msg" >&2
    return 1
  }
  pre_tables=$(line_count "$pre_counts")
  pre_rows=$(row_total "$pre_counts")
  root=$(root_hash "$db") || {
    msg="db=$db root hash lookup failed"
    record_evidence "failed" "$msg"
    printf 'compactor: %s\n' "$msg" >&2
    return 1
  }

  record_evidence "preflight" \
    "db=$db mode=$mode commits_before=$before root=$root tables=$pre_tables rows=$pre_rows counts=$(counts_summary "$pre_counts")"

  # Evidence writes can target the same Dolt database being compacted. Refresh
  # the integrity baseline after recording preflight evidence so verification
  # does not flag the executor's own notes as data drift.
  pre_counts=$(table_counts "$db") || {
    msg="db=$db preflight table counts refresh failed"
    record_evidence "failed" "$msg"
    printf 'compactor: %s\n' "$msg" >&2
    return 1
  }
  pre_tables=$(line_count "$pre_counts")
  pre_rows=$(row_total "$pre_counts")

  if [ -n "$dry_run" ]; then
    msg="db=$db commits_before=$before root=$root tables=$pre_tables rows=$pre_rows dry_run=true"
    printf 'compactor: %s — would flatten\n' "$msg"
    record_evidence "dry-run" "$msg"
    return 0
  fi

  start=$(date +%s)
  compact_sql="CALL DOLT_RESET('--soft', '$root'); CALL DOLT_COMMIT('-Am', 'compaction: flatten history');"
  compact_out=$(sql_csv "$db" "$sql_timeout" "$compact_sql" 2>&1) || {
    msg="db=$db flatten failed output=$(sanitize_output "$compact_out")"
    record_evidence "failed" "$msg"
    printf 'compactor: %s\n' "$msg" >&2
    return 1
  }

  post_counts=$(table_counts "$db") || {
    msg="db=$db postflight table counts failed"
    record_evidence "failed" "$msg"
    printf 'compactor: %s\n' "$msg" >&2
    return 1
  }
  concurrent_msg=""
  if [ "$pre_counts" != "$post_counts" ]; then
    if drift=$(allowed_concurrent_churn "$pre_counts" "$post_counts"); then
      concurrent_msg=" concurrent_changes=$drift"
    else
      msg="db=$db integrity mismatch pre=$(counts_summary "$pre_counts") post=$(counts_summary "$post_counts")"
      record_evidence "failed" "$msg"
      printf 'compactor: %s\n' "$msg" >&2
      return 1
    fi
  fi

  after=$(commit_count "$db") || {
    msg="db=$db postflight commit count failed"
    record_evidence "failed" "$msg"
    printf 'compactor: %s\n' "$msg" >&2
    return 1
  }
  elapsed=$(( $(date +%s) - start ))
  msg="db=$db commits_before=$before commits_after=$after tables=$pre_tables rows=$pre_rows integrity=ok${concurrent_msg} duration=${elapsed}s"
  printf 'compactor: %s\n' "$msg"
  record_evidence "verified" "$msg"

  gc_start=$(date +%s)
  gc_out=$(sql_csv "$db" "$gc_timeout" "CALL DOLT_GC();" 2>&1) || {
    msg="db=$db dolt_gc failed output=$(sanitize_output "$gc_out")"
    record_evidence "gc-failed" "$msg"
    printf 'compactor: %s\n' "$msg" >&2
    return 1
  }
  gc_elapsed=$(( $(date +%s) - gc_start ))
  msg="db=$db dolt_gc=ok duration=${gc_elapsed}s output=$(sanitize_output "$gc_out")"
  printf 'compactor: %s\n' "$msg"
  record_evidence "gc" "$msg"
  return 0
}

db_tmp=$(mktemp)
discover_database_names > "$db_tmp"
if [ ! -s "$db_tmp" ]; then
  printf 'compactor: no databases discovered — nothing to do\n'
  record_evidence "skipped" "no databases discovered"
  exit 0
fi

seen=""
failed=0
processed=0
while IFS= read -r db; do
  [ -n "$db" ] || continue
  case " $seen " in
    *" $db "*) continue ;;
  esac
  seen="$seen $db"
  processed=$((processed + 1))
  if ! compact_database "$db"; then
    failed=$((failed + 1))
  fi
done < "$db_tmp"

if [ "$failed" -gt 0 ]; then
  msg="processed=$processed failed=$failed"
  record_evidence "failed" "$msg"
  printf 'compactor: %s\n' "$msg" >&2
  exit 1
fi

msg="processed=$processed failed=0"
record_evidence "complete" "$msg"
printf 'compactor: %s\n' "$msg"
