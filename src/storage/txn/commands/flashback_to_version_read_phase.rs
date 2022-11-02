// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use txn_types::{Key, TimeStamp};

use crate::storage::{
    mvcc::MvccReader,
    txn::{
        commands::{
            Command, CommandExt, FlashbackToVersion, ProcessResult, ReadCommand, TypedCommand,
        },
        flashback_to_version_read_lock, flashback_to_version_read_write,
        sched_pool::tls_collect_keyread_histogram_vec,
        Error, ErrorInner, Result,
    },
    ScanMode, Snapshot, Statistics,
};

command! {
    FlashbackToVersionReadPhase:
        cmd_ty => (),
        display => "kv::command::flashback_to_version_read_phase -> {} | {} {} | {:?}", (version, start_ts, commit_ts, ctx),
        content => {
            region_id: u64,
            start_ts: TimeStamp,
            commit_ts: TimeStamp,
            version: TimeStamp,
            end_key: Option<Key>,
            next_lock_key: Option<Key>,
            next_write_key: Option<Key>,
        }
}

impl CommandExt for FlashbackToVersionReadPhase {
    ctx!();
    tag!(flashback_to_version);
    request_type!(KvFlashbackToVersion);
    property!(readonly);
    gen_lock!(empty);

    fn write_bytes(&self) -> usize {
        0
    }
}

/// FlashbackToVersion contains two phases:
///   1. Read phase:
///     - Scan all locks to delete them all later.
///     - Scan all the latest writes to flashback them all later.
///   2. Write phase:
///     - Delete all locks we scanned at the read phase.
///     - Write the old MVCC version writes for the keys we scanned at the read
///       phase.
impl<S: Snapshot> ReadCommand<S> for FlashbackToVersionReadPhase {
    fn process_read(self, snapshot: S, statistics: &mut Statistics) -> Result<ProcessResult> {
        info!("FlashbackToVersionReadPhase::process_read";
            "region_id" => self.region_id,
            "start_ts" => ?self.start_ts,
            "commit_ts" => ?self.commit_ts,
            "version" => ?self.version,
            "end_key" => log_wrappers::Value::key(self.end_key.as_ref().unwrap_or(&Key::from_raw(b"")).as_encoded().as_slice()),
            "next_lock_key" => log_wrappers::Value::key(self.next_lock_key.as_ref().unwrap_or(&Key::from_raw(b"")).as_encoded().as_slice()),
            "next_write_key" => log_wrappers::Value::key(self.next_write_key.as_ref().unwrap_or(&Key::from_raw(b"")).as_encoded().as_slice()),
        );
        if self.commit_ts <= self.start_ts {
            return Err(Error::from(ErrorInner::InvalidTxnTso {
                start_ts: self.start_ts,
                commit_ts: self.commit_ts,
            }));
        }
        let mut reader = MvccReader::new_with_ctx(snapshot, Some(ScanMode::Forward), &self.ctx);
        // Scan the locks.
        let (mut key_locks, has_remain_locks) = flashback_to_version_read_lock(
            &mut reader,
            &self.next_lock_key,
            &self.end_key,
            statistics,
        )?;
        // Scan the writes.
        let (mut key_old_writes, has_remain_writes) = flashback_to_version_read_write(
            &mut reader,
            key_locks.len(),
            &self.next_write_key,
            &self.end_key,
            self.version,
            self.start_ts,
            self.commit_ts,
            statistics,
            self.region_id,
        )?;
        tls_collect_keyread_histogram_vec(
            self.tag().get_str(),
            (key_locks.len() + key_old_writes.len()) as f64,
        );
        // Check next key firstly.
        let next_lock_key = if has_remain_locks {
            key_locks.pop().map(|(key, _)| key)
        } else {
            None
        };
        let next_write_key = match (has_remain_writes, key_old_writes.is_empty()) {
            (true, false) => key_old_writes.pop().map(|(key, _)| key),
            // We haven't read any write yet, so we need to read the writes in the next
            // batch later.
            (true, true) => self.next_write_key,
            (..) => None,
        };
        info!(
            "FlashbackToVersionReadPhase::process_read next";
            "region_id" => self.region_id,
            "next_lock_key" => log_wrappers::Value::key(next_lock_key.as_ref().unwrap_or(&Key::from_raw(b"")).as_encoded().as_slice()),
            "next_write_key" => log_wrappers::Value::key(next_write_key.as_ref().unwrap_or(&Key::from_raw(b"")).as_encoded().as_slice()),
        );
        if key_locks.is_empty() && key_old_writes.is_empty() {
            if next_write_key.is_some() || next_lock_key.is_some() {
                // Although there is no key left in this batch, keep processing the next batch
                // since next key(write or lock) exist.
                let next_cmd = FlashbackToVersionReadPhase {
                    region_id: self.region_id,
                    ctx: self.ctx,
                    deadline: self.deadline,
                    start_ts: self.start_ts,
                    commit_ts: self.commit_ts,
                    version: self.version,
                    end_key: self.end_key,
                    next_lock_key,
                    next_write_key,
                };
                return Ok(ProcessResult::NextCommand {
                    cmd: Command::FlashbackToVersionReadPhase(next_cmd),
                });
            }
            Ok(ProcessResult::Res)
        } else {
            let next_cmd = FlashbackToVersion {
                region_id: self.region_id,
                ctx: self.ctx,
                deadline: self.deadline,
                start_ts: self.start_ts,
                commit_ts: self.commit_ts,
                version: self.version,
                end_key: self.end_key,
                key_locks,
                key_old_writes,
                next_lock_key,
                next_write_key,
            };
            Ok(ProcessResult::NextCommand {
                cmd: Command::FlashbackToVersion(next_cmd),
            })
        }
    }
}
