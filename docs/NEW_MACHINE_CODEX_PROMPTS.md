# New Machine Codex Prompts

Use these prompts on the destination Windows machine after cloning `codexalpaca_repo`.

## 1. Bootstrap The Machine

```text
Open C:\Users\<you>\Downloads\codexalpaca_repo. Set this machine up for the multi-ticker paper trader on Windows using the native path, not Docker. Run the setup script, verify the Python environment, verify tests and doctor checks, and install the scheduled tasks. Use Google Drive for the shared ownership lease path. Do not start trading yet. Summarize anything I still need to provide locally.
```

## 2. Restore The Live Runtime Handoff

```text
In C:\Users\<you>\Downloads\codexalpaca_repo, restore the latest multi-ticker runtime migration bundle from C:\Users\<you>\Downloads\<runtime-bundle-folder>. Set MULTI_TICKER_MACHINE_LABEL to a new unique label for this machine, keep the shared Google Drive lease path, run the standby failover preflight, and make sure the machine is safe to become the standby or active runner. Do not modify strategy logic or risk settings.
```

## 3. Restore The Cleanroom Research Workspace

```text
Restore the qqq_options_30d_cleanroom handoff bundle from C:\Users\<you>\Downloads\<cleanroom-bundle-folder> so that qqq_options_30d_cleanroom sits beside codexalpaca_repo under the same parent directory. Then verify that the research scripts can run using the codexalpaca_repo virtualenv and summarize the exact command to test a small ticker batch.
```

## 4. Harden And Validate Before Go-Live

```text
Perform a full readiness check for the multi-ticker paper trader on this machine. Verify the shared Google Drive lease path, the scheduled tasks, ntfy configuration, local runtime state, health-check path, EOD close guard, and standby failover safety. If there are safe operational fixes, apply them. Do not change strategy logic or risk settings. Tell me whether this machine is ready to take over trading.
```

## 5. Post-Cutover Verification

```text
Verify that this machine now owns the multi-ticker portfolio lease, that the paper trader is running correctly, that Alpaca positions match the local session ledger, and that the health check reports no issues. If there is a safe operational mismatch, fix it directly and summarize the final state.
```
