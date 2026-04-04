# freebsd-jails-tui

Terminal UI for managing FreeBSD jails, built in Go with Bubble Tea.

`freebsd-jails-tui` is intended for hosts that already use the base FreeBSD jail tooling such as `jail`, `jls`, `service jail`, `rctl`, and `zfs`.

## What It Does

- Discover and display configured jails
- Start and stop jails
- Create jails through a guided wizard
- Inspect configured state, runtime state, networking, ZFS, and `rctl`
- Manage ZFS snapshots and clone from snapshots
- Manage reusable ZFS template datasets
- Run initial host configuration checks
- Destroy jails with confirmation and safety guardrails

## Requirements

- FreeBSD host
- Go 1.25+
- Root privileges, or equivalent through `doas` or `sudo`, for real host operations

## Build

```bash
go build .
```

## Run

For development:

```bash
go run .
```

For real host operations:

```bash
go build .
doas ./freebsd-jails-tui
```

## Safety

- Create, update, and destroy actions run against the real host
- The TUI validates inputs before mutation where possible
- Destructive actions use confirmation flows and additional guardrails
- Managed config-file backups are kept in the app config directory for recovery-oriented workflows

This tool should still be treated as an administrative interface, not a sandbox.

## Configuration Files

The TUI reads standard FreeBSD jail configuration locations, including:

- `/etc/jail.conf`
- `/usr/local/etc/jail.conf`
- `/etc/jail.conf.d/*`
- `/usr/local/etc/jail.conf.d/*`

User-specific TUI state is stored under the app config directory:

- `$XDG_CONFIG_HOME/freebsd-jails-tui`
- or `~/.config/freebsd-jails-tui`

## Documentation

The README is intentionally brief.

Detailed documentation such as:

- screen-by-screen workflows
- key bindings
- jail type behavior
- VNET and Linux notes
- template manager lifecycle details
- operational caveats and recovery procedures

should live in the GitHub repository wiki.

## License

BSD 2-Clause
