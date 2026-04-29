# freebsd-jails-tui

Terminal UI for managing FreeBSD jails, built in Go with Bubble Tea.

## What It Does

- Discover and display configured jails
- Create jails through a guided wizard
- Inspect configured state, runtime state, networking, ZFS, and `rctl`
- Manage ZFS snapshots and clone from snapshots
- Manage reusable ZFS template datasets

## Requirements

- FreeBSD host
- Go 1.25+
- Root privileges, or equivalent through `doas` or `sudo`

## Build

```bash
go build .
```

## Run

For development:

```bash
go run .
```

## Configuration Files

The TUI reads standard FreeBSD jail configuration locations, including:

- `/etc/jail.conf`
- `/usr/local/etc/jail.conf`
- `/etc/jail.conf.d/*`
- `/usr/local/etc/jail.conf.d/*`

User-specific TUI state is stored under the app config directory:

- `$XDG_CONFIG_HOME/freebsd-jails-tui` or
- `~/.config/freebsd-jails-tui`

## License

BSD 2-Clause
