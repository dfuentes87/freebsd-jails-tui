# freebsd-jails-tui

Terminal UI for creating and managing FreeBSD jails, built in Go with Bubble Tea.

The application is aimed at a FreeBSD host that already uses the base jail tooling (`jail`, `jls`, `rctl`, `zfs`, `service jail ...`). It provides a dashboard, detail views, a creation wizard, ZFS snapshot actions, startup configuration checks, and destructive jail cleanup workflows.

## Features

- Scrollable dashboard of discovered jails
- Live CPU and memory metrics
- Running/stopped state based on JID presence
- Jail detail view that combines:
  - `jls`
  - `jail.conf`
  - ZFS dataset information
  - `rctl`
- First-run initial configuration check
- Jail creation wizard
- Template manager for ZFS template datasets
- Save/load wizard templates
- ZFS integration panel for snapshot and rollback actions
- Start/stop actions from the dashboard
- Jail destroy workflow with confirmation
- Built-in help/shortcuts page

## Requirements

- FreeBSD host
- Go 1.25+
- Root privileges, or equivalent via `doas`/`sudo`

## Notes and Limitations

- The application assumes the host is using the standard FreeBSD jail tooling.
- The type selector now changes provisioning and generated `jail.conf`, but it is still opinionated and intentionally scoped.
- `thin` assumes an OpenZFS-backed template dataset and destination parent dataset.
- `vnet` can create a missing bridge automatically, but it still depends on valid host networking choices.
- `linux` bootstraps a Linux userspace under `/compat/<distro>` and still depends on working networking/package access inside the jail.
- Linux bootstrap is treated as a second phase. If jail creation succeeds but Linux bootstrap preflight fails, the jail is kept and the TUI reports a warning instead of destroying or rolling back the new jail.
- Detail view includes some raw runtime values from `jls`, which may show kernel defaults or module parameters in addition to explicit `jail.conf` settings.
- Create/destroy/start/stop operations are real host actions. Run carefully.

## Build

```bash
go mod tidy
go build .
```

## Run

```bash
go run .
```

For real host operations, build the binary and run that with privileges:

```bash
go build .
doas ./freebsd-jails-tui
```

## Screens

### Dashboard

The main dashboard shows all discovered jails and their current state.

Data shown includes:

- Jail name
- Running/stopped badge
- JID
- CPU usage
- Memory usage

Key actions from the dashboard include:

- `c` to open the jail creation wizard
- `i` to re-run the initial configuration check
- `t` to open the template manager
- `s` to start or stop the selected jail
- `z` to open the selected jail's ZFS panel
- `x` to destroy the selected jail

### Initial Config Check

On first launch, the TUI runs an initial configuration check before opening the dashboard.

It checks:

- `jail_enable`
- `jail_parallel_start`
- whether one of these jail roots exists:
  - `/jail`
  - `/usr/jail`
  - `/usr/local/jails`
- whether a jail-related ZFS dataset exists

It can optionally:

- enable missing `rc.conf` settings
- create the FreeBSD documentation-style jail directory layout
- create the FreeBSD documentation-style ZFS layout
- accept custom paths and custom dataset names

After a successful first completion, the startup check is skipped on later runs.

The dashboard also provides `i` to re-run the initial configuration check manually.

Persistent state is stored in the user config directory:

- `$XDG_CONFIG_HOME/freebsd-jails-tui/initial-check.done`
- or `~/.config/freebsd-jails-tui/initial-check.done`

### Jail Detail View

The detail view shows a consolidated view of a single jail.

Sections include:

- Overview
- configured state
- runtime state
- network summary
- ZFS dataset
- `rctl`
- Linux readiness, when applicable
- Source errors, when present

### ZFS Integration Panel

The ZFS panel works on the dataset associated with the selected jail.

Actions:

- list snapshots
- create snapshot
- rollback snapshot
- clone a selected snapshot into a new jail dataset/config
- refresh snapshot list

### Jail Creation Wizard

The wizard has:

- a jail type page
- a single configuration page for steps 1-5
- a confirmation page

Current jail types:

- `thick`
- `thin`
- `vnet`
- `linux`

Type-specific behavior is implemented:

- `thick`: full extracted or copied jail root
- `thin`: OpenZFS snapshot and clone workflow
- `vnet`: VNET-style jail networking config with bridge/epair hooks
- `linux`: Linux ABI host setup plus Linux-specific jail permissions and mount directives

Wizard fields include:

- jail name
- destination
- template or release source
- interface
- bridge
- uplink
- IPv4
- IPv6
- default router
- hostname
- Linux distro
- Linux release
- Linux bootstrap mode
- CPU percentage limit
- memory limit
- max process limit
- mount points

Important behavior:

- `Destination` expects a full path, for example `/usr/local/jails/containers/web01`
- `Destination` is prefilled from the initial config path when available
- `Hostname` is optional; if empty, the jail name is used
- `IPv6` is optional
- `inherit` is allowed for non-`vnet` networking
- `inherit` is rejected for `vnet` jails
- `vnet` uses dedicated `Bridge` and optional `Uplink` fields instead of `Interface`
- `Bridge policy` controls whether a missing bridge is auto-created or must already exist
- Linux bootstrap mode supports `auto` or `skip`
- the wizard writes new configs into `/etc/jail.conf.d/<name>.conf`
- the wizard refuses to overwrite an existing jail config file

Type-specific notes:

- `thick`
  - provisions a full root filesystem in the destination
  - seeds host `resolv.conf` and `localtime`
- `thin`
  - requires the template source to resolve to an exact ZFS dataset mountpoint
  - supports `ctrl+t` to open the template manager in selection mode
  - creates `@freebsd-jails-tui-base` on that template dataset if missing
  - clones the template dataset into the destination dataset
- `vnet`
  - uses `vnet`, `vnet.interface`, `devfs_ruleset = 5`, and generated `exec.prestart` / `exec.poststop` commands
  - requires a bridge such as `bridge0`, validates bridge/uplink/IP host state before create, and honors the selected bridge policy
  - can attach an optional uplink to that bridge before jail start
  - checks requested jail IPs against both host interfaces and already-running jails
  - warns when the requested jail subnet overlaps the addresses or subnets of already-running jails
  - configures IP addresses inside the jail with `ifconfig`
- `linux`
  - enables `linux_enable=YES` and starts the host `linux` service during creation
  - has a dedicated Linux bootstrap step for distro and release selection
  - preflights IPv4/IPv6 default routing, family-specific DNS answers, and mirror fetch access before bootstrapping
  - can skip bootstrap during creation and retry later from detail view with `b`
  - prepares compatibility mount targets under `$path/compat/<distro>`
  - bootstraps the selected Linux userspace with `debootstrap` after the jail starts
  - adds Linux-oriented mount and permission directives from the FreeBSD Handbook

### Template Manager

The TUI includes a dedicated template manager for reusable ZFS template datasets used by thin-jail cloning.

It is opened from the dashboard with `t`. From the thin-jail wizard, `ctrl+t` opens the same manager in selection mode so the chosen mountpoint is written back into `Template/Release`.

The manager provides:

- a scrollable template dataset list
- inline inspect/details for the selected template dataset
- create, clone-from-snapshot, rename, and destroy actions
- cached list/detail state that refreshes after lifecycle actions

Create mode shows:

- source input
- detected parent `templates` dataset
- derived child dataset name
- target mountpoint
- source type and whether the create step will copy or extract
- execution output after creation

If the parent `templates` dataset does not exist, create mode can:

- propose a parent dataset and mountpoint derived from the current jail layout
- create that parent dataset first
- let you edit the parent dataset and mountpoint manually before creation

Manager shortcuts:

- `c` create a new template dataset
- `n` clone a selected template snapshot into a new template dataset
- `r` rename the selected template dataset
- `x` destroy the selected template dataset
- `ctrl+r` refresh the list, or refresh create preview while creating
- `ctrl+e` edit parent dataset values in create mode when needed

Supported create sources:

- local directory
- local archive
- named entry from `/usr/local/jails/media`
- release tag such as `15.0-RELEASE`
- custom `https://...` URL

### Destroy Confirmation

Destroy is available from the dashboard and detail view.

The destroy workflow can:

- stop the jail if it is running
- remove jail-specific `rctl` rules when possible
- destroy the matched ZFS dataset recursively when one is detected
- otherwise remove the jail root path recursively
- remove `/etc/jail.conf.d/<name>.conf`
- remove `/etc/fstab.<name>`

Safety behavior:

- destroy requires explicit confirmation
- shared config files such as `/etc/jail.conf` are not removed automatically
- obvious shared root paths are refused

## Userland Sources

The wizard accepts multiple sources for jail userland:

- explicit filesystem path
- entry found in `/usr/local/jails/media`
- release tag such as `15.0-RELEASE`
- custom `https://...` URL

For release tags, the resolver checks in this order:

1. `/usr/freebsd-dist/base.txz`
2. matching archive in `/usr/local/jails/media`
3. download from FreeBSD mirrors

Release download pattern:

```text
https://download.freebsd.org/ftp/releases/<arch>/<arch>/<RELEASE>/base.txz
```

## Templates

Wizard templates are stored in the user config directory:

- `$XDG_CONFIG_HOME/freebsd-jails-tui/templates.json`
- or `~/.config/freebsd-jails-tui/templates.json`

Templates persist:

- jail type
- name
- destination
- template/release source
- networking values
- resource limits
- mount points

## Data Sources

The TUI reads from the host system using standard FreeBSD tooling and config locations.

Config discovery:

- `/etc/jail.conf`
- `/usr/local/etc/jail.conf`
- `/etc/jail.conf.d/*`
- `/usr/local/etc/jail.conf.d/*`

Runtime and metrics:

- `jls -n`
- `ps -axo jid=,pcpu=,rss=`
- `rctl`
- `zfs list`

Actions:

- `service jail start <name>`
- `service jail stop <name>`
- `zfs snapshot`
- `zfs rollback`
- `zfs destroy -r`

## License

BSD 2-Clause
